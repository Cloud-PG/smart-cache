import json
import sys
from collections import OrderedDict
from datetime import date, timedelta
from multiprocessing import Process, Queue
from os import path
from time import time

import findspark
from pyspark import SparkConf, SparkContext
from tqdm import tqdm
from yaspin import yaspin

from DataManager.agent.api import HTTPFS
from DataManager.collector.api import DataFile
from DataManager.collector.datafeatures.extractor import (CMSDataPopularity,
                                                          CMSDataPopularityRaw,
                                                          CMSSimpleRecord)
from DataManager.collector.datafile.json import JSONDataFileWriter

from .utils import ReadableDictAsAttribute, SupportTable


class CMSDatasetV0Process(Process):

    """Multiprocessing process to extract data."""

    def __init__(self, return_data: 'Queue', return_indexes: 'Queue'):
        """The process that extract data in parallel.

        Args:
            return_data (Queue): the queue where the proces will put the data
            return_indexes (Queue): the queue where the proces will put the indexes
        """
        super(CMSDatasetV0Process, self).__init__()
        self.__window_date = None
        self.__collector = None
        self.__return_data = return_data
        self.__return_indexes = return_indexes

    def add_data(self, window_date, collector):
        """Add the collector for the extraction."""
        self.__window_date = "{}-{:02d}-{:02d}".format(*window_date)
        self.__collector = collector

    def run(self):
        with yaspin(text="Starting raw data extraction of {}".format(self.__window_date)) as spinner:
            extraction_start_time = time()
            start_time = time()
            counter = 0
            extractions = 0

            for idx, record in enumerate(self.__collector, 1):
                obj = CMSDataPopularityRaw(record)
                if obj:
                    self.__return_data.put(obj)
                    self.__return_indexes.put(obj.FileName)
                    extractions += 1

                time_delta = time() - start_time
                if time_delta >= 1.0:
                    counter_delta = idx - counter
                    counter = idx
                    spinner.text = "[{:0.2f} it/s][Extracted {} records of {} from {}]".format(
                        counter_delta / time_delta, extractions, idx, self.__window_date)
                    start_time = time()

            spinner.write("[Extracted {} of {} items from '{}' in {:0.2f}s]".format(
                extractions, idx, self.__window_date, time() - extraction_start_time)
            )

        print("[Process -> {}] Done!".format(self.pid))


class CMSDatasetV0(object):

    """Generator of CMS dataset V0.

    This generator uses HTTPFS or Spark"""

    def __init__(self, spark_conf: dict={}, source: dict={}):
        self._httpfs = None
        self._spark_context = None
        self._hdfs_base_path = None
        self._local_folder = None

        # Spark defaults
        self._spark_master = spark_conf.get('master', "local")
        self._spark_app_name = spark_conf.get('app_name', "CMSDatasetV0")
        self._spark_conf = spark_conf.get('config', {})

        if 'httpfs' in source:
            self._httpfs = HTTPFS(
                source['httpfs'].get('url'),
                source['httpfs'].get('user'),
                source['httpfs'].get('password')
            )
            self._httpfs_base_path = source['httpfs'].get(
                'base_path', "/project/awg/cms/jm-data-popularity/avro-snappy"
            )
        elif 'hdfs' in source:
            self._hdfs_base_path = source['hdfs'].get(
                'hdfs_base_path', "hdfs://analytix/project/awg/cms/jm-data-popularity/avro-snappy",
            )
        elif 'local' in source:
            self._local_folder = source['local'].get(
                'folder', "data",
            )

    @property
    def spark_context(self):
        if not self._spark_context and 'sc' not in locals():
            findspark.init()
            conf = SparkConf()
            conf.setMaster(self._spark_master)
            conf.setAppName(self._spark_app_name)

            for name, value in self._spark_conf.items():
                conf.set(name, value)

            self._spark_context = SparkContext.getOrCreate(conf=conf)
        elif 'sc' in locals():
            self._spark_context = sc

        return self._spark_context

    def get_data_collector(self, year, month, day):
        if self._httpfs is not None:
            for type_, name, full_path in self._httpfs.liststatus(
                    "/{}year={}/month={}/day={}".format(
                        self._httpfs_base_path, year, month, day
                    )
            ):
                cur_file = self._httpfs.open(full_path)
                collector = DataFile(cur_file)
        elif self._hdfs_base_path:
            sc = self.spark_context
            binary_file = sc.binaryFiles("{}/year={:4d}/month={:d}/day={:d}/part-m-00000.avro".format(
                self._spark_hdfs_base_path, year, month, day)
            ).collect()
            collector = DataFile(binary_file[0])
        elif self._local_folder:
            cur_file_path = path.join(
                path.abspath(self._local_folder),
                "year={}".format(year),
                "month={}".format(month),
                "day={}".format(day),
                "part-m-00000.avro"
            )
            collector = DataFile(cur_file_path)
        else:
            raise Exception("No methods to retrieve data...")
        return collector

    @staticmethod
    def __gen_interval(year: int, month: int, day: int, window_size: int, step: int=1, next_week: bool=False):
        """Create date interval in the window view requested.

        Args:
            year (int): year of the start date
            month (int): month of the start date
            day (int): day of the start date
            window_size (int): number of days of the interval
            step (int): number of days for each step (stride)
            next_week (bool): indicates if you need the next window period

        Returns:
            generator (year: int, month:int, day: int): a list of tuples of the
                generated days

        """
        window_step = timedelta(days=step)
        window_size = timedelta(days=window_size)
        if not next_week:
            start_date = date(year, month, day)
        else:
            start_date = date(year, month, day) + window_size
        end_date = start_date + window_size
        while start_date != end_date:
            yield (start_date.year, start_date.month, start_date.day)
            start_date += window_step

    def __get_raw_data(self, year: int, month: int, day: int):
        """Take raw data from a cms data popularity file in avro format.

        This function extract a specific period and it returns the data and the
        indexes for such period.

        Args:
            year (int): the year to extract
            month (int): the month to extract
            day (int): the day to extract

        Returns:
            tuple(list, set): the list of data and the index set

        """
        tmp_data = []
        tmp_indexes = set()
        full_date = "{}-{:02d}-{:02d}".format(year, month, day)

        with yaspin(text="Starting raw data extraction of {}".format(full_date)) as spinner:
            collector = self.get_data_collector(year, month, day)
            extraction_start_time = time()
            start_time = time()
            counter = 0
            extractions = 0

            for idx, record in enumerate(collector, 1):
                obj = CMSDataPopularityRaw(record)
                if obj:
                    tmp_data.append(obj)
                    tmp_indexes |= set((obj.FileName,))
                    extractions += 1

                time_delta = time() - start_time
                if time_delta >= 1.0:
                    counter_delta = idx - counter
                    counter = idx
                    spinner.text = "[{:0.2f} it/s][Extracted {} records of {} from {}]".format(
                        counter_delta / time_delta, extractions, idx, full_date)
                    start_time = time()

            spinner.write("[Extracted {} of {} items from '{}' in {:0.2f}s]".format(
                extractions, idx, full_date, time() - extraction_start_time)
            )

        return tmp_data, tmp_indexes

    def spark_extract(self, start_date: str, window_size: int,
                      extract_support_tables: bool=True,
                      num_partitions: int=10, chunk_size: int=1000,
                      log_level: str="WARN"
                      ):
        """Extract data in a time window with Spark.

        Args:
            start_date (str): a string that represents the date since to start
                              in the format "YYYY MM DD",
                              for example: "2018 5 27"
            window_size (int): the number of days to extract
            extract_support_tables (bool=True): ask to extract the support table 
                                                information
            num_partitions (int=10): number of RDD partition to create
            chunk_size (int=1000): size of records to extract in the same time
            log_level (str=WARN): the log level for Spark tasks

        Returns:
            (list, SupportTable, list, dict): the data, the support table object,
                                              the raw data list and the raw info

        Notes: 
            - see __gen_output for more information about the return
            - Valid log levels: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

        """
        start_year, start_month, start_day = [
            int(elm) for elm in start_date.split()
        ]

        sc = self.spark_context
        sc.setLogLevel(log_level)

        window = self.__gen_interval(
            start_year, start_month, start_day, window_size
        )
        next_window = self.__gen_interval(
            start_year, start_month, start_day, window_size, next_week=True
        )

        data = []
        next_data = []
        window_indexes = set()
        next_window_indexes = set()

        tmp_data, tmp_indexes = self.__spark_run(
            window, num_partitions, chunk_size)
        data += tmp_data
        window_indexes |= tmp_indexes

        tmp_data, tmp_indexes = self.__spark_run(
            next_window, num_partitions, chunk_size)
        next_data += tmp_data
        next_window_indexes |= tmp_indexes

        print("[Spark task] DONE!")
        return self.__gen_output(
            data, window_indexes,
            next_data, next_window_indexes, extract_support_tables
        )

    def __spark_run(self, window: list, num_partitions: int=10, chunk_size: int=1000,):
        """The spark run task that extract a window.

        Args:
            window (list): the dates to extract
            num_partitions (int=10): number of RDD partition to create
            chunk_size (int=1000): size of records to extract in the same time

        Returns:
            (list, set): the data and the index set
        """
        sc = self.spark_context
        out_data = []
        out_indexes = set()

        for year, month, day in window:
            print("[Spark task] Get RAW data for {}/{}/{}".format(year, month, day))
            collector = self.get_data_collector(year, month, day)
            print("[Spark task] Extract data...")
            pbar = tqdm(unit="record")
            for chunk in collector.get_chunks(chunk_size):
                processed_data = sc.parallelize(chunk, num_partitions).map(
                    lambda elm: CMSDataPopularityRaw(elm)
                ).filter(
                    lambda elm: elm.valid == True
                ).map(
                    lambda elm: (elm, elm.FileName)
                )
                tmp_data = processed_data.collect()
                if tmp_data:
                    new_data, new_indexes = map(list, zip(*tmp_data))
                    out_data += new_data
                    out_indexes |= set(new_indexes)
                pbar.update(len(chunk))
            pbar.close()

        return out_data, out_indexes

    @staticmethod
    def __flush_queue(queue):
        """Get all data from the queue.

        Notes: this is just a multiprocessing support function.

        Args:
            queue (Queue): the multiprocessing queue

        Returns:
            list: the result data pushed in the queue
        """
        data = []
        while not queue.empty():
            data.append(queue.get())
        return data

    def extract(self, start_date: str, window_size: int,
                extract_support_tables: bool = True,
                multiprocess: bool = False, num_processes: int = 2
                ):
        """Extract data in a time window.

        Args:
            start_date (str): a string that represents the date since to start
                              in the format "YYYY MM DD",
                              for example: "2018 5 27"
            window_size (int): the number of days to extract
            extract_support_tables (bool): ask to extract the support table 
                                           information
            multiprocess (bool): use Python multiprocessing
            num_processes (int=2): number of process for Python multiprocessing

        Returns:
            (list, SupportTable, list, dict): the data, the support table object,
                                              the raw data list and the raw info

        Notes: see __gen_output for more information about the return
        """
        start_year, start_month, start_day = [
            int(elm) for elm in start_date.split()
        ]

        data = []
        next_data = []
        window_indexes = set()
        next_window_indexes = set()

        # Get raw data
        if not multiprocess:

            for year, month, day in self.__gen_interval(
                start_year, start_month, start_day, window_size
            ):
                new_data, new_indexes = self.__get_raw_data(year, month, day)
                data += new_data
                window_indexes |= new_indexes

            for year, month, day in self.__gen_interval(
                start_year, start_month, start_day, window_size, next_week=True
            ):
                new_data, new_indexes = self.__get_raw_data(
                    year, month, day)
                next_data += new_data
                next_window_indexes |= new_indexes
        else:
            all_tasks = []
            launched_processes = []
            task_data = Queue()
            task_next_data = Queue()
            task_window_indexes = Queue()
            task_next_window_indexes = Queue()

            for year, month, day in self.__gen_interval(
                start_year, start_month, start_day, window_size
            ):
                all_tasks.append(
                    (
                        (year, month, day),
                        CMSDatasetV0Process(
                            task_data, task_window_indexes
                        )
                    )
                )

            for year, month, day in self.__gen_interval(
                start_year, start_month, start_day, window_size, next_week=True
            ):
                all_tasks.append(
                    (
                        (year, month, day),
                        CMSDatasetV0Process(
                            task_next_data, task_next_window_indexes
                        )

                    )
                )

            while len(all_tasks) > 0 or len(launched_processes) > 0:
                print("[Len task queue: {}][Num. launched processes: {}]".format(
                    len(all_tasks), len(launched_processes), flush=True)
                )
                if len(launched_processes) < num_processes and all_tasks:
                    window_date, process = all_tasks.pop(0)
                    collector = self.get_data_collector(*window_date)
                    process.add_data(window_date, collector)
                    process.start()
                    launched_processes.append(process)
                    print("[Process {}] started...".format(
                        process.pid), flush=True)
                    print("[Len task queue: {}][Num. launched processes: {}]".format(
                        len(all_tasks), len(launched_processes), flush=True)
                    )
                elif launched_processes:
                    while any([process.is_alive() for process in launched_processes]):
                        for process in launched_processes:
                            print("[Process -> {}][Alive: {}] try to join...".format(
                                process.pid, process.is_alive()), flush=True)
                            # Wait for a process to end
                            process.join(1)
                            print("[Process -> {}][Alive: {}]".format(
                                process.pid, process.is_alive()), flush=True)
                        print("[Flush queues...]", flush=True)
                        # Get some data...
                        data += self.__flush_queue(task_data)
                        next_data += self.__flush_queue(task_next_data)
                        window_indexes |= set(
                            self.__flush_queue(task_window_indexes)
                        )
                        next_window_indexes |= set(
                            self.__flush_queue(task_next_window_indexes)
                        )
                        # Update launched proces list
                        launched_processes = [
                            process
                            for process in launched_processes
                            if process.is_alive()
                        ]
                        print("[Len task queue: {}][Num. launched processes: {}]".format(
                            len(all_tasks), len(launched_processes), flush=True)
                        )

            # Update data and indexes with latest results
            data += self.__flush_queue(task_data)
            next_data += self.__flush_queue(task_next_data)
            window_indexes |= set(
                self.__flush_queue(task_window_indexes)
            )
            next_window_indexes |= set(
                self.__flush_queue(task_next_window_indexes)
            )
            # Close queues
            task_data.close()
            task_window_indexes.close()
            task_next_window_indexes.close()

        return self.__gen_output(
            data, window_indexes,
            next_data, next_window_indexes, extract_support_tables
        )

    def __gen_output(self, data: list, window_indexes: set,
                     next_data: list, next_window_indexes: set,
                     extract_support_tables: bool=True
                     ):
        """Generate output data.

        Args:
            data (list): the data to prepare
            next_data (list): the data of the week next to the one selected
            window_indexes (set): the set of the indexes for the selected window
            next_window_indexes (set): the set of the indexes for the next one 
                                       selected window
            extract_support_tables (bool): ask to extract the support table 
                                           information

        Returns:
            (list, SupportTable, list, dict): the data, the support table object,
                                              the raw data list and the raw info
        """
        res_data = OrderedDict()
        all_raw_data = []

        raw_info = {
            'len_raw_week': len(data),
            'len_raw_next_week': len(next_data),
        }

        if extract_support_tables:
            feature_support_table = SupportTable()

        print("[Records stats]")
        print("[raw data: {}]".format(len(data)))
        print("[raw next data: {}]".format(len(next_data)))
        print("[window_indexes: {}]".format(len(window_indexes)))
        print("[next_window_indexes: {}]".format(len(next_window_indexes)))

        # Create output
        with yaspin(text="Merge indexes...") as spinner:
            # Merge indexes
            indexes = window_indexes & next_window_indexes
            spinner.write("Indexes merged...")

            spinner.text = "Merge raw data..."
            all_raw_data = data + next_data
            spinner.write("Raw data merged...")

            # Create output data
            spinner.write("Create output data...")
            spinner.stop()
            for idx, record in enumerate(tqdm(data)):
                cur_data_pop = CMSDataPopularity(record.data)
                if cur_data_pop:
                    if cur_data_pop.FileName in next_window_indexes:
                        cur_data_pop.is_in_next_window()
                    new_record = CMSSimpleRecord(cur_data_pop)
                    if new_record.record_id not in res_data:
                        res_data[new_record.record_id] = new_record
                    else:
                        res_data[new_record.record_id] += new_record
                    if extract_support_tables:
                        for feature, value in new_record.features:
                            feature_support_table.insert(
                                'features', feature, value)

            spinner.write("Output data created...")
            spinner.start()

            if extract_support_tables:
                spinner.text = "Generate support table indexes..."
                feature_support_table.reduce_categories(
                    "features", "process",
                    feature_support_table.filters.split_process
                )
                feature_support_table.gen_indexes()
                spinner.write("Support table generated...")

        print("[result data: {}]".format(len(res_data)))

        if extract_support_tables:
            return res_data, feature_support_table, all_raw_data, raw_info
        else:
            return res_data, {}, all_raw_data, raw_info

    def save(self, from_: str, window_size: int, outfile_name: str='',
             use_spark: bool=False, extract_support_tables: bool=True,
             multiprocess: bool=False, num_processes: int=2, 
             checkpoint_step: int=5000
             ):
        """Extract and save a dataset.

        Args:
            from_ (str): a string that represents the date since to start
                         in the format "YYYY MM DD",
                         for example: "2018 5 27"
            window_size (int): the number of days to extract
            outfile_name (str): output file name
            use_spark (bool): use spark to extract data
            extract_support_tables (bool): ask to extract the support table 
                                           information
            multiprocess (bool): use Python multiprocessing
            num_processes (int=2): number of process for Python multiprocessing
            checkpoint_step (int=5000): stride for checkpoint extraction

        Returns:
            This object instance (for chaining operations)

        Note:
            The output file will be written at the end of this function. 
        """
        start_time = time()
        if not use_spark:
            data, support_tables, raw_data, raw_info = self.extract(
                from_, window_size,
                extract_support_tables=extract_support_tables,
                multiprocess=multiprocess,
                num_processes=num_processes
            )
        else:
            data, support_tables, raw_data, raw_info = self.spark_extract(
                from_, window_size,
                extract_support_tables=extract_support_tables,
            )

        extraction_time = time() - start_time
        print("Data extracted in {}s".format(extraction_time))

        if not outfile_name:
            outfile_name = "CMSDatasetV0_{}_{}.json.gz".format(
                "-".join(from_.split()), window_size)

        metadata = {
            'type': "metadata",
            'from': from_,
            'window_size': window_size,
            'support_tables': support_tables.to_dict() if extract_support_tables else False,
            'len': len(data),
            'len_raw_week': raw_info['len_raw_week'],
            'len_raw_next_week': raw_info['len_raw_next_week'],
            'raw_week_start': len(data) + 1,
            'raw_next_week_start': len(data) + raw_info['len_raw_week'] + 1,
            'extraction_time': extraction_time,
            'checkpoints': {}
        }

        with JSONDataFileWriter(outfile_name) as out_file:

            for record in tqdm(data.values(), desc="Write data"):
                cur_record = record
                if 'features' in support_tables:
                    sorted_features = support_tables.get_sorted_keys(
                        'features'
                    )
                    cur_record = cur_record.add_tensor(
                        [
                            float(
                                support_tables.get_close_value(
                                    'features',
                                    feature_name,
                                    cur_record.feature[feature_name]
                                )
                            )
                            for feature_name in sorted_features
                        ]
                    )
                out_file.append(cur_record.to_dict())

            for idx, record in tqdm(enumerate(raw_data), desc="Write raw data"):
                position = out_file.append(record.to_dict())
                if idx in [0, raw_info['len_raw_week']] or idx % checkpoint_step == 0:
                    metadata['checkpoints'][metadata['len'] + idx] = position

            with yaspin(text="Write metadata...") as spinner:
                spinner.text = "Write metadata..."
                start_time = time()
                out_file.append(metadata)
                spinner.write("Metadata written in {}s".format(
                    time() - start_time)
                )

        return self
