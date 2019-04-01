import json
import sys
from collections import OrderedDict
from datetime import date, timedelta
from functools import partial
from multiprocessing import Pool
from time import time

from tqdm import tqdm
from yaspin import yaspin

from DataManager.agent.api import HTTPFS
from DataManager.collector.api import DataFile
from DataManager.collector.datafeatures.extractor import (CMSDataPopularity,
                                                          CMSDataPopularityRaw,
                                                          CMSSimpleRecord)


class CMSDatasetV0(object):

    """Generator of CMS dataset V0.

    This generator uses HTTPFS"""

    def __init__(self, httpfs_url, httpfs_user, httpfs_password):
        self._httpfs = HTTPFS(httpfs_url, httpfs_user, httpfs_password)

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

    def get_raw_data(self, year, month, day, only_indexes=False):
        tmp_data = []
        tmp_indexes = set()
        for type_, name, fullpath in self._httpfs.liststatus("/project/awg/cms/jm-data-popularity/avro-snappy/year={}/month={}/day={}".format(year, month, day)):
            cur_file = self._httpfs.open(fullpath)

            with yaspin(text="Starting raw data extraction of {}".format(fullpath)) as spinner:
                collector = DataFile(cur_file)
                extraction_start_time = time()
                start_time = time()
                counter = 0

                for idx, record in enumerate(collector, 1):
                    obj = CMSDataPopularityRaw(record)
                    if obj:
                        if not only_indexes:
                            tmp_data.append(obj)
                        tmp_indexes |= set((obj.FileName,))

                    time_delta = time() - start_time
                    if time_delta >= 1.0:
                        counter_delta = idx - counter
                        counter = idx
                        spinner.text = "[{:0.2f} it/s][Extracted {} records from {}]".format(
                            counter_delta / time_delta, idx, fullpath)
                        start_time = time()

                spinner.write("[Extracted {} items from '{}' in {}s]".format(
                    idx, fullpath, time() - extraction_start_time))

        return tmp_data, tmp_indexes

    def extract(self, start_date, window_size, extract_support_tables=True, num_processes=2):
        start_year, start_month, start_day = [
            int(elm) for elm in start_date.split()
        ]

        res_data = OrderedDict()
        data = []
        window_indexes = set()
        next_window_indexes = set()

        if extract_support_tables:
            feature_support_table = {}

        pool = Pool(processes=num_processes)

        # Get raw data
        window = pool.starmap(
            self.get_raw_data,
            self.__gen_interval(
                start_year, start_month, start_day, window_size
            )
        )

        next_window = pool.starmap(
            partial(self.get_raw_data, only_indexes=True),
            self.__gen_interval(
                start_year, start_month, start_day, window_size, next_week=True
            )
        )

        # Merge results
        with yaspin(text="Merge results...") as spinner:
            for result in window:
                for new_data, new_indexes in result:
                    data += new_data
                    window_indexes = window_indexes | new_indexes

            for result in next_window:
                for _, new_indexes in result:
                    next_window_indexes = next_window_indexes | new_indexes

            # Merge indexes
            spinner.write("Merge indexes...")
            indexes = window_indexes & next_window_indexes

            # Create output data
            spinner.write("Create output data...")
            for idx, record in enumerate(tqdm(data)):
                cur_data = CMSDataPopularity(record.data, indexes)
                if cur_data:
                    new_data = CMSSimpleRecord(cur_data)
                    if new_data.record_id not in res_data:
                        res_data[new_data.record_id] = new_data
                    else:
                        res_data[new_data.record_id] += new_data
                    if extract_support_tables:
                        for feature, value in new_data.features:
                            if feature not in feature_support_table:
                                feature_support_table[feature] = set()
                            feature_support_table[feature] |= set((value, ))

            if extract_support_tables:
                spinner.write("Generate support tables...")
                for feature, values in feature_support_table.items():
                    feature_support_table[feature] = dict(
                        list(enumerate(sorted(values, key=lambda elm: elm.lower())))
                    )

        if extract_support_tables:
            return res_data, {'features': feature_support_table}
        else:
            return res_data, {}

    def save(self, from_, window_size, outfile_name=None, extract_support_tables=True):
        """Extract and save a dataset.

        Args:
            from_ (str): a string that represents the date since to start
                         in the format "YYYY MM DD",
                         for example: "2018 5 27"
            window_size (str): a string that represents the ending date
            outfile_name (str): output file name

        Returns:
            This object instance (for chaining operations)
        """
        start_time = time()
        data, support_tables = self.extract(from_, window_size,
                                            extract_support_tables=extract_support_tables)
        print("Data extracted in {}s".format(time() - start_time))

        if not outfile_name:
            outfile_name = "CMSDatasetV0_{}_{}.json".format(
                "-".join(from_.split()), window_size)

        start_time = time()
        with open(outfile_name, "w") as outfile:
            for record in data.values():
                outfile.write(json.dumps(record.to_dict()))
                outfile.write("\n")
        print("Output data written in {}s".format(time() - start_time))

        for name, values in support_tables.items():
            start_time = time()
            with open("{}-support-{}.json".format(outfile_name, name), "w") as outfile:
                json.dump(values, outfile)
            print("{} support table written in {}s".format(
                name.capitalize(), time() - start_time))

        return self
