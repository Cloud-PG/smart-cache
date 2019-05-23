from multiprocessing import Pool, Process, Queue, cpu_count
from tempfile import TemporaryFile

from tqdm import tqdm
from yaspin import yaspin

from ..api import DataFile
from ..datafeatures.extractor import (CMSDataPopularity, CMSDataPopularityRaw,
                                      CMSRecordTest0)
from ..datafile.json import JSONDataFileReader, JSONDataFileWriter
from ..datafile.avro import AvroDataFileReader, AvroDataFileWriter
from .utils import BaseSpark, flush_queue


class Stage(BaseSpark):

    def __init__(
        self,
        name: str,
        source: 'Resource' = None,
        spark_conf: dict = {}
    ):
        super(Stage, self).__init__(spark_conf=spark_conf)
        self._name = name
        self._output = AvroDataFileWriter(TemporaryFile())

    @property
    def name(self):
        return self._name

    @property
    def output(self):
        return self._output

    @staticmethod
    def __update_tasks(task_list):
        return [
            task for task in task_list if task.is_alive()
        ]

    @staticmethod
    def process(records, queue: 'Queue' = None):
        raise NotImplementedError

    def pre_input(self, input_):
        return input_

    def pre_output(self, input_):
        return input_

    def task(self, input_, num_process: int = cpu_count(), use_spark: bool = False):
        if use_spark:
            sc = self.spark_context
            print("[STAGE][{}][SPARK]".format(self.name))
            tasks = []
            for cur_input in input_:
                if len(tasks) < sc.defaultParallelism:
                    tasks.append(cur_input)
                    continue

                tmp_res = sc.parallelize(
                    tasks
                ).map(
                    self.process
                ).collect()

                for cur_res in tmp_res:
                    self._output.append(cur_res)

                tasks = [cur_input]
            else:
                if tasks:
                    tmp_res = sc.parallelize(
                        tasks,
                        len(tasks)
                    ).map(
                        self.process
                    ).collect()

                    for cur_res in tmp_res:
                        self._output.append(cur_res)
        else:
            tasks = []
            output_queue = Queue()
            with yaspin(text="[STAGE][{}]".format(self.name)) as spinner:
                for cur_input in input_:
                    if len(tasks) < num_process:
                        new_process = Process(
                            target=self.process,
                            args=(cur_input, output_queue)
                        )
                        tasks.append(new_process)
                        new_process.start()
                        spinner.write(
                            "[STAGE][{}][TASK ADDED]".format(self.name))
                        spinner.write("[STAGE][{}][{} task{} running]".format(
                            self.name,
                            len(tasks),
                            's' if len(tasks) > 1 else ''
                        ))
                        continue

                    while len(tasks) == num_process:
                        for task in tasks:
                            spinner.text = "[STAGE][{}][{} task{} running]".format(
                                self.name,
                                len(tasks),
                                's' if len(tasks) > 1 else ''
                            )
                            task.join(5)
                        else:
                            tasks = self.__update_tasks(tasks)
                            self._output.append(flush_queue(output_queue))
                            spinner.text = "[STAGE][{}][{} task{} running]".format(
                                self.name,
                                len(tasks),
                                's' if len(tasks) > 1 else ''
                            )
                else:
                    while len(tasks) > 0:
                        for task in tasks:
                            spinner.text = "[STAGE][{}][{} task{} running]".format(
                                self.name,
                                len(tasks),
                                's' if len(tasks) > 1 else ''
                            )
                            task.join(5)
                        else:
                            tasks = self.__update_tasks(tasks)
                            self._output.append(flush_queue(output_queue))
                            spinner.text = "[STAGE][{}][{} task{} running]".format(
                                self.name,
                                len(tasks),
                                's' if len(tasks) > 1 else ''
                            )

        return self._output

    def run(self, input_, use_spark: bool = False):
        task_input = self.pre_input(input_)
        task_output = self.task(task_input, use_spark=use_spark)
        self._output = self.pre_output(DataFile(task_output))
        return self._output


class CMSRecordTest0Stage(Stage):

    def __init__(
        self,
        name: str = "CMS-Record-Test0",
        source: 'Resource' = None,
        spark_conf: dict = {}
    ):
        super(CMSRecordTest0Stage, self).__init__(
            name,
            source=source,
            spark_conf=spark_conf
        )

    @staticmethod
    def process(records, queue: 'Queue' = None):
        tmp = {}

        for record in records:
            new_record = CMSRecordTest0(record)
            if new_record.record_id not in tmp:
                tmp[new_record.record_id] = new_record
            else:
                tmp[new_record.record_id] += new_record

            # Limit processing for test
            if len(tmp) >= 100:
                break

        if queue:
            for record in tmp.values():
                queue.put(record.dumps())
        else:
            return [elm.dumps() for elm in tmp.values()]

    def pre_output(self, output):
        tmp = {}

        for record in output:
            cur_record = CMSRecordTest0().load(record)
            if cur_record.record_id not in tmp:
                tmp[cur_record.record_id] = cur_record
            else:
                tmp[cur_record.record_id] += cur_record

        avg_score = sum(elm.score for elm in tmp.values()) / len(tmp)

        for record in tmp.values():
            if record.score >= avg_score:
                record.set_class('good')
            else:
                record.set_class('bad')

        return [elm.to_dict() for elm in tmp.values()]


class CMSFeaturedStage(Stage):

    def __init__(
        self,
        name: str = "CMS-Featured",
        source: 'Resource' = None,
        spark_conf: dict = {}
    ):
        super(CMSFeaturedStage, self).__init__(
            name,
            source=source,
            spark_conf=spark_conf
        )

    @staticmethod
    def process(records, queue: 'Queue' = None):
        tmp = []

        for record in records:
            new_record = CMSDataPopularity(record['features'])
            if new_record:
                tmp.append(new_record.dumps())

            # Limit processing for test
            # if len(tmp) >= 1000:
            #     break

        if queue:
            for record in tmp:
                queue.put(record)
        else:
            return tmp


class CMSRawStage(Stage):

    def __init__(
        self,
        name: str = "CMS-raw",
        source: 'Resource' = None,
        spark_conf: dict = {},
        batch_size: int = 42000
    ):
        super(CMSRawStage, self).__init__(
            name,
            source=source,
            spark_conf=spark_conf
        )
        self.__batch_size = batch_size

    def pre_input(self, input_):
        tmp_data = []
        for cur_input in input_:
            for record in cur_input:
                tmp_data.append(record)
                if len(tmp_data) == self.__batch_size:
                    print("[Pre-input][Generated batch of size {}]".format(
                        len(tmp_data))
                    )
                    yield tmp_data
                    tmp_data = []
        else:
            if len(tmp_data) != 0:
                print("[Pre-input][Generated batch of size {}]".format(
                    len(tmp_data))
                )
                yield tmp_data

    @staticmethod
    def process(records, queue: 'Queue' = None):
        tmp = []
        for record in records:
            new_record = CMSDataPopularityRaw(record)
            if new_record:
                tmp.append(new_record.to_dict())

            # Limit processing for test
            # if len(tmp) >= 1:
            #     break

        if queue:
            for record in tmp:
                queue.put(record)
        else:
            return tmp
