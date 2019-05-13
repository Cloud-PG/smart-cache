from multiprocessing import Pool, Process, Queue, cpu_count
from tempfile import TemporaryFile

from tqdm import tqdm
from yaspin import yaspin

from ..api import DataFile
from ..datafeatures.extractor import CMSDataPopularity, CMSDataPopularityRaw
from ..datafile.json import JSONDataFileReader, JSONDataFileWriter
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
        self._output = JSONDataFileWriter(descriptor=TemporaryFile())

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
                        continue

                    while len(tasks) == num_process:
                        for task in tasks:
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
                        spinner.text = "[STAGE][{}][{} task{} running]".format(
                            self.name,
                            len(tasks),
                            's' if len(tasks) > 1 else ''
                        )
                        for task in tasks:
                            task.join(5)
                        else:
                            tasks = self.__update_tasks(tasks)
                            self._output.append(flush_queue(output_queue))

        return self._output

    def run(self, input_, use_spark: bool = False):
        cur_input = self.pre_input(input_)
        cur_output = self.task(cur_input, use_spark=use_spark)
        cur_output = self.pre_output(DataFile(cur_output))
        return cur_output


class CMSRawStage(Stage):

    def __init__(
        self,
        name: str = "CMS-raw",
        source: 'Resource' = None,
        spark_conf: dict = {},
        batch_size: int = 100000
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
                    yield tmp_data
                    tmp_data = []
        else:
            if len(tmp_data) != 0:
                yield tmp_data

    @staticmethod
    def process(records, queue: 'Queue' = None):
        tmp = []
        for record in records:
            new_record = CMSDataPopularityRaw(record)
            if new_record:
                tmp.append(new_record.dumps())

            # Limit processing for test
            if len(tmp) >= 1000:
                break

        if queue:
            for record in tmp:
                queue.put(record)
        else:
            return tmp


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
