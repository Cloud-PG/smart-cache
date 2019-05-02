from multiprocessing import Process, Queue, cpu_count

from yaspin import yaspin

from ..datafeatures.extractor import CMSDataPopularityRaw
from .generator import Stage
from .utils import flush_queue


class CMSRawStage(Stage):

    def __init__(
        self,
        name: str="CMS-raw",
        source: 'Resource'=None,
        save_stage: bool=False,
        spark_conf: dict={}
    ):
        super(CMSRawStage, self).__init__(
            name,
            source=source,
            save_stage=save_stage,
            spark_conf=spark_conf
        )

    @staticmethod
    def __update_tasks(task_list):
        return [
            task for task in task_list if task.is_alive()
        ]

    @staticmethod
    def _process(records, queue: 'Queue'= None):
        tmp = []
        for record in records:
            new_record = CMSDataPopularityRaw(record)
            if new_record:
                tmp.append(new_record.dumps())

            # Limit processing for test
            # if len(tmp) >= 1:
            #     break

        if queue:
            for record in tmp:
                queue.put(record)
        else:
            return tmp

    def task(self, input_, num_process: int=cpu_count(), use_spark: bool=False):
        result = []
        if use_spark:
            sc = self.spark_context
            print("[STAGE][CMS RAW][SPARK]")
            tasks = []
            for cur_input in input_:
                if len(tasks) < sc.defaultParallelism:
                    tasks.append(cur_input)
                    continue
                tasks_results = sc.parallelize(
                    tasks
                ).map(
                    self._process
                ).collect()
                for cur_result in tasks_results:
                    result += cur_result
                tasks = []
            else:
                if tasks:
                    tasks_results = sc.parallelize(
                        tasks
                    ).map(
                        self._process
                    ).collect()
                    for cur_result in tasks_results:
                        result += cur_result
        else:
            tasks = []
            output_queue = Queue()
            with yaspin(text="[STAGE][CMS RAW]") as spinner:
                for cur_input in input:
                    if len(tasks) < num_process:
                        new_process = Process(
                            target=self._process,
                            args=(cur_input, output_queue)
                        )
                        tasks.append(new_process)
                        new_process.start()
                        spinner.write("[STAGE][CMS RAW][TASK ADDED]")
                    else:
                        while len(tasks) == num_process:
                            for task in tasks:
                                task.join(5)
                            else:
                                tasks = self.__update_tasks(tasks)
                                result += flush_queue(output_queue)
                    spinner.text = "[STAGE][CMS RAW][{} task{} running]".format(
                        len(tasks),
                        's' if len(tasks) > 1 else ''
                    )
                else:
                    while len(tasks) > 0:
                        spinner.text = "[STAGE][CMS RAW][{} task{} running]".format(
                            len(tasks),
                            's' if len(tasks) > 1 else ''
                        )
                        for task in tasks:
                            task.join(5)
                        else:
                            tasks = self.__update_tasks(tasks)
                            result += flush_queue(output_queue)
        return result
