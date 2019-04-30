from multiprocessing import Process, Queue

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
    def _process(records, queue):
        tmp = []
        for record in records:
            new_record = CMSDataPopularityRaw(record)
            if new_record:
                tmp.append(new_record.dumps())

            # Limit processing for test
            # if len(tmp) >= 100:
            #     break

        for record in tmp:
            queue.put(record)

    @staticmethod
    def __update_tasks(task_list):
        return [
            task for task in task_list if task.is_alive()
        ]

    def task(self, input, num_process: int=4, use_spark: bool=False):
        result = []
        if use_spark:
            sc = self.spark_context
            print("[STAGE][CMS RAW][SPARK]")
            for cur_input in input:
                for chunk in cur_input.get_chunks(100000):
                    processed_data = sc.parallelize(
                        chunk, num_process
                    ).map(
                        lambda record: CMSDataPopularityRaw(record)
                    ).filter(
                        lambda cur_elm: cur_elm.valid == True
                    ).map(
                        lambda record: record.to_dict()
                    )
                    result += processed_data.collect()
                    print("[STAGE][CMS RAW][SPARK][Processed {} records][Tot. extracted records: {}]".format(
                        len(chunk),
                        len(result)
                    ))

                    # Limit processing for test
                    # break
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
