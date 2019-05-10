from multiprocessing import Process, Queue, cpu_count

from yaspin import yaspin

from ..datafile.json import JSONDataFileReader, JSONDataFileWriter
from ..datafeatures.extractor import CMSDataPopularityRaw, CMSDataPopularity
from .generator import Stage
from .utils import flush_queue
from tqdm import tqdm


class CMSRawStage(Stage):

    def __init__(
        self,
        name: str="CMS-raw",
        source: 'Resource'=None,
        batch_size: int=100000,
        spark_conf: dict={}
    ):
        super(CMSRawStage, self).__init__(
            name,
            source=source,
            spark_conf=spark_conf
        )
        self._batch_size = batch_size

    def pre_input(self, input_):
        batch = []
        for cur_input in input_:
            for record in cur_input:
                batch.append(record)
                print("[Cur Batch len: {}]".format(len(batch)), end='\r')
                if len(batch) == self._batch_size:
                    yield batch
                    print("[Batch Done! {} records]".format(len(batch)))
                    batch = []
        else:
            if len(batch) != 0:
                yield batch

    @staticmethod
    def process(records, queue: 'Queue'= None):
        tmp = []
        for record in records:
            new_record = CMSDataPopularityRaw(record)
            if new_record:
                tmp.append(new_record)

            # Limit processing for test
            # if len(tmp) >= 1000:
            #     break

        if queue:
            for record in tmp:
                queue.put(record)
        else:
            return tmp


class CMSFeaturedStage(Stage):

    def __init__(
        self,
        name: str="CMS-Featured",
        source: 'Resource'=None,
        spark_conf: dict={}
    ):
        super(CMSFeaturedStage, self).__init__(
            name,
            source=source,
            spark_conf=spark_conf
        )

    @staticmethod
    def process(records, queue: 'Queue'= None):
        tmp = []
        for record in records:
            new_record = CMSDataPopularity(record.feature_dict)
            if new_record:
                tmp.append(new_record)

            # Limit processing for test
            # if len(tmp) >= 1000:
            #     break

        if queue:
            for record in tmp:
                queue.put(record)
        else:
            return tmp
