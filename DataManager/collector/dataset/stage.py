from .generator import Stage

from multiprocessing import Pool
from ..datafeatures.extractor import CMSDataPopularityRaw


class CMSRawStage(Stage):

    def __init__(
        self,
        name: str="CMS-RAW-STAGE",
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

    def _inner_task(self, records):
        tmp = []
        for record in records:
            new_record = CMSDataPopularityRaw(record)
            if new_record:
                tmp.append(new_record)
            
            if len(tmp) >= 100:
                break

        return tmp

    def task(self, input, use_spark: bool=False):
        result = []
        if use_spark:
            pass
        else:
            pool = Pool()
            for res in pool.imap(self._inner_task, input):
                result += res
        return result
