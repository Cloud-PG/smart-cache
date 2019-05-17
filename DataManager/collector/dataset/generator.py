import json
import sys
from collections import OrderedDict
from multiprocessing import Pool, Process, Queue
from os import makedirs, path
from os import remove as os_remove
from time import time

from tqdm import tqdm
from yaspin import yaspin

from ...agent.api import HTTPFS
from ..api import DataFile
from ..datafeatures.extractor import (CMSDataPopularity, CMSDataPopularityRaw,
                                      CMSSimpleRecord)
from ..datafile.json import JSONDataFileWriter
from .stage import Stage
from .utils import (ReadableDictAsAttribute, SupportTable, flush_queue,
                    gen_window_dates)


class Pipeline(object):

    def __init__(
        self,
        dataset_name: str = "dataset",
        stages: list = [],
        source: 'Resource' = None,
        spark_conf: dict = {},
        batch_size: int = 42000
    ):
        assert all(isinstance(stage, Stage)
                   for stage in stages), "You can pass only a list of Stages..."
        self._dataset_name = dataset_name
        self._stages = [] + stages
        self._source = source
        self._result = None
        self._batch_size = batch_size
        self.__stats = {
            'time': {
                'stages': {},
                'out_file': None
            },
            'result': {
                'len': 0
            }
        }
        # Update Spark config without overwrite
        for stage in self._stages:
            stage.update_config(spark_conf, overwrite_config=False)

    @property
    def result(self):
        return self._result

    @property
    def stats(self):
        return self.__stats

    def save(self, out_dir: str = 'PipelineResults'):
        out_name = "{}.json.gz".format(self._dataset_name)
        makedirs(out_dir, exist_ok=True)
        out_file_path = path.join(out_dir, out_name)
        # Write output
        start_time = time()
        print("[Pipeline][{}][Write output]".format(self._dataset_name))
        with JSONDataFileWriter(out_file_path) as out_file:
            for record in tqdm(self.result, desc="[Save dataset]"):
                out_file.append(record)
        self.__stats['time']['out_file'] = time() - start_time
        # Write stats
        print("[Pipeline][{}][Write stats]".format(self._dataset_name))
        with open(
            path.join(out_dir, "{}.stats.json".format(self._dataset_name)), 'w'
        ) as stat_file:
            json.dump(self.stats, stat_file, indent=2)
        return self

    def gen_batches(self, data, stage_name):
        batch = []
        for record in data:
            batch.append(record)
            if len(batch) == self._batch_size:
                print("[Pipeline][{}][{}][Batch creation][Generated with {} records]".format(
                    self._dataset_name, stage_name, len(batch)
                ))
                yield batch
                batch = []
        else:
            if len(batch) != 0:
                print("[Pipeline][{}][{}][Batch creation][Generated with {} records]".format(
                    self._dataset_name, stage_name, len(batch)
                ))
                yield batch

    def run(self, save_stage: bool = False, use_spark: bool = False):
        output = None

        print("[Pipeline][{}][START]".format(self._dataset_name))
        for stage in self._stages:
            start_time = time()

            if output is None:
                output = self._source.get()
                print("[Pipeline][{}][{}][RUN]".format(
                    self._dataset_name, stage.name)
                )
                output = stage.run(
                    output,
                    use_spark=use_spark
                )
            else:
                print("[Pipeline][{}][{}][RUN]".format(
                    self._dataset_name, stage.name)
                )
                output = stage.run(
                    self.gen_batches(output, stage.name),
                    use_spark=use_spark
                )

            if save_stage:
                self._source.set(
                    stage.output,
                    stage_name=stage.name
                )

            self.__stats['time']['stages'][stage.name] = time() - start_time

        self._result = output
        self.__stats['result']['len'] = len(self.result)
        print("[Pipeline][{}][END]".format(self._dataset_name))

        return self
