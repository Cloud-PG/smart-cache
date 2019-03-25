import json
from time import time

from DataManager.agent.api import HTTPFS
from DataManager.collector.api import DataFile
from DataManager.collector.datafeatures.extractor import (CMSDataPopularity,
                                                          CMSSimpleRecord)
from yaspin import yaspin
from multiprocessing import Pool
import sys


class CMSDatasetV0(object):

    """Generator of CMS dataset V0.

    This generator uses HTTPFS"""

    def __init__(self, httpfs_url, httpfs_user, httpfs_password):
        self._httpfs = HTTPFS(httpfs_url, httpfs_user, httpfs_password)

    @staticmethod
    def to_cms_simple_record(records):
        tmp = {}
        for data in records:
            cur_data = CMSDataPopularity(data)
            record = CMSSimpleRecord(cur_data.features)
            id_ = cur_data.record_id
            if id_ not in tmp:
                tmp[id_] = record
            else:
                tmp[id_] += record
        return len(records), tmp

    def extract(self, from_, to_, chunksize=16000, ui_update_time=2):
        f_year, f_month, f_day = [int(elm) for elm in from_.split()]
        t_year, t_month, t_day = [int(elm) for elm in to_.split()]

        records = {}
        pool = Pool()

        for year in range(f_year, t_year + 1):
            for month in range(f_month, t_month + 1):
                for day in range(f_day, t_day + 1):
                    for type_, name, fullpath in self._httpfs.liststatus("/project/awg/cms/jm-data-popularity/avro-snappy/year={}/month={}/day={}".format(year, month, day)):
                        cur_file = self._httpfs.open(fullpath)
                        collector = DataFile(cur_file)
                        with yaspin(text="Starting extraction") as spinner:
                            start_time = time()
                            counter = 0
                            partial_counter = 0
                            for num_parsed, result in pool.imap(self.to_cms_simple_record, collector.get_chunks(chunksize)):
                                for record_id, record in result.items():
                                    if record_id not in records:
                                        records[record_id] = record
                                    else:
                                        records[record_id] += record
                                partial_counter += num_parsed
                                counter += partial_counter

                                elapsed_time = time() - start_time
                                if elapsed_time >= ui_update_time:
                                    spinner.text = "[Year: {} | Month: {} | Day: {}][Parsed ~{} items | {:0.2f} it/s][{} records stored]".format(
                                        year, month, day, counter, float(partial_counter/elapsed_time), len(records))
                                    partial_counter = 0
                                    start_time = time()

                            spinner.text = "[Year: {} | Month: {} | Day: {}][parsed {} items][{} records stored]".format(
                                year, month, day, counter, len(records))

        if pool:
            pool.terminate()

        return records

    def save(self, from_, to_, outfile_name=None):
        """Extract and save a dataset.

        Args:
            from_ (str): a string that represents the date since to start
                         in the format "YYYY MM DD",
                         for example: "2018 5 27"
            to_ (str): a string that represents the ending date
            outfile_name (str): output file name

        Returns:
            This object instance (for chaining operations)
        """
        data = self.extract(from_, to_)

        if not outfile_name:
            outfile_name = "CMSDatasetV0_{}-{}.json".format(
                "".join(from_.split()), "".join(to_.split()))

        with open(outfile_name, "w") as outfile:
            for record in data.values():
                outfile.write(json.dumps(record.to_dict()))
                outfile.write("\n")

        return self
