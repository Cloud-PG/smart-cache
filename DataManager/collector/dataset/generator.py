import json
import sys
from collections import OrderedDict
from datetime import date, timedelta
from multiprocessing import Pool
from time import time

from tqdm import tqdm

from DataManager.agent.api import HTTPFS
from DataManager.collector.api import DataFile
from DataManager.collector.datafeatures.extractor import (CMSDataPopularity,
                                                          CMSDataPopularityRaw,
                                                          CMSSimpleRecord)
from yaspin import yaspin


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
            if cur_data:
                record = CMSSimpleRecord(cur_data)
                id_ = cur_data.record_id
                if id_ not in tmp:
                    tmp[id_] = record
                else:
                    tmp[id_] += record
        return len(records), tmp

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
        tmp_indexes = []
        for type_, name, fullpath in self._httpfs.liststatus("/project/awg/cms/jm-data-popularity/avro-snappy/year={}/month={}/day={}".format(year, month, day)):
            cur_file = self._httpfs.open(fullpath)
            with yaspin(text="Starting raw data extraction of {}".format(fullpath)) as spinner:
                collector = DataFile(cur_file)
                for idx, record in enumerate(collector, 1):
                    spinner.text = "Extracted {} records".format(idx)
                    obj = CMSDataPopularityRaw(record)
                    if not only_indexes:
                        tmp_data.append(obj)
                    if obj.record_id not in tmp_indexes:
                        tmp_indexes.append(obj.record_id)
                    break
        return tmp_data, set(tmp_indexes)

    def extract(self, start_date, window_size, chunksize=1000, n_processes=2, ui_update_time=1):
        start_year, start_month, start_day = [
            int(elm) for elm in start_date.split()]

        res_data = {}
        data = []
        indexes = set()
        next_indexes = set()

        # Get raw data
        for year, month, day in self.__gen_interval(start_year, start_month, start_day, window_size):
            new_data, new_indexes = self.get_raw_data(year, month, day)
            data += new_data
            indexes = indexes | new_indexes
            break

        for year, month, day in self.__gen_interval(start_year, start_month, start_day, window_size, next_week=True):
            _, new_indexes = self.get_raw_data(
                year, month, day, only_indexes=True)
            next_indexes = next_indexes | new_indexes
            break

        indexes = indexes & next_indexes

        for idx, record in enumerate(tqdm(data)):
            cur_data = CMSDataPopularity(record.data, indexes)
            new_data = CMSSimpleRecord(cur_data)
            if new_data.record_id not in res_data:
                res_data[new_data.record_id] = new_data
            else:
                res_data[new_data.record_id] += new_data

        return res_data

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
