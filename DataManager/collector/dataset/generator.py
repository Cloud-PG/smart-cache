import json
import sys
from collections import OrderedDict
from datetime import date, timedelta
from time import time

from tqdm import tqdm
from yaspin import yaspin

from DataManager.agent.api import HTTPFS
from DataManager.collector.api import DataFile
from DataManager.collector.datafeatures.extractor import (CMSDataPopularity,
                                                          CMSDataPopularityRaw,
                                                          CMSSimpleRecord)
from DataManager.collector.datafile.json import JSONDataFileWriter
from .utils import ReadableDictAsAttribute


class SupportTables(object):

    """Class to manage support tables for feature conversions."""

    def __init__(self, support_table: dict=None):
        self._tables = {}
        self._indexed_tables = {}
        self.filters = ReadableDictAsAttribute({
            'split_process': self._filter_split_process
        })
        if support_table:
            self._indexed_tables = support_table
            for table_name, table in self._indexed_tables.items():
                self._tables[table_name] = {}
                for key in table.keys():
                    self._tables[table_name][key] = set(table[key].keys())

    @staticmethod
    def __get_similarity(_a_: str, _b_: str):
        num_eq = 0
        min_len = min([len(_a_), len(_b_)])
        max_len = max([len(_a_), len(_b_)])
        for idx in range(min_len):
            if _a_[idx] == _b_[idx]:
                num_eq += 1
        if num_eq == 0:
            num_eq = -1.
        return float(num_eq / min_len)

    @staticmethod
    def _filter_split_process(process: str):
        tmp = " ".join(process.split("-"))
        tmp = " ".join(tmp.split("_"))
        return tmp.split()

    def reduce_categories(self, table_name: str, target, filter_=None, lvls: int=0):
        assert filter_ is not None, "You need to specify a filter"
        reduced_set = {}
        categories = list(sorted(self._tables[table_name][target]))
        for category in tqdm(categories):
            cur_category = filter_(category)
            cur_lvl = reduced_set
            for word in cur_category:
                if word not in cur_lvl:
                    cur_lvl[word] = {'times': 0}
                cur_lvl[word]['times'] += 1
                cur_lvl = cur_lvl[word]

        result = set()
        cur_lvl = reduced_set
        for key, value in tqdm(cur_lvl.items()):
            cur_output = [key]
            cur_inner = value
            for cur_lvl in range(lvls):
                try:
                    next_key = [
                        inn_key for inn_key in cur_inner.keys()
                        if inn_key != 'times'
                    ].pop()
                    if next_key:
                        cur_output.append(next_key)
                        cur_inner = cur_inner[next_key]
                except IndexError:
                    break
            result |= set((" ".join(cur_output),))

        self._tables[table_name][target] = result

    @property
    def list(self):
        return list(self._indexed_tables.keys())

    def __getattr__(self, name):
        if name in self._indexed_tables:
            return self._indexed_tables[name]
        raise AttributeError(name)

    def insert(self, table_name: str, key, value):
        """Insert a value in a table.

        Note: all tables are sets, so support tables manage
              unique values.
        """
        if table_name not in self._tables:
            self._tables[table_name] = {}
        if key not in self._tables[table_name]:
            self._tables[table_name][key] = set()
        self._tables[table_name][key] |= set((value, ))
        return self

    def get_sorted_keys(self, table_name: str):
        """Returns a sorted list of the sorted key in a table."""
        return sorted(self._indexed_tables[table_name].keys())

    def get_value(self, table_name: str, key, value):
        """Convert a value with the respective index.

        Note: You have to call gen_indexes before the conversion at least
              one time to generate the indexes.
        """
        return self._indexed_tables[table_name][key][value]

    def get_close_value(self, table_name: str, key, value):
        """Convert a value with the respective index.

        Note: You have to call gen_indexes before the conversion at least
              one time to generate the indexes.
        """
        for cur_key in self._indexed_tables[table_name][key]:
            if value.index(cur_key) == 0:
                return self._indexed_tables[table_name][key][cur_key]
        raise KeyError("'{}' is not close to any index in '{}' table at '{}' key...".format(
            value, table_name, key))

    def __getitem__(self, index: int):
        """Make object interable to check if a specific table exists."""
        return list(self._indexed_tables.keys())[index]

    def gen_indexes(self):
        """Generate an unique index for each value in a table.

        Note: indexes are integer values sorted in ascending order in base
              the value strings.
        """
        for table_name, table in self._tables.items():
            for feature, values in table.items():
                if table_name not in self._indexed_tables:
                    self._indexed_tables[table_name] = {}
                self._indexed_tables[table_name][feature] = dict(
                    (key, index)
                    for index, key in list(
                        enumerate(
                            sorted(values, key=lambda elm: elm.lower())
                        )
                    )
                )
        return self

    def to_dict(self):
        """Returns this object as a dictionary.

        Note: it exports only the indexed tables.
        """
        return self._indexed_tables


class CMSDatasetV0(object):

    """Generator of CMS dataset V0.

    This generator uses HTTPFS"""

    def __init__(self, httpfs_url: str, httpfs_user: str, httpfs_password: str):
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

    def get_raw_data(self, year: int, month: int, day: int, only_indexes: bool=False):
        """Take raw data from a cms data popularity file in avro format.

        This function extract a specific period and it returns the data and the
        indexes for such period.
        """
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
                        break

                    time_delta = time() - start_time
                    if time_delta >= 1.0:
                        counter_delta = idx - counter
                        counter = idx
                        spinner.text = "[{:0.2f} it/s][Extracted {} records from {}]".format(
                            counter_delta / time_delta, idx, fullpath)
                        start_time = time()

                spinner.write("[Extracted {} items from '{}' in {:0.2f}s]".format(
                    idx, fullpath, time() - extraction_start_time))

        return tmp_data, tmp_indexes

    def extract(self, start_date: str, window_size: int, extract_support_tables: bool=True):
        """Extract data in a time window."""
        start_year, start_month, start_day = [
            int(elm) for elm in start_date.split()
        ]

        res_data = OrderedDict()
        data = []
        window_indexes = set()
        next_window_indexes = set()

        if extract_support_tables:
            feature_support_table = SupportTables()

        # Get raw data
        window = [
            self.get_raw_data(year, month, day)
            for year, month, day in self.__gen_interval(
                start_year, start_month, start_day, window_size
            )
        ]

        next_window = [
            self.get_raw_data(year, month, day, only_indexes=True)
            for year, month, day in self.__gen_interval(
                start_year, start_month, start_day, window_size, next_week=True
            )
        ]

        # Merge results
        with yaspin(text="Merge results...") as spinner:
            for new_data, new_indexes in window:
                data += new_data
                window_indexes = window_indexes | new_indexes

            for _, new_indexes in next_window:
                next_window_indexes = next_window_indexes | new_indexes

            # Merge indexes
            spinner.text = "Merge indexes..."
            indexes = window_indexes & next_window_indexes
            spinner.write("Indexes merged...")

            # Create output data
            spinner.text = "Create output data..."
            for idx, record in enumerate(tqdm(data)):
                cur_data_pop = CMSDataPopularity(record.data)
                if cur_data_pop:
                    if cur_data_pop.FileName in next_window_indexes:
                        cur_data_pop.is_in_next_window()
                    new_record = CMSSimpleRecord(cur_data_pop)
                    if new_record.record_id not in res_data:
                        res_data[new_record.record_id] = new_record
                    else:
                        res_data[new_record.record_id] += new_record
                    if extract_support_tables:
                        for feature, value in new_record.features:
                            feature_support_table.insert(
                                'features', feature, value)

            spinner.write("Output data created...")

            if extract_support_tables:
                spinner.text = "Generate support table indexes..."
                feature_support_table.reduce_categories(
                    "features", "process",
                    feature_support_table.filters.split_process
                )
                feature_support_table.gen_indexes()
                spinner.write("Support table generated...")

        if extract_support_tables:
            return res_data, feature_support_table
        else:
            return res_data, {}

    def save(self, from_: str, window_size: int, outfile_name: str='', extract_support_tables: bool=True):
        """Extract and save a dataset.

        Args:
            from_ (str): a string that represents the date since to start
                         in the format "YYYY MM DD",
                         for example: "2018 5 27"
            window_size (int): the number of days to extract
            outfile_name (str): output file name
            extract_support_tables (bool): ask to extract the support table information

        Returns:
            This object instance (for chaining operations)
        """
        start_time = time()
        data, support_tables = self.extract(
            from_, window_size,
            extract_support_tables=extract_support_tables
        )
        extraction_time = time() - start_time
        print("Data extracted in {}s".format(extraction_time))

        if not outfile_name:
            outfile_name = "CMSDatasetV0_{}_{}.json.gz".format(
                "-".join(from_.split()), window_size)

        metadata = {
            'from': from_,
            'window_size': window_size,
            'support_tables': support_tables.to_dict() if extract_support_tables else False,
            'len': len(data),
            'extraction_time': extraction_time
        }

        with yaspin(text="Create dataset...") as spinner:
            with JSONDataFileWriter(outfile_name) as out_file:
                spinner.text = "Write metadata..."
                start_time = time()
                out_file.append(metadata)
                spinner.write("Metadata written in {}s".format(
                    time() - start_time)
                )

                spinner.text = "Write data..."
                start_time = time()
                for record in data.values():
                    cur_record = record
                    if 'features' in support_tables:
                        sorted_features = support_tables.get_sorted_keys(
                            'features'
                        )
                        cur_record = cur_record.add_tensor(
                            [
                                float(
                                    support_tables.get_close_value(
                                        'features',
                                        feature_name,
                                        cur_record.feature[feature_name]
                                    )
                                )
                                for feature_name in sorted_features
                            ]
                        )
                    out_file.append(cur_record.to_dict())
                spinner.write("Data written in {}s".format(
                    time() - start_time))

        return self
