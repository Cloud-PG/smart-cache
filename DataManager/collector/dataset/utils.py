
import json
import pickle
from datetime import date, datetime, timedelta
from functools import lru_cache
from io import IOBase
from multiprocessing import cpu_count

import findspark
import numpy as np
import pandas as pd
from pyspark import SparkConf, SparkContext
from tensorflow import keras
from tqdm import tqdm


class BaseSpark(object):

    """Class that allows objects to interact with Spark context."""

    def __init__(self, spark_conf: dict = {}):
        """Initialize the Spark configuration.

        Args:
            spark_conf (dict): a dictionary with Spark properties

        Returns:
            BaseSpark: this object

        Note:
            The spark_conf dictionary has these schema:

            {
                'master': "...",
                'app_name': "...",
                'config': {
                    'spark.executor.cores': 4
                    ...
                }
            }
        """
        self._spark_context = None

        # Spark configuration
        self._spark_master = "local[{}]".format(cpu_count())
        self._spark_app_name = "SPARK"
        self._spark_conf = {
            'spark.driver.memory': "2g",
            'spark.executor.memory': "1g"
        }
        self.update_config(spark_conf)

    @property
    def spark_context(self):
        """Detects and returns the Spark context."""
        if not self._spark_context and 'sc' not in locals():
            findspark.init()
            conf = SparkConf()
            conf.setMaster(self._spark_master)
            conf.setAppName(self._spark_app_name)

            for name, value in self._spark_conf.items():
                conf.set(name, value)

            self._spark_context = SparkContext.getOrCreate(conf=conf)
        elif 'sc' in locals():
            self._spark_context = sc

        return self._spark_context

    def update_config(self, spark_conf: dict, overwrite_config: bool = True) -> 'BaseSpark':
        """Update Spark configuration.

        Args:
            spark_conf (dict): a Spark configuration, same as in init function.
            overwrite_config (bool): indicates if the Spark config have to be overwritten

        Returs:
            BaseSpark: this object
        """
        self._spark_master = spark_conf.get(
            'master',
            self._spark_master
        )

        self._spark_app_name = spark_conf.get(
            'app_name',
            self._spark_app_name
        )

        if overwrite_config:
            self._spark_conf.update(spark_conf.get('config', {}))
        else:
            new_config = spark_conf.get('config', {})
            for key, value in new_config.items():
                if key not in self._spark_conf:
                    self._spark_conf[key] = value

        return self


def date_from_timestamp_ms(timestamp: (int, float)) -> 'datetime':
    """Convert a millisecond timestamp to a date.

    Args:
        timestamp (int or float): the timestamp in milliseconds

    Returns:
        datetime: The corresponding date of the timestamp

    NOTE: for example, millisecond timestamp is used in HDFS
    """
    return datetime.fromtimestamp(float(timestamp) / 1000.)


def gen_window_dates(year: int, month: int, day: int, window_size: int, step: int = 1, next_window: bool = False):
    """Generate date interval in the window view requested.

    Args:
        year (int): year of the start date
        month (int): month of the start date
        day (int): day of the start date
        window_size (int): number of days of the interval
        step (int): number of days for each step (stride)
        next_window (bool): indicates if you need the next window period

    Returns:
        generator (year: int, month:int, day: int): a list of tuples of the
                                                    generated days

    """
    window_step = timedelta(days=step)
    window_size = timedelta(days=window_size)
    if not next_window:
        start_date = date(year, month, day)
    else:
        start_date = date(year, month, day) + window_size
    end_date = start_date + window_size
    while start_date != end_date:
        yield (start_date.year, start_date.month, start_date.day)
        start_date += window_step


def flush_queue(queue):
    """Get all data from the queue.

    Notes: this is just a multiprocessing support function.

    Args:
        queue (Queue): the multiprocessing queue

    Returns:
        list: the result data pushed in the queue
    """
    data = []
    while not queue.empty():
        data.append(queue.get())
    return data


class FeatureConverter(object):

    def __init__(self):
        self._features = {}
        self._indexes = {}

    def __contains__(self, feature_name):
        return feature_name in self._features

    def gen_indexes(self, feature_name: str, unknown_value: bool = True):
        for idx, value in enumerate(sorted(self._features[feature_name]),
                                    1 if unknown_value else 0):
            self._indexes[feature_name][value] = idx

    def insert(self, feature_name: str):
        if feature_name not in self._features:
            self._features[feature_name] = set()
            self._indexes[feature_name] = {}

    def insert_from_values(self, feature_name: str, values: list,
                           unknown_value: bool = True):
        self.insert(feature_name)
        self._features[feature_name] |= set(values)
        if unknown_value:
            self._features[feature_name] |= set(("UNKNOWN",))
        self.gen_indexes(feature_name, unknown_value)

    def get_category(self, feature_name: str, value) -> int:
        if value in self._indexes[feature_name]:
            return self._indexes[feature_name][value]
        elif "UNKNOWN" in self._indexes[feature_name]:
            return self._indexes[feature_name]['UNKNOWN']
        raise KeyError(f'{value} not in {feature_name}')

    def get_values(self, feature_name: str, values: list) -> list:
        cat_values = []
        for value in values:
            cat_values.append(self.get_category(feature_name, value))

        return np.array(cat_values)

    def get_categories(self, feature_name: str, values: list) -> list:
        categories = []
        for value in values:
            categories.append(self.get_category(feature_name, value))

        return keras.utils.to_categorical(
            categories,
            len(self._features[feature_name]),
            dtype='float32'
        )

    def get_column_categories(self, feature_name: str,
                              values: list) -> 'pd.DataFrame':
        categories = self.get_categories(feature_name, values)
        return pd.DataFrame(
            categories,
            columns=[
                f"{feature_name}_{idx}"
                for idx in range(len(self._features[feature_name]))
            ]
        )

    def __getstate__(self):
        return {
            'features': self._features,
            'indexes': self._indexes
        }

    def __setstate__(self, state: dict):
        self._features = state['features']
        self._indexes = state['indexes']

    def dump(self, out_file: [IOBase, str]) -> 'FeatureConverter':
        if isinstance(out_file, str):
            with open(out_file, "wb") as file_:
                pickle.dump(self, file_)
        else:
            pickle.dump(self, out_file)
        return self

    def load(self, in_file: [IOBase, str]) -> 'FeatureConverter':
        if isinstance(in_file, str):
            with open(in_file, "rb") as file_:
                pickle.load(self, file_)
        else:
            pickle.load(self, in_file)
        return self


class SupportTable(object):

    """Class to manage support tables for feature conversions."""

    def __init__(self, support_table: dict = None):
        self._tables = {}
        self._indexed_tables = {}
        self.filters = ReadableDictAsAttribute({
            'simple_split': self._filter_simple_split
        })
        self.__sorted_keys = {}
        self.__sizes = {}
        if support_table:
            self._indexed_tables = support_table
            for table_name, table in self._indexed_tables.items():
                self._tables[table_name] = {}
                for key in table.keys():
                    self._tables[table_name][key] = set(table[key].keys())

    def __getstate__(self):
        """Makes obj pickable."""
        return {
            'tables': self._tables,
            'indexed_tables': self._indexed_tables,
            'sorted_keys': self.__sorted_keys,
            'sizes': self.__sizes,
        }

    def __setstate__(self, state):
        """Makes obj pickable."""
        self._tables = state['tables']
        self._indexed_tables = state['indexed_tables']
        self.__sorted_keys = state['sorted_keys']
        self.__sizes = state['sizes']

    def close_conversion(self, table_name: str, data: dict, normalized: bool = True, one_hot: bool = False):
        """Convert data value following the support tables."""
        if table_name not in self.__sorted_keys:
            self.__sorted_keys[table_name] = self.get_sorted_keys(table_name)
        if table_name not in self.__sizes:
            self.__sizes[table_name] = []
            for key in self.__sorted_keys[table_name]:
                self.__sizes[table_name].append(
                    len(self._indexed_tables[table_name][key])
                )
        sorted_keys = self.__sorted_keys[table_name]
        sizes = self.__sizes[table_name]
        res = [
            self.get_close_value(
                table_name,
                key,
                data[key]
            )
            for key in sorted_keys
        ]
        assert normalized != one_hot, "You can choose normalized or one hot features..."
        if normalized:
            for idx, value in enumerate(res):
                res[idx] = float(value / sizes[idx])
        elif one_hot:
            tmp = []
            for idx, size in enumerate(sizes):
                inner_tmp = [
                    0. for _ in range(
                        size
                    )
                ]
                inner_tmp[res[idx]] = 1.
                for elm in inner_tmp:
                    tmp.append(elm)
            res = tmp
        return res

    @staticmethod
    def _filter_simple_split(process: str) -> list:
        tmp = " ".join(process.split("-"))
        tmp = " ".join(tmp.split("_"))
        return tmp.split()

    def reduce_categories(self, table_name: str, target, filter_: callable = None, lvls: int = 0) -> 'SupportTable':
        assert filter_ is not None, "You need to specify a filter"
        reduced_set = {}
        categories = list(
            elm for elm in sorted(
                self._tables[table_name][target]
            ) if elm != "__unknown__"
        )
        for category in tqdm(categories, desc="[Get category '{}']".format(target)):
            cur_category = filter_(category)
            cur_lvl = reduced_set
            for word in cur_category:
                if word not in cur_lvl:
                    cur_lvl[word] = {'times': 0}
                cur_lvl[word]['times'] += 1
                cur_lvl = cur_lvl[word]

        result = set()
        cur_lvl = reduced_set
        for key, value in tqdm(cur_lvl.items(), desc="[Reduce category '{}']".format(target)):
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

        result |= set(("__unknown__", ))

        self._tables[table_name][target] = result
        return self

    @property
    def list(self) -> list:
        return list(self._indexed_tables.keys())

    def __getattr__(self, name):
        if name in self._indexed_tables:
            return self._indexed_tables[name]
        raise AttributeError(name)

    def insert(self, table_name: str, key, value, with_unknown: bool = True):
        """Insert a value in a table.

        Note: all tables are sets, so support tables manage
              unique values.
        """
        if table_name not in self._tables:
            self._tables[table_name] = {}
        if key not in self._tables[table_name]:
            self._tables[table_name][key] = set()
        self._tables[table_name][key] |= set((value, ))
        if with_unknown:
            self._tables[table_name][key] |= set(('__unknown__', ))
        return self

    def get_sorted_keys(self, table_name: str):
        """Returns a sorted list of the sorted key in a table."""
        return sorted(self._indexed_tables[table_name].keys())

    def get_len(self, table_name: str, key):
        return len(self._indexed_tables[table_name][key])

    @lru_cache(256)
    def get_value(self, table_name: str, key, value):
        """Convert a value with the respective index.

        Note: You have to call gen_indexes before the conversion at least
              one time to generate the indexes.
        """
        return self._indexed_tables[table_name][key][value]

    @lru_cache(256)
    def get_close_value(self, table_name: str, key, value):
        """Convert a value with the respective index.

        Note: You have to call gen_indexes before the conversion at least
              one time to generate the indexes.
        """
        for cur_key in self._indexed_tables[table_name][key]:
            if value.find(cur_key) == 0:
                return self._indexed_tables[table_name][key][cur_key]
        if '__unknown__' in self._indexed_tables[table_name][key]:
            return self._indexed_tables[table_name][key]['__unknown__']
        else:
            raise KeyError("'{}' is not close to any index in '{}' table at '{}' key...".format(
                value, table_name, key))

    def __getitem__(self, index: int):
        """Make object interable to check if a specific table exists."""
        return list(self._indexed_tables.keys())[index]

    def gen_indexes(self) -> 'SupportTable':
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

    def to_dict(self) -> dict:
        """Returns this object as a dictionary.

        Note: it exports only the indexed tables.
        """
        return self._indexed_tables

    def __repr__(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class ReadableDictAsAttribute(object):

    def __init__(self, obj: dict):
        self.__dict = obj
        self.__current = -1
        self.__items = list(sorted(self.__dict.keys()))
        if 'support_tables' in self.__dict:
            self.__dict['support_tables'] = SupportTable(
                self.__dict['support_tables'])

    @property
    def list(self):
        return list(self.__dict.keys())

    def __iter__(self):
        self.__current = -1
        return self

    def __next__(self):
        self.__current += 1
        if self.__current >= len(self.__items):
            raise StopIteration
        return self.__items[self.__current]

    def to_dict(self):
        tmp_dict = dict((key, value) for key, value in self.__dict.items())
        if 'support_tables' in self.__dict:
            tmp_dict['support_tables'] = tmp_dict['support_tables'].to_dict()
        return tmp_dict

    def __getattr__(self, name):
        return self.__dict[name]

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)
