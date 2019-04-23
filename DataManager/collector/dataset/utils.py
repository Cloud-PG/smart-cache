
import json
from datetime import date, timedelta

from tqdm import tqdm


def gen_window_dates(year: int, month: int, day: int, window_size: int, step: int=1, next_window: bool=False):
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


class SupportTable(object):

    """Class to manage support tables for feature conversions."""

    def __init__(self, support_table: dict=None):
        self._tables = {}
        self._indexed_tables = {}
        self.filters = ReadableDictAsAttribute({
            'split_process': self._filter_split_process
        })
        self.__sorted_keys = {}
        self.__sizes = {}
        if support_table:
            self._indexed_tables = support_table
            for table_name, table in self._indexed_tables.items():
                self._tables[table_name] = {}
                for key in table.keys():
                    self._tables[table_name][key] = set(table[key].keys())

    def close_conversion(self, table_name: str, data: dict, normalized: bool=True, one_hot_categories: bool=False):
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
        assert normalized != one_hot_categories, "You can choose normalized or one hot features..."
        if normalized:
            for idx, value in enumerate(res):
                res[idx] = float(value / sizes[idx])
        elif one_hot_categories:
            tmp = []
            for idx, key in enumerate(sorted_keys):
                inner_tmp = [
                    0. for _ in range(
                        sizes[key]
                    )
                ]
                inner_tmp[res[idx]] = 1.
                for elm in inner_tmp:
                    tmp.append(elm)
            res = tmp
        return res

    @staticmethod
    def _filter_split_process(process: str):
        tmp = " ".join(process.split("-"))
        tmp = " ".join(tmp.split("_"))
        return tmp.split()

    def reduce_categories(self, table_name: str, target, filter_=None, lvls: int=0):
        assert filter_ is not None, "You need to specify a filter"
        reduced_set = {}
        categories = list(
            elm for elm in sorted(
                self._tables[table_name][target]
            ) if elm != "__unknown__"
        )
        for category in tqdm(categories, desc="Get category '{}'".format(target)):
            cur_category = filter_(category)
            cur_lvl = reduced_set
            for word in cur_category:
                if word not in cur_lvl:
                    cur_lvl[word] = {'times': 0}
                cur_lvl[word]['times'] += 1
                cur_lvl = cur_lvl[word]

        result = set()
        cur_lvl = reduced_set
        for key, value in tqdm(cur_lvl.items(), desc="Reduce category '{}'".format(target)):
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

    @property
    def list(self):
        return list(self._indexed_tables.keys())

    def __getattr__(self, name):
        if name in self._indexed_tables:
            return self._indexed_tables[name]
        raise AttributeError(name)

    def insert(self, table_name: str, key, value, with_unknown: bool=True):
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

    def __repr__(self):
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
