
from tqdm import tqdm

class SupportTable(object):

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


class ReadableDictAsAttribute(object):

    def __init__(self, obj: dict):
        self.__dict = obj
        if 'support_tables' in self.__dict:
            self.__dict['support_tables'] = SupportTable(
                self.__dict['support_tables'])

    @property
    def list(self):
        return list(self.__dict.keys())

    def __getattr__(self, name):
        return self.__dict[name]

    def __repr__(self):
        return json.dumps(self.__dict, indent=2)
