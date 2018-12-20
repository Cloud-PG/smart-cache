import gzip
import json
from string import whitespace

from .utils import gen_increasing_slice

__all__ = ['JSONGzDataFileReader', 'JSONGzDataFileWriter']


class JSONGzDataFileWriter(object):

    """Write json.gz file."""

    def __init__(self, filename, data=None):
        """Init function of data writer for json.gz files.

        Args:
            filename (str): name of the json.gz file to write.
            data (str, dict, list(str), list(dict)): initial data to be inserted

        Returns:
            JSONGzDataFileWriter: the instance of this object

        """
        self.__filename = filename
        self.__descriptor = gzip.open(self.__filename, "ab")
        if data is not None:
            self.append(data)

    @staticmethod
    def __valid_json(string):
        """Check if a string is a valid JSON.

        Args:
            string (str): the string to analyse

        Returns:
            bool: if it is a valid JSON or not

        """
        try:
            json.loads(string)
        except ValueError:
            return False
        else:
            return True

    def __write(self, data):
        """Write data to the json.gz file.

        Args:
            data (str): the JSON string to write

        Returns:
            JSONGzDataFileWriter: this object instance

        """
        self.__descriptor.write(data.encode("utf-8") + b'\n')
        return self

    def append(self, data):
        """Append data to the json.gz file.

        Args:
            data (str, dict, list(str), list(dict)): data to be inserted

        Returns:
            JSONGzDataFileWriter: this object instance

        """
        if type(data) == str:
            if self.__valid_json(data):
                self.__write(data)
        elif type(data) == dict:
            self.__write(json.dumps(data))
        elif type(data) == list:
            if all(type(elm) == dict for elm in data):
                for elm in data:
                    self.__write(json.dumps(elm))
            elif all(self.__valid_json(elm) for elm in data):
                for elm in data:
                    self.__write(elm)
            else:
                raise Exception(
                    "You can pass only a list of 'dict' or JSON strings".format(type(data)))
        else:
            raise Exception(
                "'{}' is not a valid input data type".format(type(data)))

        return self

    def __del__(self):
        """Object destructor."""
        if not self.__descriptor.closed:
            self.__descriptor.close()

    def __enter__(self):
        """Initialization for 'with' statement.

        Returns:
            JSONGzDataFileWriter: this object instance

        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closing function for the 'with' statement."""
        self.__descriptor.close()


class JSONGzDataFileReader(object):

    """Read json.gz file with easy access to data."""

    def __init__(self, filename):
        """Init function of data reader for json.gz files.

        Args:
            filename (str): name of the json.gz file to open.

        Returns:
            JSONGzDataFileReader: the instance of this object

        """
        self.__filename = filename
        self.__descriptor = gzip.open(self.__filename, "rb")
        self.__last_index = -1
        self.__last_index_pos = None
        self.__whitespaces = [elm.encode("utf-8") for elm in whitespace]

    def __get_json(self):
        """Extract a json object string from the file.

        Returns:
            tuple: (dict, int) The JSON object converted in a
                   dictionary and the position of that object
                   in the file

        """
        buffer = b''
        tmp_p = 0
        start = self.__descriptor.tell()
        for cur_char in iter(lambda: self.__descriptor.read(1), b''):
            buffer += cur_char

            if cur_char == b'{':
                tmp_p += 1
            elif cur_char == b'}':
                tmp_p -= 1
            elif cur_char in self.__whitespaces and tmp_p == 0:
                pass

            if tmp_p == 0 and len(buffer) >= 2:
                json_obj_dict = json.loads(buffer, encoding="utf-8")
                buffer = b''
                return json_obj_dict, start

        return (None, -1)

    def __getitem__(self, idx):
        """Select an item or a group of item from the file.

        If the idx argument is a slice it is converted to a list
        of indexes. All index lists will be processed in increasing
        order (datafile.utils.gen_increasing_slice function for more details)
        and then will be returned in the order requested by the user.

        Args:
            idx (int or slice): indexes to extract

        Returns:
            list or dict: The JSON object converted in a dictionary or a list
                          of converted JSON objects

        """
        self.__descriptor.seek(0)

        if type(idx) is slice:
            to_extract = (elm for elm in gen_increasing_slice(idx))
        else:
            to_extract = [idx]

        results = []
        cur_idx = -1

        for target_idx in to_extract:
            if self.__last_index != -1 and target_idx - self.__last_index > 1:
                self.__descriptor.seek(self.__last_index_pos)
                cur_idx = self.__last_index

            for obj, start in iter(lambda: self.__get_json(), None):
                last_obj = obj
                self.__last_index_pos = start
                self.__last_index = cur_idx

                cur_idx += 1

                if cur_idx == target_idx:
                    results.append(last_obj)
                    break

        if type(idx) is slice:
            if idx.start is not None and idx.stop is not None and idx.start > idx.stop:
                return list(reversed(results))
            return results
        else:
            return results.pop(0)

    def __iter__(self):
        """Initialize the JSONGz reader iterator.

        Returns:
            JSONGzDataFileReader: this object instance

        """
        self.__descriptor.seek(0, 0)
        return self

    def __next__(self):
        """Get the next JSON object (inside iteration).

        Returns:
            dict: The JSON object converted in a dictionary

        Raises:
            StopIteration: to end the iterator

        """
        next_json, _ = self.__get_json()
        if next_json is not None:
            return next_json
        else:
            raise StopIteration

    def __del__(self):
        """Object destructor."""
        if not self.__descriptor.closed:
            self.__descriptor.close()

    def __enter__(self):
        """Initialization for 'with' statement.

        Returns:
            JSONGzDataFileReader: this object instance

        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closing function for the 'with' statement."""
        self.__descriptor.close()


if __name__ == "__main__":
    ##
    # Test DataFileWriter
    with JSONGzDataFileWriter("test.json.gz", ['{"a": 2}']) as data:
        data.append(json.dumps({}))
        data.append([json.dumps({})])
        data.append([{}, {"a": 2}, {}])

    ##
    # Test JSONGzDataFileReader
    with JSONGzDataFileReader("test.json.gz") as data:
        for obj in data:
            print(obj)

        print(data[0] == data[4])

    for data_ in JSONGzDataFileReader("test.json.gz"):
        print(data_)
