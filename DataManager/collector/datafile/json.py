import json
from string import whitespace
from types import GeneratorType

from .utils import gen_increasing_slice, get_stream

__all__ = ['JSONDataFileReader', 'JSONDataFileWriter']


class JSONDataFileWriter(object):

    """Write json.gz file."""

    def __init__(self, filename, data=None, append: bool=False):
        """Init function of data writer for json.gz files.

        Args:
            filename (str): name of the json.gz file to write.
            data (str, dict, list(str), list(dict)): initial data to be inserted

        Returns:
            JSONDataFileWriter: the instance of this object

        """
        self.__filename = filename
        self.__descriptor = None
        if append:
            self.__descriptor = get_stream(self.__filename, "ab")
            self.__descriptor.seek(0, 2)
        else:
            self.__descriptor = get_stream(self.__filename, "wb")
        if data is not None:
            self.append(data)

    @staticmethod
    def __valid_json(string):
        """Check if a string is a valid json.

        Args:
            string (str): the string to analyse

        Returns:
            bool: if it is a valid JSON or not

        """
        try:
            obj = json.loads(string)
        except ValueError:
            return False
        else:
            return json.dumps(obj)

    def __write(self, data):
        """Write data to the json.gz file.

        Args:
            data (str): the JSON string to write

        Returns:
            JSONDataFileWriter: this object instance

        """
        self.__descriptor.write(data.encode("utf-8") + b'\n')
        return self.__descriptor.tell()

    def append(self, data):
        """Append data to the json.gz file.

        Args:
            data (str, dict, list(str), list(dict)): data to be inserted

        Returns:
            JSONDataFileWriter: this object instance

        """
        if isinstance(data, str):
            return self.__write(self.__valid_json(elm))
        elif isinstance(data, dict):
            return self.__write(json.dumps(data))
        elif isinstance(data, (list, GeneratorType)):
            for elm in data:
                if isinstance(elm, dict):
                    return self.__write(json.dumps(elm))
                elif self.__valid_json(elm) != False:
                    return self.__write(self.__valid_json(elm))
                else:
                    raise Exception(
                        "You can pass only a list of 'dict' or JSON strings".format(type(data)))
        else:
            raise Exception(
                "'{}' is not a valid input data type".format(type(data)))

    def __del__(self):
        """Object destructor."""
        if not self.__descriptor.closed:
            self.__descriptor.close()

    def __enter__(self):
        """Initialization for 'with' statement.

        Returns:
            JSONDataFileWriter: this object instance

        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closing function for the 'with' statement."""
        self.__descriptor.close()


class JSONDataFileReader(object):

    """Read json.gz file with easy access to data."""

    def __init__(self, filename):
        """Init function of data reader for json.gz files.

        Args:
            filename (str): name of the json.gz file to open.

        Returns:
            JSONDataFileReader: the instance of this object

        """
        self.__filename = filename
        self.__descriptor = get_stream(self.__filename)
        self.__last_index = -1
        self.__last_index_pos = None
        self.__len = None
        self.__whitespaces = [elm.encode("utf-8") for elm in whitespace]
        self.__getitem_start = 0

    def __len__(self):
        if not self.__len:
            self.__descriptor.seek(0, 0)
            num_lines = self.__descriptor.read().decode("utf-8").count('\n')
            self.__len = num_lines
        return self.__len

    def __get_json_from_end(self, step: int=512):
        buffer = b''
        index = -512 - 1
        cur_chars = b''
        while cur_chars.rfind(b'\n') == -1:
            self.__descriptor.seek(index, 2)
            last_pos = self.__descriptor.tell()
            cur_chars = self.__descriptor.read(step)
            buffer = cur_chars + buffer
            print(last_pos, cur_chars, buffer)
            index -= step
        if len(buffer) >= 2:
            return buffer, last_pos + cur_chars.rfind(b'\n')
        else:
            return (None, -1)

    def __get_json(self):
        """Extract a json object string from the file.

        Returns:
            tuple: (dict, int) The JSON object converted in a
                   dictionary and the position of that object
                   in the file

        """
        buffer = b''
        start = self.__descriptor.tell()
        for cur_char in iter(lambda: self.__descriptor.read(1), b''):
            buffer += cur_char
            if cur_char == b'\n' and len(buffer) >= 2:
                return buffer, start

        return (None, -1)

    def start_from(self, index: int):
        """Set the cursor to a specific object index to start.

        Returns:
            dict: the last object extracted
        """
        if index < 0:
            raise Exception("Index have to be positive or equal to 0...")
        self.__descriptor.seek(0, 0)
        pos = self.__descriptor.tell()
        for _ in range(index):
            last_obj, _ = self.__get_json()
            pos = self.__descriptor.tell()
        self.__getitem_start = pos
        return json.loads(last_obj, encoding="utf-8")

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
        assert isinstance(
            idx, (int, slice)), "Index Could be an integer or a slice"

        if isinstance(idx, int) and idx < 0:
            for cur_index in range(-idx):
                obj, pos = self.__get_json_from_end()
                if -cur_index == idx:
                    return json.loads(obj, encoding="utf-8")
            raise IndexError

        self.__descriptor.seek(self.__getitem_start)

        if isinstance(idx, slice):
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
                    results.append(json.loads(last_obj, encoding="utf-8"))
                    break

        if isinstance(idx, slice):
            if idx.start is not None and idx.stop is not None and idx.start > idx.stop:
                return list(reversed(results))
            return results
        else:
            return results.pop(0)

    def __iter__(self):
        """Initialize the JSON reader iterator.

        Returns:
            JSONDataFileReader: this object instance

        """
        self.__descriptor.seek(self.__getitem_start, 0)
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
            return json.loads(next_json, encoding="utf-8")
        else:
            raise StopIteration

    def __del__(self):
        """Object destructor."""
        if not self.__descriptor.closed:
            self.__descriptor.close()

    def __enter__(self):
        """Initialization for 'with' statement.

        Returns:
            JSONDataFileReader: this object instance

        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closing function for the 'with' statement."""
        self.__descriptor.close()


if __name__ == "__main__":
    ##
    # Test DataFileWriter

    for data_ in JSONDataFileReader("test.json.gz"):
        print(data_)
