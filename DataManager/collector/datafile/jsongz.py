import gzip
import json


__all__ = ['DataFileReader']


class DataFileReader(object):

    """Read json.gz file with easy access to data."""

    def __init__(self, filename):
        """Init function of data reader for json.gz files.

        Args:
            filename (str): name of the json.gz file to open.

        Returns:
            DataFileReader: the instance of this object

        """
        self.__filename = filename
        self.__descriptor = gzip.open(self.__filename, "rb")
        self.__last_index = -1
        self.__last_index_pos = None

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
        for cur_char in iter(lambda: self.__descriptor.read(1), ''):
            buffer += cur_char

            if cur_char == b'{':
                tmp_p += 1
            elif cur_char == b'}':
                tmp_p -= 1

            if tmp_p == 0 and len(buffer) >= 2:
                json_obj_dict = json.loads(buffer, encoding="utf-8")
                buffer = b''
                return json_obj_dict, start

    @staticmethod
    def __gen_increasing_slice(slice):
        """Generate a sequence of indexes from a slice.

        Args:
            slice (slice): the slice object to expand

        Returns:
            generator: the indexes in increasing order

        """
        start = slice.start if slice.start else 0
        stop = slice.stop
        step = slice.step if slice.step else 1
        if start > stop:
            start, stop = stop + 1, start + 1
        cur = start
        while cur < stop:
            assert cur >= 0, "Negative index not supported..."
            yield cur
            cur += step

    def __getitem__(self, idx):
        """Select an item or a group of item from the file.

        If the idx argument is a slice it is converted to a list
        of indexes. All index lists will be processed in increasing
        order (see __gen_increasing_slice function for more details)
        and then will be returned in the order requested by the user.

        Args:
            idx (int or slice): indexes to extract

        Returns:
            list or str: a single JSON string or a list of JSON strings

        """
        self.__descriptor.seek(0)

        if type(idx) is slice:
            to_extract = (elm for elm in self.__gen_increasing_slice(idx))
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
            if idx.start > idx.stop:
                return list(reversed(results))
            return results
        else:
            return results.pop(0)

    def __iter__(self):
        """Initialize the JSONGz reader iterator.

        Returns:
            DataFileReader: this object instance

        """
        self.__descriptor.seek(0, 0)
        return self

    def __next__(self):
        """Get the next JSON object string (inside iteration).

        Returns:
            str: the JSON string

        Raises:
            StopIteration: to end the iterator

        """
        next_json, _ = self.__get_json()
        if next_json:
            return next_json
        else:
            raise StopIteration()

    def __del__(self):
        """Object destructor."""
        if not self.__descriptor.closed:
            self.__descriptor.close()

    def __enter__(self):
        """Initialization for 'with' statement.

        Returns:
            DataFileReader: this object instance

        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closing function for the 'with' statement."""
        self.__descriptor.close()


if __name__ == "__main__":
    ##
    # Tests

    from sys import argv

    with DataFileReader(argv[1]) as data:
        print(data[2])
        print(data[2:0][0])
        print("EQUAL?", data[2] == data[2:0][0])
        print("EQUAL?", data[1] == data[2:0][1])
        print("EQUAL?", data[10] == data[10:0][0])
        print("===")
        _10 = data[10]
        __10 = data[10:0][0]
        print(_10)
        print(__10)
        print(_10 == __10)
        print("EQUAL?", data[10] == data[10:0][0])
        data[0:2]
        data[3]

        data[10]
        for data_ in data:
            print(data_)
            break

        print("EQUAL?", data[0] == data[10:-1][-1])

        print(data[0])
        print("---")
        data[1]
        data[2]
        data[1]
        data[2]
        print("---")
        print("EQUAL?", data[10] == data[10:0][0])
        print("---")
        print(data[20])
        print("---")
        print(data[100])
        print("---")

    for data_ in DataFileReader(argv[1]):
        print(data_)
        break
