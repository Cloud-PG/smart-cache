from ..datafile.jsongz import DataFileReader as jsonGzReader
from os import path


class DataInterface(object):

    def __init__(self, source):
        self.__source = source
        self.__data_collector = self.__get_collector(source)
        self.__index = 0

    @staticmethod
    def __get_collector(source):
        if path.isfile(source):
            filename, ext = path.splitext(source)
            if ext == ".gz":
                if path.splitext(filename)[1] == ".json":
                    return jsonGzReader(source)

        raise Exception(
            "Collector for source:\n  -> '{}'\nis not yet implemented...".format(source))

    def get_data(self):
        for data in self.__data_collector:
            yield data

    def __iter__(self):
        """Initialize the DataInterface reader iterator.

        Returns:
            DataInterface: this object instance

        """
        self.__index = 0
        return self

    def __next__(self):
        """Get the next data collected.

        Returns:
            str: the JSON string

        Raises:
            StopIteration: to end the iterator

        """
        next_data = self.__data_collector[self.__index]
        self.__index += 1
        if next_data is not None:
            return next_data
        else:
            raise StopIteration()
