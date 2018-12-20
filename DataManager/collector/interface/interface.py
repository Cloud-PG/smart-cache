from ..datafile.jsongz import JSONGzDataFileReader
from ..datafile.avro import AvroDataFileReader
from os import path

__all__ = ['DataFileInterface']


class DataFileInterface(object):

    """Interface for file access."""

    def __init__(self, source):
        self.__source = source
        self.__data_collector = self.__get_collector(source)
        self.__iter = None
        self.__index = 0

    @staticmethod
    def __get_collector(source):
        if path.isfile(source):
            filename, ext = path.splitext(source)
            if ext == ".gz":
                if path.splitext(filename)[1] == ".json":
                    return JSONGzDataFileReader(source)
            elif ext == ".avro":
                return AvroDataFileReader(source)

        raise Exception(
            "Collector for source:\n  -> '{}'\nis not yet implemented...".format(source))

    def get_data(self):
        for data in self.__data_collector:
            yield data

    def __getitem__(self, idx):
        return self.__data_collector[idx]

    def __iter__(self):
        """Initialize the DataFileInterface reader iterator.

        Returns:
            DataFileInterface: this object instance

        """
        self.__iter = iter(self.__data_collector)
        return self

    def __next__(self):
        """Get the next data collected.

        Returns:
            str: the JSON string

        Raises:
            StopIteration: to end the iterator

        """
        return next(self.__iter)
