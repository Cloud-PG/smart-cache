from io import BytesIO
from os import path

from .datafile.avro import AvroDataFileReader, AvroDataFileWriter
from .datafile.json import JSONDataFileReader, JSONDataFileWriter
from tqdm import tqdm

__all__ = ['DataFile']


class DataFile(object):

    """Interface for file access."""

    def __init__(self, source):
        self.__source = source
        self.__data_collector = self.__get_collector(source)
        self.__iter = None
        self.__index = 0

    def __len__(self):
        return len(self.__data_collector)

    def __setstate__(self, state):
        cur_source = state['source']
        self.__source = cur_source
        self.__data_collector = self.__get_collector(cur_source)

    def __getstate__(self):
        return {
            'source': self.__source
        }

    @property
    def raw_data(self):
        return self.__data_collector.raw_data

    @staticmethod
    def __get_collector(source):
        if isinstance(source, BytesIO):
            tmp = source.read(100).decode("utf-8", errors="ignore")
            source.seek(0)
            if tmp.find("avro.schema") != -1:
                return AvroDataFileReader(source)
            else:
                return JSONDataFileReader(source)
        elif isinstance(source, AvroDataFileWriter):
            tmp = BytesIO(source.raw_data)
            return AvroDataFileReader(tmp)
        elif isinstance(source, JSONDataFileWriter):
            tmp = BytesIO(source.raw_data)
            return JSONDataFileReader(descriptor=tmp)
        elif path.isfile(source):
            filename, ext = path.splitext(source)
            if ext == ".gz" or ext == ".bz2":
                if path.splitext(filename)[1] == ".json":
                    return JSONDataFileReader(source)
                else:
                    raise Exception("Format {} is not supported...".format(
                        path.splitext(filename)[1]))
            elif ext == ".avro":
                return AvroDataFileReader(source)
            else:
                raise Exception("File type {} is not supported...".format(ext))
        elif not path.exists(source):
            raise FileNotFoundError("{}".format(source))

        raise Exception(
            "Collector for source:\n  -> '{}'\nis not yet implemented...".format(source))

    def get_chunks(self, chunksize=100):
        tmp = []
        for data in tqdm(
            self.get_data(), unit="record", desc="Chunk extraction"
        ):
            tmp.append(data)
            if len(tmp) == chunksize:
                yield tmp
                tmp = []
        if len(tmp) != 0:
            yield tmp

    def get_data(self):
        for data in self.__data_collector:
            yield data

    def __getitem__(self, idx):
        return self.__data_collector[idx]

    def __iter__(self):
        """Initialize the DataFile reader iterator.

        Returns:
            DataFile: this object instance

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
