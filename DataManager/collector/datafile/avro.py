import json
from io import BytesIO, IOBase

from fastavro import writer as fast_writer
from fastavro import reader as fast_reader
from fastavro import parse_schema

from .utils import gen_increasing_slice

__all__ = ['AvroDataFileReader']


class AvroDataFileWriter(object):

    """Write an avro file."""

    def __init__(self, filename, data=None, schema=None, codec: str = 'snappy'):
        """Create an avro archive.

        Note:
            Specification: https://avro.apache.org/docs/1.8.2/spec.html

        Args:
            filename (str): the output filename
            data (dict, list(dict)): the data to write
            schema (dict): the avro schema as dictionary

        Returns:
            AvroDataFileWriter: the instance of this object
        """
        self.__filename = filename
        self.__descriptor = open(self.__filename, 'wb')
        self.__schema = None
        self.__codec = codec
        if schema:
            self.__schema = json.dumps(schema).encode("utf-8")
        if data is not None:
            self.append(data)

    @property
    def raw_data(self):
        self.__descriptor.seek(0, 0)
        return self.__descriptor.read()

    @staticmethod
    def __get_primitive_type(key, value):
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "int"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, bytes):
            return "bytes"

    def __get_schema(self, data):
        """Generate an avro schema from data automatically.

        NOTE:
            Only supports basic types and arrays.
        """
        schema = {
            'namespace': "datamanager.avro",
            'type': "record",
            'name': "data",
            'fields': []
        }
        for key, value in data.items():
            prim_type = self.__get_primitive_type(value)
            if prim_type is not None:
                schema['fields'].append(
                    {'name': key, 'type': prim_type}
                )
            else:
                if isinstance(value, list):
                    prim_type = self.__get_primitive_type(value[0])
                    schema['fields'].append(
                        {'name': key, 'type': "array", 'items': prim_type}
                    )
                elif isinstance(value, dict):
                    prim_type = self.__get_primitive_type(value.values()[0])
                    schema['fields'].append(
                        {'name': key, 'type': "map", 'items': prim_type}
                    )
                else:
                    raise Exception(
                        "Type '{}' of field '{}' is not supported...".format(type(value), key))

        return json.dumps(schema).encode("utf-8")

    def __write(self, data):
        """Write data into the avro file."""
        if not self.__schema:
            self.__schema = parse_schema(self.__get_schema(data[0]))

        fast_writer(self.__descriptor, self.__schema, data, self.__codec)

    def append(self, data):
        """Add data to the avro archive."""
        if isinstance(data, dict):
            self.__write([data])
        elif isinstance(data, list):
            if all(isinstance(elm, dict) for elm in data):
                self.__write(data)
            else:
                raise Exception(
                    "You can pass only a list of 'dict'"
                )
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
            JSONDataFileWriter: this object instance

        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closing function for the 'with' statement."""
        if not self.__descriptor.closed:
            self.__descriptor.close()


class AvroDataFileReader(object):

    """Read an avro file."""

    def __init__(self, file_):
        """Init function of data reader for .avro files.

        Args:
            file_ (str): name of the .avro file to read.

        Returns:
            AvroDataFileReader: the instance of this object

        """
        self.__descriptor = None
        if isinstance(file_, str):
            self.__descriptor = open(file_, 'rb')
        elif isinstance(file_, (BytesIO, IOBase)):
            self.__descriptor = file_
        else:
            raise Exception(
                "Type '{}' for file_ is not supported...".format(type(file_)))
        self.__avro_iter = None
        self.__len = None

    def __len__(self):
        if not self.__len:
            counter = 0
            self.__descriptor.seek(0, 0)
            print("[Check avro lenght...]")
            for _ in fast_reader(self.__descriptor):
                counter += 1
            self.__len = counter
        return self.__len

    @property
    def raw_data(self):
        self.__descriptor.seek(0, 0)
        return self.__descriptor.read()

    def __getitem__(self, idx):
        """Select an item or a group of item from the file.

        If the idx argument is a slice it is converted to a list
        of indexes. All index lists will be processed in increasing
        order (datafile.utils.gen_increasing_slice function for more details)
        and then will be returned in the order requested by the user.

        Args:
            idx (int or slice): indexes to extract

        Returns:
            list or dict: a dictionary or a list of dictionary

        """
        assert isinstance(
            idx, (int, slice)), "Index Could be an integer or a slice"

        if isinstance(idx, slice):
            to_extract = [elm for elm in gen_increasing_slice(idx)]
        else:
            to_extract = [idx]

        results = []
        cur_idx = -1
        self.__descriptor.seek(0, 0)
        self.__avro_iter = fast_reader(self.__descriptor)

        while len(to_extract):
            try:
                cur_elm = next(self.__avro_iter)
            except StopIteration:
                raise IndexError

            cur_idx += 1

            if cur_idx == to_extract[0]:
                results.append(cur_elm)
                to_extract.pop(0)

        if isinstance(idx, slice):
            if idx.start is not None and idx.stop is not None and idx.start > idx.stop:
                return list(reversed(results))
            return results
        else:
            return results.pop(0)

    def __iter__(self):
        """Initialize the Avro reader iterator.

        Returns:
            AvroDataFileReader: this object instance

        """
        self.__descriptor.seek(0, 0)
        self.__avro_iter = fast_reader(self.__descriptor)
        return self

    def __next__(self):
        """Get the next Avro object (inside iteration).

        Returns:
            dict: The avro object converted in a dictionary

        Raises:
            StopIteration: to end the iterator

        """
        return next(self.__avro_iter)

    def __del__(self):
        """Object destructor."""
        if not self.__descriptor.closed:
            self.__descriptor.close()

    def __enter__(self):
        """Initialization for 'with' statement.

        Returns:
            AvroDataFileReader: this object instance

        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closing function for the 'with' statement."""
        if not self.__descriptor.closed:
            self.__descriptor.close()
