
import bz2
import gzip
import json
from os import path

__all__ = ['gen_increasing_slice',
           'get_or_create_descriptor', 'AvroObjectTranslator']


def gen_increasing_slice(slice):
    """Generate a sequence of indexes from a slice.

    Args:
        slice (slice): the slice object to expand

    Returns:
        generator: the indexes in increasing order

    """
    start = slice.start if slice.start else 0
    stop = slice.stop
    step = slice.step if slice.step else 1
    if stop is None:
        raise Exception("Slice with None stop is not supported...")
    if start > stop:
        start, stop = stop + 1, start + 1
    cur = start
    while cur < stop:
        assert cur >= 0, "Negative index not supported..."
        yield cur
        cur += step


def get_or_create_descriptor(filename, open_mode='rb'):
    """Open a stream to write or read data.

    Depending on the file requested it opens a different
    file descriptor, such a gzip file or bzip file descriptor.

    Args:
        filename (str): the file to open
        open_mode (str): the mode with which open the file

    Returns:
        file_descriptor

    """
    body, ext_0 = path.splitext(filename)
    body, ext_1 = path.splitext(body)
    if ext_1 == '.json':
        if ext_0 == ".gz":
            stream = gzip.GzipFile(filename, mode=open_mode)
        elif ext_0 == ".bz2":
            stream = bz2.BZ2File(filename, mode=open_mode)
        else:
            raise Exception(
                "Compression extension '{}' not supported...".format(ext_0))
    elif ext_0 == '.json':
        stream = open(filename, mode=open_mode)
    else:
        raise Exception(
            "Stream format '{}' not supported...".format(ext_0))
    return stream


class AvroObjectTranslator(object):

    def deduce_scheme(self, obj, lvl: int = 0):

        primitive_type = self.__get_primitive_type(obj)
        if primitive_type != "complex":
            return {'type': primitive_type}

        complex_type = self.__get_complex_type(obj)
        if complex_type == 'array':
            return {'type': complex_type, 'items': self.__get_primitive_type(obj[0])}
        elif complex_type == 'map':
            return {'type': complex_type, 'values': self.__get_primitive_type(list(obj.values())[0])}
        else:
            schema = {
                "namespace": "translator.avro",
                'type': "record",
                'name': "data_lvl_{}".format(lvl),
                'fields': []
            }
            for key, value in obj.items():
                schema['fields'].append(
                    {"name": key, "type": self.deduce_scheme(value, lvl + 1)}
                )
            return schema

    @staticmethod
    def __get_primitive_type(value):
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
        if value is None:
            return "null"
        else:
            return "complex"

    @staticmethod
    def __get_complex_type(obj):
        if isinstance(obj, dict):
            first_type = type(list(obj.values())[0])
            if all([type(elm) == first_type for elm in obj.values()]):
                return "map"
            else:
                return "record"
        elif isinstance(obj, list):
            first_type = type(obj[0])
            if all([type(elm) == first_type for elm in obj]):
                return "array"
            else:
                return "record"
        else:
            raise Exception(
                "Unknown type '{}' of object {}...".format(type(obj), obj)
            )
