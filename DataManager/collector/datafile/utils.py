
import gzip
import bz2
from os import path

__all__ = ['gen_increasing_slice', 'get_stream']


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
    if start > stop:
        start, stop = stop + 1, start + 1
    cur = start
    while cur < stop:
        assert cur >= 0, "Negative index not supported..."
        yield cur
        cur += step


def get_stream(filename, open_mode='rb'):
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
