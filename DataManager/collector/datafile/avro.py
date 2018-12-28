from avro.datafile import DataFileReader
from avro.io import DatumReader

from .utils import gen_increasing_slice

__all__ = ['DataFileReader']


class AvroDataFileReader(object):

    """Write avro file."""

    def __init__(self, filename):
        """Init function of data reader for .avro files.

        Args:
            filename (str): name of the .avro file to read.

        Returns:
            AvroDataFileReader: the instance of this object

        """
        self.__filename = filename
        self.__descriptor = open(filename, 'rb')
        self.__avro_file = None
        self.__avro_iter = None

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
        assert isinstance(idx, [int, slice]), "Index Could be an integer or a slice"

        self.__avro_file = DataFileReader(self.__descriptor, DatumReader())

        if isinstance(idx, slice):
            to_extract = [elm for elm in gen_increasing_slice(idx)]
        else:
            to_extract = [idx]

        results = []
        cur_idx = -1
        self.__avro_iter = iter(self.__avro_file)

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
        """Initialize the JSONGz reader iterator.

        Returns:
            AvroDataFileReader: this object instance

        """
        self.__avro_file = DataFileReader(self.__descriptor, DatumReader())
        self.__avro_iter = iter(self.__avro_file)
        return self

    def __next__(self):
        """Get the next JSON object (inside iteration).

        Returns:
            dict: The JSON object converted in a dictionary

        Raises:
            StopIteration: to end the iterator

        """
        return next(self.__avro_iter)

    def __del__(self):
        """Object destructor."""
        self.__avro_file.close()
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
        self.__avro_file.close()
