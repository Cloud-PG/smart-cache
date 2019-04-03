from ..datafile.json import JSONDataFileReader

class CMSDatasetV0Reader(object):

    def __init__(self, filename):
        self._collector = JSONDataFileReader(filename)
        self._meta = self._collector[0]

    def __len__(self):
        return self._meta['len']