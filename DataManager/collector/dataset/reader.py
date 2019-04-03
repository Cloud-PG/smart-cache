from ..datafile.json import JSONDataFileReader

class CMSDatasetV0Reader(object):

    def __init__(self, filename):
        self._collector = JSONDataFileReader(filename)