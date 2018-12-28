
from .common import DataConverter

__all__ = ['CSVConverter']


class CSVConverter(DataConverter):

    def row(self, data):
        if isinstance(data, dict):
            features = [str(data[key]) for key in sorted(data.keys())]
        else:
            features = [str(elm) for elm in data]
        
        return ",".join(features)

    def labeled_row(self, data, label='label'):
        return None
