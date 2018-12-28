
__all__ = ['DataConverter']


class DataConverter(object):

    def row(self, data):
        raise NotImplementedError

    def labeled_row(self, data, label='label'):
        raise NotImplementedError
