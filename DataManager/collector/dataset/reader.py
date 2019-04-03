import matplotlib.pyplot as plt
import numpy as np

from ..datafile.json import JSONDataFileReader


class ReadableDictAsAttribute(object):

    def __init__(self, obj: dict):
        self.__dict = obj

    def __getattr__(self, name):
        return self.__dict[name]


class CMSDatasetV0Reader(object):

    def __init__(self, filename):
        self._collector = JSONDataFileReader(filename)
        self._meta = ReadableDictAsAttribute(self._collector[0])
        self._use_feature_support = self.meta.support_tables.get(
            'features', False)
        self.__features = None
        self.__feature_order = None
        self._collector.start_from(1)  # Skip metadata
        self._score_avg = None

    def __len__(self):
        return self.meta.len

    def __getitem__(self, index):
        if self._use_feature_support:
            if not self.__features:
                self.__features = {}
                self.__feature_order = set()
                for name, table in self.meta.support_tables['features'].items():
                    self.__features[name] = dict(
                        (value, key)
                        for key, value in table.items()
                    )
                    self.__feature_order |= set((name, ))
                self.__feature_order = sorted(self.__feature_order)
            res = self._collector[index]
            if isinstance(res, list):
                for idx, record in enumerate(res):
                    res[idx] = np.array([
                        float(
                            self.__features[feature_name]
                            [record['features']
                             [feature_name]]
                        )
                        for feature_name in self.__feature_order]
                    )
                return np.array(res)
            else:
                return np.array([
                    float(
                        self.__features[feature_name]
                        [res['features']
                         [feature_name]]
                    )
                    for feature_name in self.__feature_order
                ])
        else:
            return self._collector[index]

    def features(self):
        return self[:len(self)]

    def labels(self, one_hot=True):
        labels = []
        for score in list(self.scores):
            res = np.zeros((2,))
            if one_hot:
                res[int(score >= self.score_avg)] = 1
                labels.append(res)
            else:
                labels.append(score >= self.score_avg)
        return np.array(labels)

    def toggle_feature_support(self):
        self._use_feature_support = not self._use_feature_support

    @property
    def meta(self):
        return self._meta

    @property
    def records(self):
        for record in self._collector:
            yield record

    @property
    def scores(self):
        for record in self.records:
            yield record['score']

    @property
    def score_avg(self):
        if not self._score_avg:
            self._score_avg = sum(self.scores) / len(self)
        return self._score_avg

    def scores_show(self):
        scores = list(self.scores)
        avg = self.score_avg
        plt.plot(range(len(scores)), scores, label="scores")
        plt.plot(range(len(scores)), [
                 avg for _ in range(len(scores))], label="avg")
        plt.legend()
        plt.show()
