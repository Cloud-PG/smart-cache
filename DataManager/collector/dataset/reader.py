import json

import matplotlib.pyplot as plt
import numpy as np

from ..datafile.json import JSONDataFileReader
from .utils import ReadableDictAsAttribute


class CMSDatasetV0Reader(object):

    def __init__(self, filename):
        self._collector = JSONDataFileReader(filename)
        # Extract metadata and skip them for future reading
        self._meta = ReadableDictAsAttribute(self._collector.start_from(1))
        self._use_tensor = True
        self.__features = None
        self.__feature_order = None
        self._score_avg = None

    def get_raw(self, index, next_window: bool=False):
        if not next_window:
            if index >= self._meta.len_raw_week:
                raise IndexError("Index {} out of bound for window that has size {}".format(
                    index, self._meta.len_raw_week
                ))
        else:
            if index >= self._meta.len_raw_next_week:
                raise IndexError("Index {} out of bound for next window that has size {}".format(
                    index, self._meta.len_raw_next_week
                ))
        start = self._meta.raw_week_start if not next_window else self._meta.len_raw_next_week
        return self._collector[start + index]

    def __len__(self):
        return self.meta.len

    def __getitem__(self, index):
        if self._use_tensor:
            res = self._collector[index]
            if isinstance(res, list):
                return np.array([elm['tensor'] for elm in res])
            else:
                return np.array(res['tensor'])
        else:
            return self._collector[index]

    def train_set(self, one_hot: bool=True):
        return self.features(), self.labels(one_hot=one_hot)

    def features(self):
        return self[:len(self)]

    def labels(self, one_hot: bool=True):
        labels = []
        for score in list(self.scores):
            res = np.zeros((2,))
            if one_hot:
                res[int(score >= self.score_avg)] = 1
                labels.append(res)
            else:
                labels.append(int(score >= self.score_avg))
        return np.array(labels)

    def toggle_feature_support(self):
        self._use_tensor = not self._use_tensor

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
