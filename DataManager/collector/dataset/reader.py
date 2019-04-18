import json

import matplotlib.pyplot as plt
import numpy as np

from ..datafile.json import JSONDataFileReader
from .utils import ReadableDictAsAttribute, SupportTable


class CMSDatasetV0Reader(object):

    def __init__(self, filename):
        print("[Open dataset: {}]".format(filename))
        self._collector = JSONDataFileReader(filename)
        self._meta = ReadableDictAsAttribute(self._collector[-1])
        if 'checkpoints' in self._meta:
            print("[Load checkpoints]")
            for index, pos in self._meta.checkpoints.items():
                self._collector.add_checkpoint(int(index), pos)
        self._use_tensor = True
        self._score_avg = None
        self.__sorted_keys = None
        print("[Dataset loaded...]")

    def get_raw_window(self):
        for record in self._collector.start_from(
            self._meta.raw_window_start, 
            self._meta.raw_window_start + self._meta.len_raw_window
        ):
            yield record
    
    def get_raw_next_window(self):
        for record in self._collector.start_from(
            self._meta.raw_next_window_start, 
            self._meta.raw_next_window_start + self._meta.len_raw_next_window
        ):
            yield record

    def get_raw(self, index, next_window: bool=False, as_tensor: bool=False):
        assert index >= 0, "Index of raw data cannot be negative"
        if not next_window:
            if index >= self._meta.len_raw_window:
                raise IndexError("Index {} out of bound for window that has size {}".format(
                    index, self._meta.len_raw_window
                ))
        else:
            if index >= self._meta.len_raw_next_window:
                raise IndexError("Index {} out of bound for next window that has size {}".format(
                    index, self._meta.len_raw_next_window
                ))
        start = self._meta.raw_window_start if not next_window else self._meta.raw_next_window_start
        res = self._collector[start + index]
        if not as_tensor:
            return res
        return np.array(res['tensor'])

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
        features = []
        for record in self.records:
            if self._use_tensor:
                features.append(np.array(record['tensor']))
            else:
                np.array(record['features'])
        return np.array(features)

    def labels(self, one_hot: bool=True):
        labels = []
        for score in self.scores:
            res = np.zeros((2,))
            if one_hot:
                res[int(score >= self.score_avg)] = 1
                labels.append(res)
            else:
                labels.append(int(score >= self.score_avg))
        return np.array(labels)

    def toggle_tensor(self):
        self._use_tensor = not self._use_tensor

    @property
    def meta(self):
        return self._meta

    @property
    def records(self):
        for record in self._collector[:self._meta.len]:
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

    def score_show(self):
        scores = list(self.scores)
        avg = self.score_avg
        plt.plot(range(len(scores)), scores, label="scores")
        plt.plot(range(len(scores)), [
                 avg for _ in range(len(scores))], label="avg")
        plt.legend()
        plt.show()
