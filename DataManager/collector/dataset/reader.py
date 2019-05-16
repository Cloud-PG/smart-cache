import json

import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

from ..datafeatures.extractor import CMSRecordTest0
from ..datafile.json import JSONDataFileReader
from .utils import ReadableDictAsAttribute, SupportTable


class CMSDatasetTest0Reader(object):

    def __init__(self, filename):
        print("[Open dataset: {}]".format(filename))
        self._collector = JSONDataFileReader(filename)
        self._score_avg = 0.0
        self._support_table = SupportTable()
        print("[Dataset loaded...]")

    def __len__(self):
        return len(self._collector)

    def gen_support_table(self, reduce_categories_to_lvl: int = 0):
        categories = set()
        # Insert data
        for record in tqdm(self._collector, desc="[Gen Support Table]"):
            for key, value in record['features'].items():
                if key not in categories:
                    categories |= set((key, ))
                self._support_table.insert('features', key, value)
            if 'class' in record:
                self._support_table.insert(
                    'classes',
                    'class',
                    record['class'],
                    with_unknown=False
                )
        # Reduce categories
        for category in categories:
            self._support_table.reduce_categories(
                'features',
                category,
                filter_=self._support_table.filters.simple_split,
                lvls=reduce_categories_to_lvl
            )
        # Generate indexes
        self._support_table.gen_indexes()
        return self

    def __translate(self, record, normalized: bool = False, one_hot: bool = True, one_hot_labels: bool = False):
        features = np.array(
            self._support_table.close_conversion(
                'features',
                record['features'],
                normalized=normalized,
                one_hot=one_hot
            )
        )
        class_ = self._support_table.get_close_value(
            'classes',
            'class',
            record['class']
        )
        if one_hot_labels:
            tmp = np.zeros((self._support_table.get_len('classes', 'class'),))
            tmp[class_] = 1
            class_ = tmp
        return features, class_

    def get_num_classes(self):
        try:
            return self._support_table.get_len('classes', 'class')
        except:
            return 1

    def train_set(self, k_fold: int = 0, normalized: bool = False, one_hot: bool = True, one_hot_labels: bool = False):
        if k_fold == 0:
            features_list = []
            labels = []
            for record in tqdm(self._collector, desc="[Generate train set]"):
                features, class_ = self.__translate(
                    record, normalized, one_hot)
                features_list.append(features)
                labels.append(class_)
            return np.array(features_list), np.array(labels)
        else:
            raise Exception("K FOLD HAVE TO BE IMPLEMENTED...")

    @property
    def support_table(self):
        return self._support_table

    @property
    def scores(self):
        return (CMSRecordTest0().load(elm).score for elm in self._collector)

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

    def get_raw(self, index, next_window: bool = False, as_tensor: bool = False):
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

    def train_set(self, f_normalized: bool = True, f_one_hot_categories: bool = False, l_one_hot: bool = True):
        return self.features(f_normalized, f_one_hot_categories), self.labels(one_hot=l_one_hot)

    def features(self, normalized: bool = True, one_hot_categories: bool = False):
        features = []
        for record in self.records:
            if self._use_tensor:
                if normalized:
                    features.append(np.array(record['tensor']))
                else:
                    features.append(np.array(
                        self.meta.support_tables.close_conversion(
                            'features',
                            record['features'],
                            normalized=normalized,
                            one_hot_categories=one_hot_categories
                        )
                    ))
            else:
                np.array(record['features'])
        return np.array(features)

    def labels(self, one_hot: bool = True):
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
