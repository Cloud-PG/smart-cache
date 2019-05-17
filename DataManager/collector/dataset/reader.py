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
            yield (np.array(features_list), np.array(labels), None)
        else:
            assert k_fold > 0, "k_fold argument have to be greater than 0"

            chunk_size = len(self._collector) // k_fold
            chunks_features = []
            chunks_labels = []
            tmp_features = []
            tmp_labels = []

            for record in tqdm(self._collector, desc="[Generate chunks]"):
                features, class_ = self.__translate(
                    record, normalized, one_hot)
                tmp_features.append(features)
                tmp_labels.append(class_)
                if len(tmp_features) == chunk_size:
                    chunks_features.append(tmp_features)
                    chunks_labels.append(tmp_labels)
                    tmp_features = []
                    tmp_labels = []
            else:
                if len(tmp_features) > 0:
                    chunks_features.append(tmp_features)
                    chunks_labels.append(tmp_labels)
                    tmp_features = []
                    tmp_labels = []

            for idx_fold in range(k_fold):
                yield (
                    np.array([features for idx, chunk in enumerate(
                        chunks_features) if idx != idx_fold for features in chunk]),
                    np.array([labels for idx, chunk in enumerate(
                        chunks_labels) if idx != idx_fold for labels in chunk]),
                    (np.array(chunks_features[idx_fold]),
                     np.array(chunks_labels[idx_fold]))
                )

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
