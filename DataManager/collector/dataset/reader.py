import gzip
import json
from os import path, walk

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tensorflow import keras
from tqdm import tqdm

from ..datafeatures.extractor import CMSRecordTest0
from ..datafile.json import JSONDataFileReader
from .utils import FeatureConverter, ReadableDictAsAttribute, SupportTable
from yaspin import yaspin
from yaspin.spinners import Spinners


class SimulatorDatasetReader(object):

    def __init__(self, filename: str = "", to_categorical: bool = False):
        self._df = None
        self._data = (None, None)
        self._data_dir = None
        self._converter = None

        with yaspin(
            Spinners.bouncingBall,
            f"[Open dataset {filename}]"
        ) as sp:
            head, tail = path.splitext(filename)
            if tail in [".gzip", ".gz"]:
                head, tail = path.splitext(head)
                with gzip.GzipFile(filename, "rb") as cur_file:
                    if tail == ".feather":
                        self._df = pd.read_feather(cur_file)
                        self._data_dir = path.dirname(path.abspath(filename))
                    else:
                        raise Exception(f"Unknow extension '{tail}'")
            else:
                raise Exception(f"Unknow extension '{tail}'")
            sp.text = "[Dataset loaded...]"

    @property
    def data(self):
        return self._data

    def modify_column(self, column, function) -> 'SimulatorDatasetReader':
        self._df[column] = function(self._df[column])
        return self

    def make_converter_for(self, columns: list = [],
                           unknown_value: bool = True
                           ) -> 'SimulatorDatasetReader':
        with yaspin(
            Spinners.bouncingBall,
            f"[Make converter for (unknown value: {unknown_value})]{columns}"
        ):
            if not self._converter:
                self._converter = FeatureConverter()

            for column in columns:
                values = self._df[column].to_list()
                self._converter.insert_from_values(
                    column, values, unknown_value
                )

            self._converter.dump(
                path.join(
                    self._data_dir,
                    "featureConverter.dump.pickle"
                )
            )

        return self

    def make_data_and_labels(self, data_columns: list = [],
                             label_column: str = "",
                             for_cnn: bool = False,
                             ) -> 'SimulatorDatasetReader':
        df = self._df
        with yaspin(
            Spinners.bouncingBall,
            "[Make data and labels]]"
        ):
            if not label_column:
                label_column = df.columns[-1]
            if label_column in self._converter:
                labels = self._converter.get_categories(
                    label_column,
                    df[label_column]
                )
            else:
                labels = df[label_column].to_numpy()

            new_df = None
            for column in data_columns:
                if column in self._converter:
                    cur_data = self._converter.get_column_categories(
                        column,
                        df[column]
                    )
                else:
                    cur_data = df[column]

                if new_df is not None:
                    new_df = new_df.join(cur_data, how='outer')
                else:
                    new_df = cur_data

            if for_cnn:
                new_df = new_df.to_numpy()
                new_df = new_df.reshape(
                    new_df.shape[0], new_df.shape[1], 1
                )
            else:
                new_df = new_df.to_numpy()

            self._data = (new_df, labels)

        return self

    def save_data_and_labels(self,
                             out_name: str = "dataset.converted"
                             ) -> 'SimulatorDatasetReader':
        data, labels = self.data
        dest = path.join(self._data_dir, out_name)
        with yaspin(
            Spinners.bouncingBall,
            f"[Save data and labels][{dest}.npz]"
        ):
            np.savez(
                dest,
                data=data,
                labels=labels,
            )
        return self

    def load_data_and_labels(self,
                             in_name: str = "dataset.converted.npz"
                             ) -> 'SimulatorDatasetReader':
        with yaspin(
            Spinners.bouncingBall,
            f"[Load data and labels][{in_name}]"
        ):
            npzfiles = np.load(in_name)
            cur_dir = path.dirname(path.abspath(in_name))
            self._data_dir = cur_dir
            self._data = (npzfiles['data'], npzfiles['labels'])
        return self


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
