import gzip
import json
from os import path, walk

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tensorflow import keras
from tqdm import tqdm
from yaspin import yaspin
from yaspin.spinners import Spinners

from ..datafeatures.extractor import CMSRecordTest0
from ..datafile.json import JSONDataFileReader
from .utils import ReadableDictAsAttribute, SupportTable


class SimulatorDatasetReader(object):

    def __init__(self, filename: str = "", to_categorical: bool = False):
        self._df = None
        self._data = (None, None)
        self._data_dir = None
        self._converter_map = {}

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

    def make_converter_map(self, columns: list = [],
                           unknown_values: bool = True,
                           sort_values: bool = False,
                           sort_type=int,
                           ) -> 'SimulatorDatasetReader':

        for column in columns:
            if column not in self._converter_map:
                self._converter_map[column] = {
                    'feature': column,
                    'keys': [],
                    'values': {},
                    'unknown_values': unknown_values
                }
            cur_map = self._converter_map[column]
            cur_values = set(self._df[column].astype(str).to_list())
            for cur_value in tqdm(cur_values, desc=f"Make map of {column}",
                                  ascii=True):
                if cur_value not in cur_map['keys']:
                    cur_map['keys'].append(cur_value)
            if sort_values:
                cur_map['keys'] = list(sorted(cur_map['keys'],
                                              key=lambda elm: sort_type(elm)))

            cur_map['values'] = dict(
                (name, idx) for idx, name
                in enumerate(cur_map['keys'],
                             1 if unknown_values else 0)
            )
        return self

    def store_converter_map(self,
                            out_filename: str = "featureConverter.json.gzip"
                            ):
        with yaspin(
            Spinners.bouncingBall,
            f"[Save converter map: {path.join(self._data_dir, out_filename)}]"
        ) as sp:
            with gzip.GzipFile(path.join(self._data_dir, out_filename), "wb") as outfile:
                outfile.write(json.dumps(
                    self._converter_map,
                    indent=2).encode("utf-8")
                )
        return self

    @staticmethod
    def __get_category_value(category_obj, value) -> int:
        if value in category_obj['keys']:
            return category_obj['values'][value]
        if category_obj['unknown_values']:
            return 0

    def make_data_and_labels(self, data_columns: list = [],
                             label_column: str = "",
                             for_cnn: bool = False,
                             ) -> 'SimulatorDatasetReader':
        df = self._df

        with yaspin(
            Spinners.bouncingBall,
            f"[Make data and labels][{','.join(data_columns)} -> {label_column}]"
        ) as sp:
            sp.text = f"[Prepare labels][Column: {label_column}]"
            if not label_column:
                label_column = df.columns[-1]
            if label_column in self._converter_map:
                tmp_labels = [
                    self._converter_map[label_column]['values'][elm]
                    for elm in df[label_column].astype(str)
                ]
                labels = np.zeros(
                    (
                        len(tmp_labels),
                        len(self._converter_map[label_column]['keys'])
                    ),
                    dtype='int32'
                )
                labels[np.arange(len(tmp_labels)), tmp_labels] = 1
            else:
                labels = df[label_column].to_numpy()

            new_df = None
            for column in data_columns:
                sp.text = f"[Prepare data][Column: {column}]"
                if column in self._converter_map:
                    cur_map = self._converter_map[column]
                    if cur_map['unknown_values']:
                        num_categories = len(cur_map['keys']) + 1
                    else:
                        num_categories = len(cur_map['keys'])
                    categories = []
                    for value in df[column]:
                        cur_value = self.__get_category_value(
                            cur_map,
                            value
                        )
                        tmp = np.zeros(num_categories, dtype='float32')
                        if not cur_map['unknown_values']:
                            cur_value -= 1
                        tmp[cur_value] = 1.0
                        categories.append(
                            tmp.tolist()
                        )
                    cur_data = pd.DataFrame(
                        categories,
                        columns=[
                            f"{column}_{idx}"
                            for idx in range(num_categories)
                        ]
                    )
                else:
                    cur_data = df[column]

                sp.text = "[Merge data]"
                if new_df is not None:
                    new_df = new_df.join(cur_data, how='outer')
                else:
                    new_df = cur_data

            sp.text = "[Convert data]"
            if for_cnn:
                new_df = new_df.to_numpy()
                new_df = new_df.reshape(
                    new_df.shape[0], new_df.shape[1], 1
                )
            else:
                new_df = new_df.to_numpy()

            self._data = (new_df, labels)

        print(f"[Data shape: {new_df.shape}][Labels shape: {labels.shape}]")
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
