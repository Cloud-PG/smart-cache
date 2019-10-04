import gzip
import json
from os import path, walk

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm

from ..datafeatures.extractor import CMSRecordTest0
from ..datafile.json import JSONDataFileReader
from .utils import ReadableDictAsAttribute, SupportTable
from tensorflow import keras


class SimulatorDatasetReader(object):

    def __init__(self, folder: str, to_categorical: bool = False):
        self._data = []
        print(f"[Open dataset folder {folder}]")
        for root, dirs, files in walk(folder):
            for file_ in files:
                main_dir, window = path.split(root)
                cur_cache = path.split(main_dir)[-1]
                if cur_cache.find("lru") != 0 and file_ == "dataset.csv.gz":
                    window_num = int(window.split("_")[1])
                    with gzip.GzipFile(path.join(root, file_), "r") as gzFile:
                        cur_df = pd.read_csv(gzFile)
                        del cur_df['siteNameIntHash']
                        del cur_df['fileName']
                        del cur_df['fileNameIntHash']
                        # NOTE:too big to be categorized at the moment
                        del cur_df['taskID']
                        del cur_df['jobID']
                        self._data.insert(
                            window_num,
                            self._get_data_and_labels(cur_df)
                        )
        print("[Dataset loaded...]")

    @staticmethod
    def _get_data_and_labels(df, to_categorical: bool = False) -> ('np.ndarray', 'np.ndarray'):
        # Convert last file hit
        df['cacheLastFileHit'] = df['cacheLastFileHit'].astype('category')
        df['cacheLastFileHit'] = df[
            'cacheLastFileHit'].cat.rename_categories(
            # transform true and false to 0 and 1
            range(len(df['cacheLastFileHit'].cat.categories))
        )
        # TODO: transform category  -> one hot vector into a function
        # Convert site name
        df['siteName'] = df['siteName'].astype('category')
        num_categories = len(df['siteName'].cat.categories)
        df['siteName'] = df['siteName'].cat.rename_categories(
            # transform true and false to 0 and 1
            range(len(df['siteName'].cat.categories))
        )
        site_names_df = pd.DataFrame(
            keras.utils.to_categorical(df['siteName'], 64, dtype='float32'),
            columns=[f"siteName{idx}" for idx in range(64)]
        )
        del df['siteName']
        df = df.join(site_names_df, how='outer')
        # Convert user id
        df['userID'] = df['userID'].astype('category')
        df['userID'] = df[
            'userID'].cat.rename_categories(
            range(len(df['userID'].cat.categories))
        )
        user_id_df = pd.DataFrame(
            keras.utils.to_categorical(df['userID'], 128, dtype='float32'),
            columns=[f"userID{idx}" for idx in range(128)]
        )
        df = df.join(user_id_df, how='outer')
        data = df.loc[
            :,
            df.columns.difference(['class'])
        ]
        for column in data.columns:
            data[column] = pd.to_numeric(data[column], downcast='float')
        data = data.to_numpy()
        # Get labels
        labels = df['class'].astype('category')
        labels = labels.cat.rename_categories(
            range(len(labels.cat.categories))
        )
        if to_categorical:
            labels = keras.utils.to_categorical(
                labels, len(labels.cat.categories), dtype='float32')
        else:
            labels = labels.to_numpy()
        return data, labels

    def get_train_data(self):
        """Produce train and validation sets using cross validation."""
        for validation_idx in range(len(self._data)):
            train_sets = [df for idx, df in enumerate(
                self._data) if idx != validation_idx]
            validation_set = self._data[validation_idx]

            validation_data, validation_labels = validation_set

            for train_set in train_sets:
                data, labels = train_set
                yield data, labels, validation_data, validation_labels


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
