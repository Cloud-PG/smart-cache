import hashlib
import json

import numpy as np


class FeatureData(object):

    def __init__(self):
        self._features = {}

    @property
    def features(self):
        for feature in sorted(self._features):
            yield feature, self._features[feature]

    def features2array(self):
        tmp = []
        for feature in sorted(self._features):
            tmp.append(self._features[feature])
        return np.array(tmp)

    def __repr__(self):
        return json.dumps(list(self.features))

    def add_feature(self, name, value):
        self._features[name] = value


class CMSSimpleRecord(FeatureData):

    def __init__(self, features):
        super(CMSSimpleRecord, self).__init__()
        for feature, value in features:
            self.add_feature(feature, value)
        self.__tasks = []
        self.__tot_wrap_cpu = 0.0

    def to_dict(self):
        return {
            'features': self._features,
            'score': self.score
        }

    def __add__(self, other):
        tmp = CMSSimpleRecord(self.features)
        for task in self.tasks + other.tasks:
            tmp.add_task(task)
        tmp.update_tot_wrap_cpu(self.tot_wrap_cpu + other.tot_wrap_cpu)
        return tmp

    def __iadd__(self, other):
        for task in other.tasks:
            self.add_task(task)
        self.update_tot_wrap_cpu(other.tot_wrap_cpu)
        return self

    @property
    def tasks(self):
        return self.__tasks

    @property
    def tot_wrap_cpu(self):
        return self.__tot_wrap_cpu

    @property
    def score(self):
        return self.__tot_wrap_cpu / len(self.__tasks)

    def add_task(self, task):
        if task not in self.__tasks:
            self.__tasks.append(task)
        return self

    def update_tot_wrap_cpu(self, value: float):
        self.__tot_wrap_cpu += value

    @property
    def record_id(self):
        if self.__record_id is None:
            blake2s = hashlib.blake2s()
            blake2s.update(str(self).encode("utf-8"))
            self.__record_id = blake2s.hexdigest()
        return self.__record_id


class CMSDataPopularity(FeatureData):

    def __init__(self, data):
        super(CMSDataPopularity, self).__init__()
        self.__data = data
        self.__record_id = None
        self.__extract_features()

    def __getattr__(self, name):
        if name in self.__data:
            return self.__data[name]
        else:
            raise AttributeError("Attribute '{}' not foud...".format(name))

    def __extract_features(self):
        cur_file = self.__data['FileName']
        if cur_file != "unknown":
            logical_file_name = [elm for elm in cur_file.split("/") if elm]
            try:
                store_type, campain, process, file_type = logical_file_name[1:5]
                self.add_feature('store_type', store_type)
                self.add_feature('campain', campain)
                self.add_feature('process', process)
                self.add_feature('file_type', file_type)
            except ValueError:
                print(
                    "Cannot extract features from '{}'".format(cur_file))
                pass

    @property
    def record_id(self):
        if self.__record_id is None:
            blake2s = hashlib.blake2s()
            blake2s.update(str(self).encode("utf-8"))
            self.__record_id = blake2s.hexdigest()
        return self.__record_id
