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

    def __init__(self, data):
        super(CMSSimpleRecord, self).__init__()
        self.__tasks = set()
        self.__tot_wrap_cpu = 0.0
        self.__record_id = None
        self.__next_window_counter = [1, 1]  # [True, False] counters

        if isinstance(data, CMSDataPopularity):
            for feature, value in data.features:
                self.add_feature(feature, value)
            self.add_task(data.TaskMonitorId)
            self.add_wrap_cpu(float(data.WrapCPU))
            if data.next_window:
                self.__next_window_counter[0] += 1
            else:
                self.__next_window_counter[1] += 1
            assert self.record_id == data.record_id, "record id doesn't match..."
        else:
            for feature, value in data:
                self.add_feature(feature, value)

    def to_dict(self):
        return {
            'features': self._features,
            'score': self.score
        }

    def __add__(self, other):
        tmp = CMSSimpleRecord(self.features)
        for task in self.tasks + other.tasks:
            tmp.add_task(task)
        tmp.add_wrap_cpu(self.tot_wrap_cpu + other.tot_wrap_cpu)
        tmp.add_next_window_counter(*self.next_window_counter)
        tmp.add_next_window_counter(*other.next_window_counter)
        return tmp

    def __iadd__(self, other):
        for task in other.tasks:
            self.add_task(task)
        self.add_wrap_cpu(other.tot_wrap_cpu)
        self.add_next_window_counter(*other.next_window_counter)
        return self

    def __repr__(self):
        return json.dumps(self.to_dict())

    @property
    def tasks(self):
        return self.__tasks

    @property
    def tot_wrap_cpu(self):
        return self.__tot_wrap_cpu
    
    @property
    def next_window_counter(self):
        return self.__next_window_counter

    @property
    def score(self):
        try:
            next_window_ratio = float(
                self.__next_window_counter[0] / self.__next_window_counter[1])
        except ZeroDivisionError:
            next_window_ratio = 0.0
        return float(self.__tot_wrap_cpu / len(self.__tasks)) * next_window_ratio

    def add_task(self, task):
        self.__tasks = self.__tasks | set((task, ))
        return self

    def add_wrap_cpu(self, value: float):
        self.__tot_wrap_cpu += value

    def add_next_window_counter(self, true_values: int=0, false_values: int=0):
        self.__next_window_counter[0] += true_values
        self.__next_window_counter[1] += false_values

    @property
    def record_id(self):
        if self.__record_id is None:
            blake2s = hashlib.blake2s()
            blake2s.update(json.dumps(list(self.features)).encode("utf-8"))
            self.__record_id = blake2s.hexdigest()
        return self.__record_id


class CMSDataPopularity(FeatureData):

    def __init__(self, data, indexes=set()):
        super(CMSDataPopularity, self).__init__()
        self.__data = data
        self.__record_id = None
        self.__valid = False
        self.__next_window = False
        if self.__data.get('FileName') in indexes:
            self.__next_window = True
        self.__extract_features()

    def __bool__(self):
        return self.__valid

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
                self.__valid = True
            except ValueError:
                print(
                    "Cannot extract features from '{}'".format(cur_file))
                pass

    @property
    def next_window(self):
        return self.__next_window

    @property
    def record_id(self):
        if self.__record_id is None:
            blake2s = hashlib.blake2s()
            blake2s.update(str(self).encode("utf-8"))
            self.__record_id = blake2s.hexdigest()
        return self.__record_id


class CMSDataPopularityRaw(FeatureData):

    def __init__(self, data, feature_list=['FileName', 'TaskMonitorId', 'WrapCPU']):
        super(CMSDataPopularityRaw, self).__init__()
        self.__id = data[feature_list[0]]
        for key, value in data.items():
            if key in feature_list:
                self.add_feature(key, value)

    def __getattr__(self, name):
        if name in self._features:
            return self._features[name]
        else:
            raise AttributeError("Attribute '{}' not foud...".format(name))

    @property
    def record_id(self):
        return self.__id

    @property
    def data(self):
        return self._features

    def __repr__(self):
        return json.dumps(self._features)
