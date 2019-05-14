import hashlib
import json

import numpy as np

from .utils import FeatureData


class CMSRecordTest0(FeatureData):

    def __init__(self, data: ('CMSDataPopularity', dict) = {}):
        super(CMSRecordTest0, self).__init__()

        self.__tot_wrap_cpu = 0.0
        self.__tot_requests = 1
        self.__num_next_window_hits = 0
        self.__class = "UNKNOWN"

        if isinstance(data, CMSDataPopularity):
            for feature, value in data.features:
                self.add_feature(feature, value)
            self.__tot_wrap_cpu += float(data.WrapCPU)
        else:
            if 'features' in data:
                for feature, value in data['features'].items():
                    self.add_feature(feature, value)
            if 'data' in data:
                self.__tot_wrap_cpu += float(data['data']['WrapCPU'])

    def to_dict(self) -> dict:
        return {
            'tot_wrap_cpu': self.__tot_wrap_cpu,
            'tot_requests': self.__tot_requests,
            'features': self._features,
            'class': self.__class,
            'id': self._id
        }

    def set_class(self, class_: str):
        assert class_ == "good" or class_ == "bad", "Class could be 'good' or 'bad'"
        self.__class = class_
        return self

    def __setstate__(self, state):
        """Make object loaded by pickle."""
        self.__tot_wrap_cpu = state['tot_wrap_cpu']
        self.__tot_requests = state['tot_requests']
        self._features = state['features']
        self.__class = state['class']
        self._id = state['id']
        return self

    def load(self, data: dict) -> 'CMSRecordTest0':
        self.__tot_wrap_cpu = data['tot_wrap_cpu']
        self.__tot_requests = data['tot_requests']
        self._features = data['features']
        self.__class = data['class']
        self._id = data['id']
        return self

    @property
    def record_class(self) -> str:
        return self.__class

    @property
    def tot_wrap_cpu(self) -> float:
        return self.__tot_wrap_cpu

    @property
    def tot_requests(self) -> int:
        return self.__tot_requests

    @property
    def score(self) -> float:
        return float(self.__tot_wrap_cpu / self.__tot_requests)

    def inc_hits(self):
        self.__num_next_window_hits += 1
        return self

    def __add__(self, other: 'CMSRecordTest0'):
        self.__tot_wrap_cpu += other.tot_wrap_cpu
        self.__tot_requests += other.tot_requests
        return self

    def __iadd__(self, other: 'CMSSimpleRecord'):
        self.__tot_wrap_cpu += other.tot_wrap_cpu
        self.__tot_requests += other.tot_requests
        return self


class CMSSimpleRecord(FeatureData):

    def __init__(self, data):
        super(CMSSimpleRecord, self).__init__()
        self.__tasks = set()
        self.__tot_wrap_cpu = 0.0
        self.__record_id = None
        self.__tensor = []
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

    def __setstate__(self, state):
        """Make object loaded by pickle."""
        self._features = state['features']
        self.__tasks = state['tasks']
        self.__tot_wrap_cpu = state['tot_wrap_cpu']
        self.__record_id = state['record_id']
        self.__next_window_counter = state['next_window_counter']
        self.__tensor = state['tensor']
        return self

    def __getstate__(self):
        """Make object serializable by pickle."""
        return {
            'features': self._features,
            'tasks': self.__tasks,
            'tot_wrap_cpu': self.__tot_wrap_cpu,
            'record_id': self.__record_id,
            'next_window_counter': self.__next_window_counter,
            'tensor': self.__tensor,
        }

    def to_dict(self) -> dict:
        return {
            'features': self._features,
            'score': self.score,
            'tensor': self.__tensor
        }

    def gen_tensor(self):
        self.__tensor = [
            float(self._features[feature_name])
            for feature_name in sorted(self._features.keys())
        ]
        return self

    def add_tensor(self, tensor):
        self.__tensor = tensor
        return self

    def __add__(self, other: 'CMSSimpleRecord'):
        tmp = CMSSimpleRecord(self.features)
        for task in self.tasks + other.tasks:
            tmp.add_task(task)
        tmp.add_wrap_cpu(self.tot_wrap_cpu + other.tot_wrap_cpu)
        tmp.add_next_window_counter(*self.next_window_counter)
        tmp.add_next_window_counter(*other.next_window_counter)
        return tmp

    def __iadd__(self, other: 'CMSSimpleRecord'):
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

    def add_task(self, task: str):
        self.__tasks = self.__tasks | set((task, ))
        return self

    def add_wrap_cpu(self, value: float):
        self.__tot_wrap_cpu += value

    def add_next_window_counter(self, true_values: int = 0, false_values: int = 0):
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

    def __init__(self, data: dict,
                 filters=[
                     ('store_type', lambda elm: elm == "data" or elm == "mc")
                 ]
                 ):
        super(CMSDataPopularity, self).__init__()
        self.__data = data
        self.__id = None
        self.__valid = False
        self.__filters = filters
        self.__extract_features()

    def __setstate__(self, state):
        """Make object loaded by pickle."""
        self.__data = state['data']
        self._features = state['features']
        self.__id = state['id']
        self.__valid = state['valid']
        return self

    def to_dict(self) -> dict:
        return {
            'data': self.__data,
            'features': self._features,
            'id': self.__id,
            'valid': self.__valid,
        }

    def __bool__(self):
        return self.__valid

    def __getattr__(self, name):
        if name in self.__data:
            return self.__data[name]
        else:
            raise AttributeError("Attribute '{}' not found...".format(name))

    def __extract_features(self):
        cur_file = self.__data['FileName']
        if cur_file != "unknown":
            logical_file_name = [elm for elm in cur_file.split("/") if elm]
            try:
                store_type, campaign, process, file_type = logical_file_name[1:5]
                self.add_feature('store_type', store_type)
                self.add_feature('campaign', campaign)
                self.add_feature('process', process)
                self.add_feature('file_type', file_type)
                # Check validity
                self.__valid = all(
                    [fun(self.feature[name]) for name, fun in self.__filters]
                )
                if self.__valid:
                    self.__gen_id()
            except ValueError as err:
                print(
                    "Cannot extract features from '{}'".format(cur_file))
                print(err)
                pass


class CMSDataPopularityRaw(FeatureData):

    def __init__(self, data: dict = {},
                 feature_list=['FileName', 'TaskMonitorId',
                               'WrapCPU', 'StartedRunningTimeStamp'],
                 filters=[('Type', lambda elm: elm == "analysis")]
                 ):
        super(CMSDataPopularityRaw, self).__init__()
        self.__valid = False
        if data:
            self.__id = data[feature_list[0]]
            self.__valid = all(
                [fun(data[name]) for name, fun in filters]
            )
        if self.__valid:
            for feature in feature_list:
                self.add_feature(feature, data[feature])

    def __setstate__(self, state) -> 'CMSDataPopularityRaw':
        """Make object loaded by pickle."""
        self._features = state['features']
        self.__id = state['id']
        self.__valid = state['valid']
        return self

    def to_dict(self) -> dict:
        return {
            'features': self._features,
            'id': self.__id,
            'valid': self.__valid
        }

    def loads(self, input_string) -> 'CMSDataPopularityRaw':
        data = json.loads(input_string)
        self._features = data['features']
        self.__id = data['id']
        self.__valid = data['valid']
        return self

    @property
    def valid(self) -> bool:
        return self.__valid

    def __bool__(self) -> bool:
        return self.valid

    def __getattr__(self, name):
        if name in self._features:
            return self._features[name]
        else:
            raise AttributeError("Attribute '{}' not found...".format(name))
