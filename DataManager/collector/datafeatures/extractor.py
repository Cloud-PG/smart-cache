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

    def __iadd__(self, other: 'CMSRecordTest0'):
        self.__tot_wrap_cpu += other.tot_wrap_cpu
        self.__tot_requests += other.tot_requests
        return self


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

    def __init__(
        self,
        data: dict = {},
        feature_list=[
            'FileName',
            'Application',
            'ApplicationVersion',
            'BlockId',
            'BlockName',
            'ExeCPU',
            'FileType',
            'FinishedTimeStamp',
            'GenericType',
            'GridName',
            'InputCollection',
            'InputSE',
            'IsParentFile',
            'JobExecExitCode',
            'JobExecExitTimeStamp',
            'JobId',
            'JobMonitorId',
            'JobType',
            'LumiRanges',
            'NCores',
            'NEventsPerJob',
            'NEvProc',
            'NEvReq',
            'NewGenericType',
            'NewType',
            'NTaskSteps',
            'ProtocolUsed',
            'SchedulerJobIdV2',
            'SchedulerName',
            'SiteName',
            'StartedRunningTimeStamp',
            'StrippedBlocks',
            'StrippedFiles',
            'SubmissionTool',
            'SuccessFlag',
            'TargetCE',
            'TaskId',
            'TaskJobId',
            'TaskMonitorId',
            'Type',
            'UserId',
            'ValidityFlag',
            'WNHostName',
            'WrapCPU',
            'WrapWC',
        ],
        filters=[
            ('Type', lambda elm: elm == "analysis")
        ]
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
