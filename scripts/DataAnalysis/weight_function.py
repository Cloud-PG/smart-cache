import argparse
import datetime
import gzip
import itertools
import json
import os
import pickle
import time
from functools import partial, wraps
from multiprocessing import Pool, current_process
from random import seed, shuffle
from tempfile import NamedTemporaryFile
from time import time
from typing import Dict, List, Set, Tuple

import grpc
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import seaborn as sns
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from tqdm import tqdm

from SmartCache.sim.pySimService import simService_pb2, simService_pb2_grpc

OUTPUT_UPDATE_STEP = 100


def simple_cost_function(**kwargs) -> float:
    return(
        (
            kwargs['size']
            * kwargs['num_files']
        ) / kwargs['frequency']
    ) ** kwargs['exp']


def cost_function_with_time(**kwargs) -> float:
    return (
        (
            (
                kwargs['size']
                * kwargs['num_files'] * (
                    (time() - kwargs['first_time']) *
                    (time() - kwargs['last_time'])
                )
            ) / kwargs['frequency']
        ) ** kwargs['exp']
    )


class WeightedCache(object):

    def __init__(self, max_size: float, cost_function: callable,
                 init_state: dict = {}, cache_options: dict = {}):
        # Cache
        self._cache: Dict[str, float] = {}
        self._cache_weights: Dict[str, float] = {}
        self._cache_groups: Dict[str, str] = {}
        # Groups
        self._group_frequencies: Dict[str, float] = {}
        self._group_file_frequencies: Dict[str, Dict[str, float]] = {}
        self._group_num_files: Dict[str, float] = {}
        self._group_last_time: Dict[str, float] = {}
        self._group_first_time: Dict[str, float] = {}
        self._group_files: Dict[str, Set[str]] = {}
        self._group_dirty: Set[str] = set()
        # Stats
        self._hit = 0
        self._miss = 0
        self._max_size = max_size
        self.size_history: List[float] = []
        self.hit_rate_history: List[float] = []
        self.write_history: List[float] = []
        self.cost_function = cost_function
        self.__cache_options = cache_options

        if init_state:
            self.__setstate__(init_state)

    def __repr__(self):
        options = "_".join(
            [
                elm for elm in
                [
                    'cC' if self.__cache_options.get(
                        'clear_cache', False) else '',
                    'cW' if self.__cache_options.get(
                        'clear_weights', False) else ''
                ]
                if elm
            ]
        )
        return "WeightedCache_{}T_{}_f_{}e{}".format(
            int(self._max_size / 1024**2),
            self.__cache_options.get('fun_name', str(
                self.cost_function).split()[1]),
            int(self.__cache_options.get('exp', 2)),
            "_{}".format(
                options
            )
            if options else ''
        )

    def __len__(self):
        return len(self._cache)

    @property
    def clear_me(self):
        return self.__cache_options.get('clear_cache', False)

    @property
    def clear_my_weights(self):
        return self.__cache_options.get('clear_weights', False)

    @property
    def capacity(self):
        return (self.size / self._max_size) * 100.

    def reset_weights(self):
        self._group_frequencies = {}
        self._group_num_files = {}
        self._group_last_time = {}
        self._group_first_time = {}
        self._group_files = {}
        self._group_dirty = set()

    def clear(self):
        self._cache = {}
        self._cache_weights = {}
        self._cache_groups = {}

    def clear_history(self):
        self.size_history = [self.size_history[-1]]
        self.hit_rate_history = [self.hit_rate_history[-1]]
        self.write_history = [self.write_history[-1]]

    def reset_history(self):
        self.size_history = []
        self.hit_rate_history = []
        self.write_history = []
        self._hit = 0
        self._miss = 0

    @property
    def info(self):
        info = {
            'weights': {},
            'cache': self._cache,
            'df': None,
            'correlation_matrix': None
        }
        dataframe = {
            'size': [],
            'frequency': [],
            'num_files': [],
            'last_time': [],
            'first_time': [],
            'weight': []
        }
        for group, files in self._group_files.items():
            group_frequency = self._group_frequencies[group]
            group_num_files = self._group_num_files[group]
            group_last_time = self._group_last_time[group]
            group_first_time = self._group_first_time[group]
            for cur_str in files:
                file_, size = cur_str.split("->")
                weight = self.cost_function(
                    size=float(size),
                    frequency=group_frequency,
                    num_files=group_num_files,
                    last_time=group_last_time,
                    first_time=group_first_time,
                )
                info['weights'][file_] = weight

                dataframe['size'].append(float(size))
                dataframe['frequency'].append(group_frequency)
                dataframe['num_files'].append(group_num_files)
                dataframe['last_time'].append(group_last_time)
                dataframe['first_time'].append(group_first_time)
                dataframe['weight'].append(weight)

        # info['df'] = pd.DataFrame(dataframe)
        info['correlation_matrix'] = pd.DataFrame(
            dataframe).corr().to_csv(None, header=True, index=True)
        return info

    @property
    def state(self) -> dict:
        return {
            'cache': self._cache,
            'cache_weights': self._cache_weights,
            'cache_groups': self._cache_groups,
            'group_frequencies': self._group_frequencies,
            'group_file_frequencies': self._group_file_frequencies,
            'group_num_files': self._group_num_files,
            'group_last_time': self._group_last_time,
            'group_first_time': self._group_first_time,
            'group_files': self._group_files,
            'group_dirty': self._group_dirty,
            'hit': self._hit,
            'miss': self._miss,
            'max_size': self._max_size,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history,
            'write_history': self.write_history,
            'cost_function': self.cost_function,
            'cache_options': self.__cache_options,
        }

    def __getstate__(self) -> dict:
        return self.state

    def __setstate__(self, state):
        self._cache = state['cache']
        self._cache_weights = state['cache_weights']
        self._cache_groups = state['cache_groups']
        self._group_frequencies = state['group_frequencies']
        self._group_file_frequencies = state['group_file_frequencies']
        self._group_num_files = state['group_num_files']
        self._group_last_time = state['group_last_time']
        self._group_first_time = state['group_first_time']
        self._group_files = state['group_files']
        self._group_dirty = state['group_dirty']
        self._hit = state['hit']
        self._miss = state['miss']
        self._max_size = state['max_size']
        self.size_history = state['size_history']
        self.hit_rate_history = state['hit_rate_history']
        self.write_history = state['write_history']
        self.cost_function = state['cost_function']
        self.__cache_options = state['cache_options']

    @property
    def history(self) -> Tuple[List[float]]:
        return (self.size_history, self.hit_rate_history, self.write_history)

    @property
    def hit_rate(self) -> float:
        return float(self._hit / (self._hit + self._miss)) * 100.

    @property
    def size(self) -> float:
        return sum(self._cache.values())

    @property
    def written_data(self):
        return self.write_history[-1]

    def update_write_history(self, size: float, added: bool):
        try:
            last = self.write_history[-1]
        except IndexError:
            last = 0.0
        if not added:
            self.write_history.append(last)
        else:
            self.write_history.append(last + size)

    def check(self, filename: str) -> bool:
        return filename in self._cache

    @staticmethod
    def get_group(filename: str) -> str:
        _, store_type, campain, process, file_type, _ = [
            part for part in filename.split("/", 6) if part
        ]
        return f"/{store_type}/{campain}/{process}/{file_type}/"

    def get(self, filename: str, size: float) -> bool:
        hit = self.check(filename)
        if hit:
            self._hit += 1
        else:
            self._miss += 1

        added = self.update_policy(filename, size, hit)

        self.size_history.append(self.size)
        self.hit_rate_history.append(self.hit_rate)
        self.update_write_history(size, added)

        return hit

    def update_policy(self, filename: str, size: float, hit: bool) -> bool:
        group = self.get_group(filename)

        if group not in self._group_frequencies:
            self._group_frequencies[group] = 0.
            self._group_file_frequencies[group] = {}
            self._group_num_files[group] = 0.
            self._group_files[group] = set()
            self._group_last_time[group] = time()
            self._group_first_time[group] = time()

        if filename not in self._group_file_frequencies[group]:
            self._group_file_frequencies[group][filename] = 0.

        self._group_frequencies[group] += 1.
        self._group_num_files[group] += 1.
        self._group_files[group] |= set((f"{filename}->{size}", ))
        self._group_file_frequencies[group][filename] += 1.

        self._group_dirty = set((group,))

        if not hit:
            file_weight = self.cost_function(
                size=size,
                frequency=self._group_file_frequencies[group][filename],
                num_files=self._group_num_files[group],
                last_time=self._group_last_time[group],
                first_time=self._group_first_time[group]
            )
            if self.size + size <= self._max_size:
                self._cache[filename] = size
                self._cache_groups[filename] = group
                self._cache_weights[filename] = file_weight
                return True
            else:
                # Update weights
                if len(self._group_dirty) > 0:
                    for cur_filename, file_group in self._cache_groups.items():
                        if file_group in self._group_dirty:
                            self._cache_weights[cur_filename] = self.cost_function(
                                size=self._cache[cur_filename],
                                frequency=self._group_file_frequencies[file_group][cur_filename],
                                num_files=self._group_num_files[file_group],
                                last_time=self._group_last_time[file_group],
                                first_time=self._group_first_time[file_group]
                            )
                    else:
                        self._group_dirty = set()
                # try to insert
                for cur_filename, weight in sorted(
                        self._cache_weights.items(),
                        key=lambda elm: elm[1],
                        reverse=True
                ):
                    if weight > file_weight:
                        del self._cache[cur_filename]
                        del self._cache_groups[cur_filename]
                        del self._cache_weights[cur_filename]
                    else:
                        return False

                    if self.size + size <= self._max_size:
                        self._cache[filename] = size
                        self._cache_groups[filename] = group
                        self._cache_weights[filename] = file_weight
                        return True

        return False


class LRUCache(object):

    def __init__(self, max_size: float, cache_options: dict = {},
                 init_state: dict = {}):
        self._cache: Dict[str, float] = {}
        self._hit = 0
        self._miss = 0
        self._max_size = max_size
        self._queue: List[str] = []
        self.size_history: List[float] = []
        self.hit_rate_history: List[float] = []
        self.write_history: List[float] = []
        self.read_on_hit_history: List[float] = []
        self.__cache_options = cache_options

        if init_state:
            self.__setstate__(init_state)

    def __repr__(self):
        return "LRUCache_{}T{}".format(
            int(self._max_size / 1024**2),
            "_cC" if self.__cache_options.get('clear_cache', False) else ''
        )

    def __len__(self) -> int:
        return len(self._cache)

    @property
    def clear_me(self):
        return self.__cache_options.get('clear_cache', False)

    @property
    def clear_my_weights(self):
        return False

    @property
    def info(self):
        info = {
            'weights': None,
            'cache': self._cache,
            'df': None,
            'correlation_matrix': None
        }
        return info

    @property
    def capacity(self):
        return (self.size / self._max_size) * 100.

    @property
    def written_data(self):
        return self.write_history[-1]

    @property
    def read_on_hit(self):
        return self.read_on_hit_history[-1]

    def clear(self):
        self.__counter = 0
        self._queue = []
        self._cache = {}

    def clear_history(self):
        self.size_history = [self.size_history[-1]]
        self.hit_rate_history = [self.hit_rate_history[-1]]
        self.write_history = [self.write_history[-1]]
        self.read_on_hit_history = [self.read_on_hit_history[-1]]

    def reset_history(self):
        self.size_history = []
        self.hit_rate_history = []
        self.write_history = []
        self.read_on_hit_history = []
        self._hit = 0
        self._miss = 0

    def update_write_history(self, size: float, added: bool):
        try:
            last = self.write_history[-1]
        except IndexError:
            last = 0.0
        if not added:
            self.write_history.append(last)
        else:
            self.write_history.append(last + size)

    def update_read_history(self, size: float, hit: bool):
        try:
            last = self.read_on_hit_history[-1]
        except IndexError:
            last = 0.0
        if not hit:
            self.read_on_hit_history.append(last)
        else:
            self.read_on_hit_history.append(last + size)

    @property
    def state(self) -> dict:
        return {
            'cache': self._cache,
            'hit': self._hit,
            'miss': self._miss,
            'max_size': self._max_size,
            'queue': self._queue,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history,
            'write_history': self.write_history,
            'read_on_hit_history': self.read_on_hit_history,
            'cache_options': self.__cache_options,
        }

    def __getstate__(self) -> dict:
        return self.state

    def __setstate__(self, state):
        self._cache = state['cache']
        self._hit = state['hit']
        self._miss = state['miss']
        self._max_size = state['max_size']
        self._queue = state['queue']
        self.size_history = state['size_history']
        self.hit_rate_history = state['hit_rate_history']
        self.write_history = state['write_history']
        self.read_on_hit_history = state['read_on_hit_history']
        self.__cache_options = state['cache_options']

    @property
    def history(self) -> Tuple[List[float]]:
        return (self.size_history, self.hit_rate_history, self.write_history, self.read_on_hit_history)

    @property
    def hit_rate(self) -> float:
        return float(self._hit / (self._hit + self._miss)) * 100.

    @property
    def size(self) -> float:
        return sum(self._cache.values())

    def check(self, filename: str) -> bool:
        return filename in self._cache

    def get(self, filename: str, size: float) -> bool:
        hit = self.check(filename)
        if hit:
            self._hit += 1
        else:
            self._miss += 1

        added = self.update_policy(filename, size, hit)

        self.size_history.append(self.size)
        self.hit_rate_history.append(self.hit_rate)
        self.update_write_history(size, added)
        self.update_read_history(size, hit)

        return hit

    def update_policy(self, filename: str, size: float, hit: bool) -> bool:
        if hit:
            file_ = self._queue.pop(self._queue.index(filename))
            self._queue.append(file_)
        elif self.size + size > self._max_size:
            while self.size + size > self._max_size:
                file_ = self._queue.pop(0)
                del self._cache[file_]
            self._cache[filename] = size
            self._queue.append(filename)
            return True
        else:
            self._cache[filename] = size
            self._queue.append(filename)
            return True

        return False


def star_decorator(func):
    @wraps(func)
    def star_wrapper(inputs):
        return func(*inputs)
    return star_wrapper


@star_decorator
def simulate(cache, windows: list, region: str = "_all_",
             plot_server: str = None,
             remote: bool = False):
    process_num = int(
        str(current_process()).split("Worker-")[1].split(",")[0]
    )

    if remote:
        _, cache_name, cache_rpc_url = cache.split(':', 2)
        channel = grpc.insecure_channel(cache_rpc_url)
        stubSimService = simService_pb2_grpc.SimServiceStub(channel)
        stubSimService.SimReset(
            google_dot_protobuf_dot_empty__pb2.Empty()
        )
    else:
        cache_name = str(cache)

    if not plot_server:
        tmp_size_history = NamedTemporaryFile()
        tmp_hit_rate_history = NamedTemporaryFile()
        tmp_write_history = NamedTemporaryFile()
        tmp_info = NamedTemporaryFile()
    else:
        buffer = {
            'hit_rate': [(0, 0.0)],
            'weighted_hit_rate': [(0, 0.0)],
            'hit_over_miss': [(0, 0.0)],
            'size': [(0, 0.0)],
            'written_data': [(0, 0.0)],
            'read_on_hit': [(0, 0.0)],
        }

    win_pbar = tqdm(
        desc=f"[{str(cache_name)[:4]+str(cache_name)[-12:]}][Open Data Frames]",
        position=process_num, ascii=True
    )
    record_pbar = tqdm(
        position=process_num,
        desc=f"[{str(cache_name)[:4]+str(cache_name)[-12:]}][Simulation]",
        ascii=True
    )

    last_time = None
    time_delta = datetime.timedelta(days=1)

    for num_window, window in enumerate(windows):
        win_pbar.reset(total=len(window))

        num_file = 1

        request_idx = 0

        for filename in window:

            with gzip.GzipFile(
                filename, mode="rb"
            ) as stats_file:
                df = pd.read_feather(stats_file)
                if region != "_all_":
                    df = df[
                        df.site_name.str.contains(region, case=False)
                    ][['filename', 'size', 'day']].dropna().reset_index()
                else:
                    df = df[['filename', 'size', 'day']].dropna().reset_index()

                record_pbar.reset(total=df.shape[0])

            for row_idx, record in df.iterrows():
                if not last_time:
                    last_time = datetime.datetime.fromtimestamp(record['day'])

                if remote:
                    _ = stubSimService.SimGet(
                        simService_pb2.SimCommonFile(
                            filename=record['filename'],
                            # Convert from Bytes to MegaBytes
                            size=record['size'] / 1024**2
                        )
                    )
                else:
                    cache.get(
                        record['filename'],
                        # Convert from Bytes to MegaBytes
                        record['size'] / 1024**2
                    )

                if plot_server:
                    time_diff = datetime.datetime.fromtimestamp(
                        record['day']
                    ) - last_time

                    if time_diff >= time_delta:
                        if remote:
                            # Get stats and RESET
                            stub_result = stubSimService.SimGetInfoCacheStatus(
                                google_dot_protobuf_dot_empty__pb2.Empty()
                            )
                            cur_hit_rate = stub_result.hitRate
                            cur_weighted_hit_rate = stub_result.weightedHitRate
                            cur_hit_over_miss = stub_result.hitOverMiss
                            cur_capacity = stub_result.capacity
                            cur_written_data = stub_result.writtenData
                            cur_read_on_hit = stub_result.readOnHit
                            cur_size = stub_result.size
                            stubSimService.SimResetHitMissStats(
                                google_dot_protobuf_dot_empty__pb2.Empty()
                            )
                        else:
                            cur_hit_rate = cache.hit_rate
                            cur_weighted_hit_rate = -1
                            cur_hit_over_miss = -1
                            cur_capacity = cache.capacity
                            cur_written_data = cache.written_data
                            cur_read_on_hit = cache.read_on_hit
                            cur_size = cache.size

                        request_idx += 1

                        buffer["hit_rate"].append((request_idx, cur_hit_rate))
                        buffer["weighted_hit_rate"].append(
                            (request_idx, cur_weighted_hit_rate))
                        buffer["hit_over_miss"].append(
                            (request_idx, cur_hit_over_miss))
                        buffer["size"].append((request_idx, cur_size))
                        buffer["written_data"].append(
                            (request_idx, cur_written_data))
                        buffer["read_on_hit"].append(
                            (request_idx, cur_read_on_hit))

                        requests.put(
                            "/".join([
                                plot_server,
                                "cache",
                                "update",
                                cache_name,
                                f"{num_window}"
                            ]),
                            headers={
                                'Content-Type': 'application/octet-stream'},
                            data=gzip.compress(
                                json.dumps(buffer).encode('utf-8')
                            ),
                            timeout=None
                        )

                        buffer = {
                            'hit_rate': [],
                            'weighted_hit_rate': [],
                            'hit_over_miss': [],
                            'size': [],
                            'written_data': [],
                            'read_on_hit': [],
                        }

                        last_time = datetime.datetime.fromtimestamp(
                            record['day'])

                if row_idx % OUTPUT_UPDATE_STEP == 0:
                    if remote:
                        stub_result = stubSimService.SimGetInfoCacheStatus(
                            google_dot_protobuf_dot_empty__pb2.Empty()
                        )
                        cur_hit_rate = stub_result.hitRate
                        cur_weighted_hit_rate = stub_result.weightedHitRate
                        cur_hit_over_miss = stub_result.hitOverMiss
                        cur_capacity = stub_result.capacity
                        cur_written_data = stub_result.writtenData
                        cur_read_on_hit = stub_result.readOnHit
                        cur_size = stub_result.size
                    else:
                        cur_hit_rate = cache.hit_rate
                        cur_weighted_hit_rate = -1
                        cur_hit_over_miss = -1
                        cur_capacity = cache.capacity
                        cur_written_data = cache.written_data
                        cur_read_on_hit = cache.read_on_hit
                        cur_size = cache.size

                    desc_output = ""
                    desc_output += f"[{cache_name[:4]+cache_name[-12:]}][Simulation]"
                    desc_output += f"[Window {num_window+1}/{len(windows)}]"
                    desc_output += f"[File {num_file}/{len(window)}]"
                    desc_output += f"[Hit Rate {cur_hit_rate:06.2f}]"
                    desc_output += f"[W. Hit Rate {cur_weighted_hit_rate:06.2f}]"
                    desc_output += f"[HitOverMiss {cur_hit_over_miss:0.2f}]"
                    try:
                        desc_output += f"[Ratio {cur_read_on_hit/cur_written_data:0.2f}]"
                    except ZeroDivisionError:
                        desc_output += f"[Ratio 0.00]"
                    desc_output += f"[Capacity {cur_capacity:06.2f}]"

                    record_pbar.desc = desc_output
                    record_pbar.update(OUTPUT_UPDATE_STEP)

                # TEST
                # if row_idx == 2000:
                #     break

            else:
                if plot_server and len(buffer['hit_rate']) > 0:
                    requests.put(
                        "/".join([
                            plot_server,
                            "cache",
                            "update",
                            cache_name,
                            f"{num_window}"
                        ]),
                        headers={
                            'Content-Type': 'application/octet-stream'},
                        data=gzip.compress(
                            json.dumps(buffer).encode('utf-8')
                        ),
                        timeout=None
                    )
                    buffer = {
                        'hit_rate': [],
                        'weighted_hit_rate': [],
                        'hit_over_miss': [],
                        'size': [],
                        'written_data': [],
                        'read_on_hit': [],
                    }

            num_file += 1

        if plot_server:
            if remote:
                cur_cache_info = {
                    'cache': dict(
                        (cache_file.filename, cache_file.size)
                        for cache_file in stubSimService.SimGetInfoCacheFiles(
                            google_dot_protobuf_dot_empty__pb2.Empty()
                        )
                    ),
                    'stats': dict(
                        (FileInfo.filename, {
                            'size': FileInfo.size,
                            'totReq': FileInfo.totReq,
                            'nHits': FileInfo.nHits,
                            'nMiss': FileInfo.nMiss
                        })
                        for FileInfo in stubSimService.SimGetInfoFilesStats(
                            google_dot_protobuf_dot_empty__pb2.Empty()
                        )
                    ),
                    'weights':  dict(
                        (FileInfo.filename, FileInfo.weight)
                        for FileInfo in stubSimService.SimGetInfoFilesWeights(
                            google_dot_protobuf_dot_empty__pb2.Empty()
                        )
                    ) if cache_name.lower().find("lru") else {}
                }
            else:
                cur_cache_info = cache.info

            requests.put(
                "/".join([
                    plot_server,
                    "cache",
                    "info",
                    cache_name,
                    f"{num_window}"
                ]),
                headers={
                    'Content-Type': 'application/octet-stream'},
                data=gzip.compress(
                    json.dumps(cur_cache_info).encode('utf-8')
                ),
                timeout=None
            )

        win_pbar.desc = f"[{cache_name[:4]+cache_name[-12:]}][Open Data Frames][Window {num_window+1}/{len(windows)}][File {num_file}/{len(window)}]"
        win_pbar.update(1)

        if not plot_server and not remote:
            cur_size_history, cur_hit_rate_history, cur_write_history, _ = cache.history
            store_results(tmp_size_history.name, [cur_size_history])
            store_results(tmp_hit_rate_history.name, [cur_hit_rate_history])
            store_results(tmp_write_history.name, [cur_write_history])
            store_results(tmp_info.name, [cache.info])

        if not remote:
            cache.clear_history()

            if cache.clear_me:
                cache.clear()

            if cache.clear_my_weights:
                cache.reset_weights()

    record_pbar.close()
    win_pbar.close()

    if remote:
        channel.close()
        del stubSimService
        del channel

    if not plot_server:
        size_history = load_results(tmp_size_history.name)
        hit_rate_history = load_results(tmp_hit_rate_history.name)
        write_history = load_results(tmp_write_history.name)
        cache_info = load_results(tmp_info.name)

        tmp_size_history.close()
        tmp_hit_rate_history.close()
        tmp_write_history.close()
        tmp_info.close()

        return (size_history, hit_rate_history, write_history, cache_info)


def store_results(filename: str, data):
    cur_file_name = f"{filename}.gz"
    if not os.path.exists(cur_file_name):
        with gzip.GzipFile(cur_file_name, "wb") as out_file:
            pickle.dump(data, out_file, pickle.HIGHEST_PROTOCOL)
    else:
        with gzip.GzipFile(cur_file_name, "rb") as input_file:
            cur_file = pickle.load(input_file)
        if isinstance(cur_file, (list, tuple)):
            new_data = cur_file + data
        elif isinstance(cur_file, dict):
            cur_file.update(data)
            new_data = cur_file
        with gzip.GzipFile(cur_file_name, "wb") as out_file:
            pickle.dump(new_data, out_file, pickle.HIGHEST_PROTOCOL)


def load_results(filename: str):
    _, ext = os.path.splitext(filename)
    if ext == ".gz":
        target = filename
    else:
        target = f"{filename}.gz"
    with gzip.GzipFile(target, "rb") as input_file:
        data = pickle.load(input_file)
    return data


def plot_cache_results(caches: dict, out_folder: str, dpi: int = 300):
    grid = plt.GridSpec(96, 32, wspace=2.42, hspace=1.42)
    styles_list = list(itertools.product(
        ('+', '*', '.', 'o', ','), ('-', '--', '-.', ':')
    ))
    seed(42)
    shuffle(styles_list)
    styles = itertools.cycle(styles_list)
    markevery = itertools.cycle([50000, 100000, 150000, 200000])
    cache_styles = {}
    vertical_lines = []

    pbar = tqdm(desc="Plot results", total=len(caches)*3, ascii=True)

    axes = plt.subplot(grid[0:31, 0:])
    for cache_name, (size_history, hit_rate_history, write_history, _) in caches.items():
        cur_marker, cur_linestyle = next(styles)
        cache_styles[cache_name] = (cur_marker, cur_linestyle)
        points = [elm for sublist in size_history for elm in sublist]
        lenght = len(points)
        if not vertical_lines:
            vertical_lines = [
                len(sublist) for sublist in size_history
            ]
            for v_idx in range(1, len(vertical_lines)):
                vertical_lines[v_idx] = vertical_lines[v_idx] + \
                    vertical_lines[v_idx-1]
        axes.plot(
            range(lenght),
            points,
            label=f"{cache_name}",
            marker=cur_marker,
            markevery=next(markevery),
            linestyle=cur_linestyle,
            # alpha=0.9
        )
        axes.set_ylabel("Size (MB)")
        pbar.update(1)
    legend = axes.legend(bbox_to_anchor=(1.0, 1.0))
    axes.grid()
    for vline in vertical_lines:
        axes.axvline(vline, linewidth=0.9, color='k')
    axes.set_xlim(0)
    axes.set_yscale('log')
    axes.set_xticklabels([])

    axes = plt.subplot(grid[32:63, 0:])
    for cache_name, (size_history, hit_rate_history, write_history, _) in caches.items():
        cur_marker, cur_linestyle = cache_styles[cache_name]
        points = [elm for sublist in write_history for elm in sublist]
        lenght = len(points)
        axes.plot(
            range(lenght),
            points,
            label=f"{cache_name}",
            marker=cur_marker,
            markevery=next(markevery),
            linestyle=cur_linestyle,
            # alpha=0.9
        )
        axes.set_ylabel("MB Written")
        pbar.update(1)
    axes.grid()
    for vline in vertical_lines:
        axes.axvline(vline, linewidth=0.9, color='k')
    axes.set_xlim(0)
    axes.set_yscale('log')
    axes.set_xticklabels([])

    axes = plt.subplot(grid[64:, 0:])
    for cache_name, (size_history, hit_rate_history, write_history, _) in caches.items():
        cur_marker, cur_linestyle = cache_styles[cache_name]
        points = [elm for sublist in hit_rate_history for elm in sublist]
        lenght = len(points)
        axes.plot(
            range(lenght),
            points,
            label=f"{cache_name}",
            marker=cur_marker,
            markevery=next(markevery),
            linestyle=cur_linestyle,
            # alpha=0.9
        )
        axes.set_ylabel("Hit rate %")
        pbar.update(1)
    axes.grid()
    for vline in vertical_lines:
        axes.axvline(vline, linewidth=0.9, color='k')
    axes.set_ylim(0, 100)
    axes.set_xlim(0)
    axes.set_xlabel("Requests")

    plt.savefig(
        os.path.join(out_folder, "simulation_result.png"),
        dpi=dpi,
        bbox_extra_artists=(legend, ),
        bbox_inches='tight'
    )

    pbar.close()

    plt.cla()

    no_LRU_info = [(name, info) for name, info in caches.items()
                   if name.find("LRU") == -1]
    grid = plt.GridSpec(16*len(no_LRU_info), 16 *
                        len(vertical_lines), wspace=1.42, hspace=1.42)
    all_texts = []

    for lru_name, (_, _, _, lru_info) in [
        (name, data) for name, data in caches.items()
            if name.find("LRU") != -1]:

        pbar = tqdm(
            desc=f"Plot weight info compared to {lru_name}",
            total=len(no_LRU_info)*len(vertical_lines), ascii=True
        )
        for cache_idx, (cur_name, (_, _, _, cur_info)) in enumerate(no_LRU_info):
            for window_idx, cur_results in enumerate(cur_info):
                axes = plt.subplot(
                    grid[
                        16*cache_idx:16*cache_idx+11,
                        16*window_idx:16*window_idx+10
                    ]
                )
                axes.set_title(
                    f"{cur_name}\nwindow {window_idx} file weights",
                    {'fontsize': 6}
                )
                files = [(filename, weight) for filename, weight in sorted(
                    cur_results['weights'].items(),
                    key=lambda elm: elm[1],
                    reverse=True
                )]
                black_files = [
                    (file_idx, weight)
                    for file_idx, (filename, weight) in enumerate(files)
                ]
                red_files = [
                    (file_idx, weight)
                    for file_idx, (filename, weight) in enumerate(files)
                    if filename in lru_info[window_idx]['cache']
                ]
                blue_files = [
                    (file_idx, weight)
                    for file_idx, (filename, weight) in enumerate(files)
                    if filename in cur_results['cache']
                ]
                if black_files:
                    black_indexes, black_file_weights = zip(*black_files)
                    axes.bar(
                        black_indexes,
                        black_file_weights,
                        color='k',
                        width=1.0,
                        alpha=0.42
                    )
                if red_files:
                    red_indexes, red_file_weights = zip(*red_files)
                    axes.bar(
                        red_indexes,
                        red_file_weights,
                        color='r',
                        width=1.0,
                        alpha=0.9
                    )
                if blue_files:
                    red_indexes, red_file_weights = zip(*blue_files)
                    axes.bar(
                        red_indexes,
                        red_file_weights,
                        color='b',
                        width=1.0,
                        alpha=0.9
                    )

                axes.set_yscale('log')
                axes.set_ylim(0)
                axes.set_xlim(0)
                axes.set_xticklabels([])
                axes.grid()
                pbar.update(1)

        plt.savefig(
            os.path.join(
                out_folder, f"simulation_result_info_compare_{lru_name}.png"),
            dpi=dpi,
            bbox_extra_artists=all_texts,
            bbox_inches='tight'
        )
        pbar.close()

    plt.cla()

    pbar = tqdm(
        desc=f"Plot correlation matrices",
        total=len(no_LRU_info)*len(vertical_lines), ascii=True
    )
    grid = plt.GridSpec(16*len(no_LRU_info), 16 *
                        len(vertical_lines), wspace=1.42, hspace=1.42)
    sns.set(font_scale=0.5)
    for cache_idx, (cur_name, (_, _, _, cur_info)) in enumerate(no_LRU_info):
        for window_idx, cur_results in enumerate(cur_info):
            axes = plt.subplot(
                grid[
                    16*cache_idx:16*cache_idx+11,
                    16*window_idx:16*window_idx+10
                ]
            )
            axes.set_title(
                f"{cur_name}\nwindow {window_idx}",
                {'fontsize': 6}
            )

            corr = cur_results['df'].corr()
            # Exclude duplicate correlations by masking uper right values
            mask = np.zeros_like(corr, dtype=np.bool)
            mask[np.triu_indices_from(mask)] = True

            # Set background color / chart style
            sns.set_style(style='white')

            # Add diverging colormap
            cmap = sns.diverging_palette(10, 250, as_cmap=True)

            # Draw correlation plot
            sns.heatmap(
                corr,
                mask=mask,
                cmap=cmap,
                square=True,
                linewidths=.5,
                ax=axes
            )

            pbar.update(1)

    plt.savefig(
        os.path.join(
            out_folder, f"simulation_result_info_correlations.png"),
        dpi=dpi,
        bbox_inches='tight'
    )
    pbar.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-folder', type=str, default="./results",
                        help='The folder where the json results are stored')
    parser.add_argument('--out-folder', type=str,
                        default="./sim_res",
                        help='The output plot name.')
    parser.add_argument('--plot-server', type=str,
                        default=None,
                        help='The plotting server url. It needs the protocol, e.g. "http://localhost:4321"')
    parser.add_argument('--region', type=str, default="it",
                        help='Region to filter.')
    parser.add_argument('--plot-results', type=str, default="",
                        help='File with results to plot.')
    parser.add_argument('--window-size', '-ws', type=int, default=7,
                        help="Num. of days of a window")
    parser.add_argument('--max-windows', '-mw', type=int, default=-1,
                        help="Num. of windows to simulate")
    parser.add_argument('--exp-values', type=str,
                        # default="2,4",
                        default="2",
                        help="Exponential of cost function. List divided by ','."
                        )
    parser.add_argument('--jobs', '-j', type=int, default=4,
                        help="Num. of concurrent jobs")
    parser.add_argument('--functions', type=str,
                        default="lru,simple,with_time",
                        help="List of functions to test. List divided by ','.")
    parser.add_argument('--cache-sizes', type=str,
                        default="10485760,104857600",  # 10T and 100T
                        help="List of cache sizes in MBytes (10TB default). List divided by ','.")
    parser.add_argument('--clear-cache', '-cC', action='store_true',
                        help="Clear the cache on next window")
    parser.add_argument('--clear-weights', '-cW', action='store_true',
                        help="Clear the weights on next window")

    args, _ = parser.parse_known_args()
    args.exp_values = [float(elm) for elm in args.exp_values.split(",")]
    args.functions = [elm for elm in args.functions.split(",")]
    args.cache_sizes = [float(elm) for elm in args.cache_sizes.split(",")]

    if args.plot_results == "":
        result_files = list(sorted(os.listdir(args.result_folder)))

        files = []
        windows = []
        cache_list = []
        cache_remote_list = []
        clear_cache_list = [False] if not args.clear_cache else [False, True]
        clear_weights_list = [
            False] if not args.clear_weights else [False, True]
        cost_functions = {
            'simple': simple_cost_function,
            'with_time': cost_function_with_time,
        }

        for function in args.functions:
            if function.find('lru') != -1:
                if function.find(":") != -1:
                    cache_list.append(function)
                    cache_remote_list.append(True)
                else:
                    cache_remote_list.append(False)
                    for size in args.cache_sizes:
                        for clear_cache in clear_cache_list:
                            cache_list.append(
                                LRUCache(
                                    size,
                                    cache_options={
                                        'clear_cache': clear_cache,
                                    }
                                )
                            )
                    # TEST
                    #     break
                    # break

        for function in args.functions:
            if function.find('lru') == -1:
                if function.find(":") != -1:
                    cache_list.append(function)
                    cache_remote_list.append(True)

        for fun_name, function in [(fun_name, function)
                                   for fun_name, function in cost_functions.items()
                                   if fun_name in args.functions]:
            for size in args.cache_sizes:
                for exp in args.exp_values:
                    for clear_cache in clear_cache_list:
                        for clear_weights in clear_weights_list:
                            cache_list.append(
                                WeightedCache(
                                    size,
                                    partial(function,
                                            exp=exp),
                                    cache_options={
                                        'fun_name': fun_name,
                                        'exp': exp,
                                        'clear_cache': clear_cache,
                                        'clear_weights': clear_weights
                                    }
                                )
                            )
                            cache_remote_list.append(False)
            # TEST
            #             break
            #         break
            #     break
            # break

        pool = Pool(processes=args.jobs)

        for _, file_ in enumerate(tqdm(
            result_files, desc="Search stat results", ascii=True
        )):
            head, tail0 = os.path.splitext(file_)
            head, tail1 = os.path.splitext(head)

            if tail0 == ".gz" and tail1 == ".feather"\
                    and head.find("results_") == 0:
                cur_file = os.path.join(args.result_folder, file_)
                files.append(cur_file)

            if len(files) == args.window_size:
                windows.append(files)
                files = []

            if len(windows) == args.max_windows:
                files = []
                break

        if len(files) > 0:
            windows.append(files)
            files = []

        for idx, cache_results in enumerate(tqdm(pool.imap(simulate, zip(
            cache_list,
            [windows for _ in range(len(cache_list))],
            [f"_{args.region}_" for _ in range(len(cache_list))],
            [args.plot_server for _ in range(len(cache_list))],
            cache_remote_list
        )), position=0,
                total=len(cache_list),
                desc="Cache simulated",
                ascii=True
        )):
            print(cache_results)
            if not args.plot_server:
                cache_name = f"{str(cache_list[idx])}"
                store_results(f'cache_results_{id(pool)}.pickle', {
                    cache_name: cache_results
                })

        if not args.plot_server:
            os.makedirs(args.out_folder, exist_ok=True)
            plot_cache_results(
                load_results(f'cache_results_{id(pool)}.pickle'),
                out_folder=args.out_folder
            )

        pool.close()
        pool.join()

    else:
        os.makedirs(args.out_folder, exist_ok=True)
        plot_cache_results(
            load_results(args.plot_results),
            out_folder=args.out_folder
        )


if __name__ == "__main__":
    main()
