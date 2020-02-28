import csv
import gzip
import json
import os
from collections import OrderedDict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import gym
import math

bandwidthLimit = (1000000. / 8.) * 60. * 60. * 24
time_span = 30000


class FileStats(object):

    __slots__ = ["_size", "_hit", "_miss", "_last_request", "_recency", "_datatype"]

    def __init__(self, size: float):
        self._size: float = size
        self._hit: int = 0
        self._miss: int = 0
        self._last_request: int = 0
        self._recency: int = 0
        self._datatype: int = 0

    def update_retrieve(self, size: float, hit: bool = False):
        self._size = size
        self._recency = 0

    def update(self, size: float, datatype: int, hit: bool = False, ):
        self._size = size
        if hit:
            self._hit += 1
        else:
            self._miss += 1

        self._recency = 0
        self._datatype = datatype

    @property
    def tot_requests(self):
        return self._hit + self._miss

    @property
    def hit(self):
        return self._hit

    @property
    def miss(self):
        return self._miss

    @property
    def size(self):
        return self._size


class Stats(object):

    def __init__(self):
        self._files = {}

    def get_or_set(self, filename: str, size: float, request: int) -> 'FileStats':
        stats = None
        if filename not in self._files:
            stats = FileStats(size)
            stats._last_request = request
            self._files[filename] = stats
        else:
            stats = self._files[filename]
        return stats


class cache(object):

    def __init__(self, size: float = 104857600, h_watermark: float = 95., l_watermark: float = 75.):
        self._size: float = 0.0
        self._max_size = size

        self._filesLRU = OrderedDict()
        self._filesLRUkeys = []

        self._stats = Stats()

        # Stat attributes
        self._hit: int = 0
        self._miss: int = 0
        self._written_data: float = 0.0
        self._deleted_data: float = 0.0
        self._read_data: float = 0.0

        self._CPUtime_hit: float = 0.0
        self._CPUtime_miss: float = 0.0
        self._WALLtime_hit: float = 0.0
        self._WALLtime_miss: float = 0.0

        self._dailyReadOnHit: float = 0.0
        self._dailyReadOnMiss: float = 0.0

        self._h_watermark: float = h_watermark
        self._l_watermark: float = l_watermark

    @property
    def capacity(self) -> float:
        return (self._size / self._max_size) * 100.

    def hit_rate(self) -> float:
        if self._hit:
            return self._hit / (self._hit + self._miss)
        return 0.0

    def check(self, filename: str) -> bool:
        return filename in self._filesLRU

    def before_request(self, filename, hit: bool, size, datatype, request: int) -> 'FileStats':
        stats = self._stats.get_or_set(filename, size, request)
        stats.update(size, hit, datatype)
        return stats

    def before_request_retrieve(self, filename, hit: bool, size, request: int) -> 'FileStats':
        stats = self._stats.get_or_set(filename, size, request)
        stats.update_retrieve(size, hit)
        return stats

    def update_policy(self, filename, file_stats, hit: bool) -> bool:
        if not hit:
            self._filesLRU[filename] = file_stats
            return True
        else:
            self._filesLRU.move_to_end(filename)
            return False

    def after_request(self, fileStats, hit: bool, added: bool):
        if hit:
            self._hit += 1
            self._dailyReadOnHit += fileStats.size
        else:
            self._miss += 1
            self._dailyReadOnMiss += fileStats.size

        if added:
            self._size += fileStats.size
            self._written_data += fileStats.size

        self._read_data += fileStats.size
    
    def update_recency(self):
        for _, value in self._stats._files.items():
            value._recency += 1
        #for _, value in self._filesLRU.items():
        #    value._recency += 1

    def _get_mean_recency(self, curRequest, curDay):
        if curRequest == 0 and curDay == 0:
            return 0.
        else:
            list_=[]
            for filename,_ in self._filesLRU.items():
                list_.append(self._stats._files[filename]._recency)
                #list_.append(v._recency)
            return np.array(list_).mean()
    
    def _get_mean_frequency(self, curRequest, curDay):
        if curRequest == 0 and curDay == 0:
            return 0.
        else:
            list_=[]
            for filename,_ in self._filesLRU.items():
                list_.append(self._stats._files[filename].tot_requests)
                #list_.append(v.tot_requests)
            return np.array(list_).mean()
    
    def _get_mean_size(self, curRequest, curDay):
        if curRequest == 0 and curDay == 0:
            return 0.
        else:
            list_=[]
            for filename,_ in self._filesLRU.items():
                list_.append(self._stats._files[filename]._size)
            return np.array(list_).mean()


###############################################################################
###############################################################################
###############################################################################

def from_list_to_one_hot(list_):
    with open('features.json') as f:
        features = json.load(f)

    features_list = ["size", "numReq",
                     "deltaNumLastRequest", "cacheUsage", "dataType"]
    one_hot_tot = np.zeros(0)
    for j in range(len(features_list)-1):

        keys = features[features_list[j]]['keys']
        n = len(keys)
        one_hot = np.zeros(n+1)
        not_max = False

        for i in range(0, n):
            if list_[i] <= float(keys[i]):
                one_hot[i] = 1.0
                not_max = True
                break
        if not_max == False:
            one_hot[n] = 1.0
        one_hot_tot = np.concatenate((one_hot_tot, one_hot))

    if list_[len(features)-1] == 'data':
        one_hot_tot = np.concatenate((one_hot_tot, np.ones(1)))
        one_hot_tot = np.concatenate((one_hot_tot, np.zeros(1)))
    else:
        one_hot_tot = np.concatenate((one_hot_tot, np.zeros(1)))
        one_hot_tot = np.concatenate((one_hot_tot, np.ones(1)))

    return one_hot_tot

class CacheEnv(gym.Env):

    def write_stats(self):
        if self.curDay == self._idx_start:
            with open('results/results_ok_stats_{}/dQlONLYeviction_100T_it_shuffle_{}_startmonth{}_endmonth{}.csv'.format(str(time_span), 'onehot'+ str(self._one_hot),self._startMonth,self._endMonth), 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(
                    ['date',
                     'size',
                     'hit rate',
                     'hit over miss',
                     'weighted hit rate',
                     'written data',
                     'read data',
                     'read on hit data',
                     'read on miss data',
                     'deleted data',
                     'CPU efficiency',
                     'CPU hit efficiency',
                     'CPU miss efficiency',
                     'cost'])

        with open('results/results_ok_stats_{}/dQlONLYeviction_100T_it_shuffle_{}_startmonth{}_endmonth{}.csv'.format(str(time_span),'onehot'+ str(self._one_hot),self._startMonth,self._endMonth), 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
                [str(datetime.fromtimestamp(self.df.loc[0, 'reqDay']) + timedelta(days=1) ) + ' +0200 UTC',
                 self._cache._size,
                 self._cache.hit_rate() * 100.0,
                 self._cache._hit/self._cache._miss * 100.0,
                 0,
                 self._cache._written_data,
                 self._cache._read_data,
                 self._cache._dailyReadOnHit,
                 self._cache._dailyReadOnMiss,
                 self._cache._deleted_data,
                 (self._cache._CPUtime_hit + self._cache._CPUtime_miss) /
                    (self._cache._WALLtime_hit +
                     self._cache._WALLtime_miss * 1.15) * 100.0,
                    (self._cache._CPUtime_hit)/(self._cache._WALLtime_hit) * 100.0,
                    (self._cache._CPUtime_miss) /
                 (self._cache._WALLtime_miss * 1.15) * 100.0,
                    self._cache._written_data + self._cache._read_data + self._cache._deleted_data])

        return

    def reset_stats(self):
        self._cache._hit: int = 0
        self._cache._miss: int = 0
        self._cache._written_data: float = 0.0
        self._cache._deleted_data: float = 0.0
        self._cache._read_data: float = 0.0

        self._cache._dailyReadOnHit: float = 0.0
        self._cache._dailyReadOnMiss: float = 0.0

        self._cache._CPUtime_hit: float = 0.0
        self._cache._WALLtime_hit: float = 0.0
        self._cache._CPUtime_miss: float = 0.0
        self._cache._WALLtime_miss: float = 0.0

        return

    def get_dataframe(self, i):
        directory = "/home/ubuntu/source2018_numeric_it_shuffle_42"
        file_ = sorted(os.listdir(directory))[i]
        with gzip.open(directory + '/' + str(file_)) as f:
            df_ = pd.read_csv(f)
            df_['Size'] = df_['Size']/1.049e+6
            self.df = df_
            self.df_length = len(self.df)
        print(file_)
    
    def update_time_span_filenames_list(self):
        l=[]
        self.tmp_df = self.df
        self.tmp_req_index = self.curRequest
        self.tmp_day_index = self.curDay
        self.tmp_df_length = self.df_length
        
        for _ in range(time_span):

            self.tmp_req_index += 1
        
            if (self.tmp_req_index + 1) == self.tmp_df_length:
                
                self.tmp_day_index += 1
                directory = "/home/ubuntu/source2018_numeric_it_shuffle_42"
                file_ = sorted(os.listdir(directory))[self.tmp_day_index]
                with gzip.open(directory + '/' + str(file_)) as f:
                    df_ = pd.read_csv(f)
                    df_['Size'] = df_['Size']/1.049e+6
                    self.tmp_df = df_
                    self.tmp_df_length = len(self.tmp_df)
                self.tmp_req_index = 0

            filename = self.tmp_df.loc[self.tmp_req_index, 'Filename']
            l.append(filename)

        self._time_span_filenames_list = l

    def get_reward(self,action,filename,size):
        
        if action == 0:
            if filename in self._time_span_filenames_list:
                return self._time_span_filenames_list.count(filename) * size
            else:
                return -size 
        if action == 1:
            if filename in self._time_span_filenames_list:
                return self._time_span_filenames_list.count(filename) * (-size)
            else:
                return +size
    
    def get_next_request_stats(self):
        
        self.curRequest += 1

        if (self.curRequest + 1) == self.df_length:
            self.write_stats()
            self.reset_stats()
            self.curDay += 1
            self.curRequest = 0
            self.get_dataframe(self.curDay)

        hit = self._cache.check(self.df.loc[self.curRequest, 'Filename'])
        filename = self.df.loc[self.curRequest, 'Filename']
        size = self.df.loc[self.curRequest, 'Size']
        datatype = self.df.loc[self.curRequest, 'DataType']
        filestats = self._cache.before_request(filename, hit, size, datatype, self.curRequest)
        cputime = self.df.loc[self.curRequest, 'CPUTime']
        walltime = self.df.loc[self.curRequest, 'WrapWC']

        return hit, filename, size, filestats, cputime, walltime
    
    def get_next_file_in_cache_values(self):
        self._filesLRU_index += 1
        filename = self._filesLRUkeys[self._filesLRU_index]
        #filestats = self._cache._filesLRU[filename]
        filestats = self._cache._stats._files[filename]
        l = []
        l.append(filestats._size)
        l.append(filestats.tot_requests)
        l.append(self.curRequest - filestats._last_request)
        datatype = filestats._datatype
        if datatype == 0:
            l.append(0.)
        else:
            l.append(1.)
        l.append(self._cache._get_mean_recency(self.curRequest,self.curDay))
        l.append(self._cache._get_mean_frequency(self.curRequest,self.curDay))
        l.append(self._cache._get_mean_size(self.curRequest,self.curDay))

        return np.asarray(l)
    
    def get_filename_and_size_of_current_cache_file(self):
        filename = self._filesLRUkeys[self._filesLRU_index]
        filestats = self._cache._stats._files[filename]
        return filename, filestats._size
    
    def add_request(self):
        hit, filename, size, filestats, cputime, walltime = self.get_next_request_stats()
        if hit == False:
            self._cache._WALLtime_miss += walltime
            self._cache._CPUtime_miss += cputime
        if hit == True:
            self._cache._WALLtime_hit += walltime
            self._cache._CPUtime_hit += cputime
        self._cache.update_recency()
        added = self._cache.update_policy(filename, filestats, hit)
        self._cache.after_request(filestats, hit, added)
        print('Request: ' + str(self.curRequest) + ' / ' + str(self.df_length) + '  -  Occupancy: ' + str(round(self._cache.capacity,2)) + '%  -  ' + 'Hit rate: ' + str(round(self._cache._hit/(self._cache._hit + self._cache._miss)*100,2)) +'%' + ' ' + str(self._cache._get_mean_size(self.curRequest,self.curDay) * len(self._cache._filesLRU) / self._cache._max_size), end='\r')

    def __init__(self, one_hot: bool = True, start_month: int = 1, end_month: int = 2 ):

        self._one_hot = one_hot
        self.curRequest = 0
        self._startMonth = start_month
        self._endMonth = end_month

        start = datetime(2018, 1, 1)
        delta = timedelta(days=1)
        
        idx_start=0
        cur = start
        while cur.month != start_month:
            idx_start += 1
            cur = start + delta*idx_start

        idx_end=idx_start
        if end_month != 12: 
            while cur.month != end_month + 1:
                idx_end += 1
                print(cur)
                cur = start + delta*idx_end
        else:
            while cur.month != 1:
                idx_end += 1
                print(cur)
                cur = start + delta*idx_end

        
        self._idx_start = idx_start
        self._idx_end = idx_end

        self.curDay = idx_start 
        self._totalDays = idx_end - idx_start

        # define action and observations spaces
        self.action_space = gym.spaces.Discrete(2)
        if self._one_hot == True:
            self.observation_space = gym.spaces.Box(
                low=0, high=1, shape=(16,), dtype=np.float16)
        else:
            self.observation_space = gym.spaces.Box(
                low=0, high=1, shape=(7,), dtype=np.float16)            

        print('Environment initialized')

    def step(self, action):
        print('Freeing memory ' + str(self._filesLRU_index) + '/' + str(len(self._filesLRUkeys)) + '  -  Occupancy: ' + str(round(self._cache.capacity,2)) + '%  - action: ' + str(action) + ' ' + str(self._cache._get_mean_size(self.curRequest,self.curDay) * len(self._cache._filesLRU) / self._cache._max_size))
        curFilename, curSize = self.get_filename_and_size_of_current_cache_file()

        if action == 1:
            del self._cache._filesLRU[curFilename]
            self._cache._size -= curSize
            self._cache._deleted_data += curSize
        
        with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(time_span),self.eviction_counter), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([action])

        reward = self.get_reward(action,curFilename,curSize)

        #with open('results_okstats/reward.csv', 'a') as file:
        #    writer = csv.writer(file)
        #    writer.writerow([reward])

        if self._filesLRU_index + 1 == len(self._filesLRUkeys):
            self.eviction_counter += 1
            with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(time_span), self.eviction_counter), 'w') as file:
                writer = csv.writer(file)
                writer.writerow(['eviction_choice'])
            with open('results/results_ok_stats_{}/occupancy.csv'.format(str(time_span)), 'a') as file:
                writer = csv.writer(file)
                writer.writerow([self._cache.capacity])
            while self._cache.capacity < self._cache._h_watermark:
                self.add_request()
            self._filesLRUkeys = list(self._cache._filesLRU.keys())
            self._filesLRU_index = -1
            next_file_values = self.get_next_file_in_cache_values()
            self.update_time_span_filenames_list()

        else:
            next_file_values = self.get_next_file_in_cache_values()

        return next_file_values, reward, False, {}

    def reset(self):

        #with open('results_okstats/reward.csv', 'w') as file:
        #    writer = csv.writer(file)
        #    writer.writerow(['reward'])

        with open('results/results_ok_stats_{}/occupancy.csv'.format(str(time_span)), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['occupancy'])

        # create cache
        self._cache = cache()
        self.size_tot=0
        self.curRequest = 0
        self.get_dataframe(self.curDay)
        self._filesLRU_index = -1
        self.eviction_counter = 0

        self.eviction_counter += 1
        with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(time_span), self.eviction_counter), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['eviction_choice'])

        while self._cache.capacity < self._cache._h_watermark:
            self.add_request()
            #counter += 1
            #print('Adding files: ' + str(counter) + ' occupancy: ' + str(self._cache.capacity) +'%', end='\r')
        
        self._filesLRUkeys = list(self._cache._filesLRU.keys())

        first_file_in_cache = self.get_next_file_in_cache_values()

        self.update_time_span_filenames_list()

        return first_file_in_cache
        
        

