import csv
import gzip
import json
import os
from collections import OrderedDict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import math

bandwidthLimit = (1000000. / 8.) * 60. * 60. * 24
time_span = 30000
purge_delta = 210000

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

    def update_policy(self, filename, file_stats, hit: bool, action: int) -> bool:
        if not hit and action == 0:
            self._filesLRU[filename] = file_stats
            return True
        elif hit:
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

class env:
    def __init__(self, start_month: int = 1, end_month: int = 2, directory: str = "/home/ubuntu/source2018_numeric_it_shuffle_42"):
        
        self.time_span = time_span
        self.curRequest = 0
        self._startMonth = start_month
        self._endMonth = end_month
        self._directory = directory



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

        # create cache
        self.adding_or_evicting = 0
        self._cache = cache()
        self.size_tot = 0
        self.curRequest = -1
        self.get_dataframe(self.curDay)
        self._filesLRU_index = -1
        self.eviction_counter = 0

        self._request_window_filenames = {}
        self._request_window_counters = {}
        self._request_window_cur_values = {}
        self._request_window_rewards = {}
        self._request_window_actions = {}
        self._request_window_next_values = {}

        self._eviction_window_filenames = {}
        self._eviction_window_counters = {}
        self._eviction_window_cur_values = {}
        self._eviction_window_rewards = {}
        self._eviction_window_actions = {}
        self._eviction_window_next_values = {}


        #GET FIRST REQUEST VALUES
        nxt = self.get_next_request_stats()
        #filename = nxt[1]
        filestats = nxt[3]
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
        
        self.curValues = np.asarray(l)

        #CREATE THE WINDOW 
        l = []
        self.tmp_df = self.df
        self.tmp_req_index = self.curRequest
        self.tmp_day_index = self.curDay
        self.tmp_df_length = self.df_length
        self._time_span_filenames_list = []
        
        self.eviction_counter = 0
        self.addiction_counter = 0

        with open('results/results_ok_stats_{}/addition_choices_{}.csv'.format(str(time_span), self.addiction_counter), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['addition choice'])
    
        with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(time_span), self.eviction_counter), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['eviction choice'])

        print('Environment initialized')

    def write_stats(self):
        if self.curDay == self._idx_start:
            with open('results/results_ok_stats_{}/dQlONLYeviction_100T_it_shuffle_startmonth{}_endmonth{}.csv'.format(str(time_span),self._startMonth,self._endMonth), 'w', newline='') as file:
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

        with open('results/results_ok_stats_{}/dQlONLYeviction_100T_it_shuffle_startmonth{}_endmonth{}.csv'.format(str(time_span),self._startMonth,self._endMonth), 'a', newline='') as file:
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
        file_ = sorted(os.listdir(self._directory))[i]
        with gzip.open(self._directory + '/' + str(file_)) as f:
            df_ = pd.read_csv(f)
            df_['Size'] = df_['Size']/1.049e+6
            self.df = df_
            self.df_length = len(self.df)
        print(file_)

    def last_filename_action(self, filename, add_or_evict):
        if add_or_evict == 0:
            N = len(self._request_window_filenames)
            for i in range(N):
                if self._request_window_filenames[N-1-i] == filename:
                    return self._request_window_actions[N-1-i]
        if add_or_evict == 1:
            N = len(self._eviction_window_filenames)
            for i in range(N):
                if self._eviction_window_filenames[N-1-i] == filename:
                    return self._eviction_window_actions[N-1-i]

    def update_windows_getting_eventual_rewards(self, curFilename, curValues, nextValues, action, add_memory_vector, evict_memory_vector, end_of_adding):
        if adding_or_evicting == 0:
            for key in self._request_window_counters:                                                #increment counters
                self._request_window_counters[key] +=  1
            for key in self._eviction_window_counters:                                                #increment counters
                self._eviction_window_counters[key] +=  1

            if curFilename in self._request_window_cur_values.keys():            #if is in queue
                if self._request_window_counters[curFilename] == time_span:                          #is invalidated
                    if self._request_window_actions[curFilename] == 0:
                        self._request_window_rewards[curFilename] = - 1
                    else:
                        self._request_window_rewards[curFilename] = + 1
                else:                                                                           #is not invalidated yet
                    if self._request_window_actions[curFilename] == 0:
                        self._request_window_rewards[curFilename] = + 1
                    else:
                        self._request_window_rewards[curFilename] = - 1
                add_memory_vector.append(np.array([self._request_window_cur_values[curFilename], self._request_window_actions[curFilename], self._request_window_rewards[curFilename],  self._request_window_cur_values[curFilename])
                del self._request_window_cur_values[curFilename], self._request_window_actions[curFilename], self._request_window_rewards[curFilename],  self._request_window_cur_values[curFilename]

            else:                                                           #if isnt in queue
                _request_window_counters[curFilename] = 1
                _request_window_actions[curFilename] = action
                _request_window_rewards[curFilename] = 0
                _request_window_cur_values[curFilename] = curValues
                _request_window_next_values[curFilename] = nextValues 
            
        else:
            if curFilename in self._eviction_window_cur_values.keys():            #if is in queue
                if self._eviction_window_counters[curFilename] == time_span:                          #is invalidated
                    if self._eviction_window_actions[curFilename] == 0:
                        self._eviction_window_rewards[curFilename] = - 1
                    else:
                        self._eviction_window_rewards[curFilename] = + 1
                else:                                                                           #is not invalidated yet
                    if self._eviction_window_actions[curFilename] == 1:
                        self._eviction_window_rewards[curFilename] = + 1
                    else:
                        self._eviction_window_rewards[curFilename] = - 1
                evict_memory_vector.append(np.array([self._eviction_window_cur_values[curFilename], self._eviction_window_actions[curFilename], self._eviction_window_rewards[curFilename],  self._eviction_window_cur_values[curFilename])
                del self._eviction_window_cur_values[curFilename], self._eviction_window_actions[curFilename], self._eviction_window_rewards[curFilename],  self._eviction_window_cur_values[curFilename]

            else:                                                           #if isnt in queue
                _eviction_window_counters[curFilename] = 1
                _eviction_window_actions[curFilename] = action
                _eviction_window_rewards[curFilename] = 0
                _eviction_window_cur_values[curFilename] = curValues
                _eviction_window_next_values[curFilename] = nextValues 

    def clear_window(self):
        for curFilename in self._request_window_counters.keys():
            if self._request_window_actions[curFilename] == 0:
                self._request_window_rewards[curFilename] = - 1
            else:
                self._request_window_rewards[curFilename] = + 1
            add_memory_vector.append(np.array([self._request_window_cur_values[curFilename], self._request_window_actions[curFilename], self._request_window_rewards[curFilename],  self._request_window_cur_values[curFilename])
            del self._request_window_cur_values[curFilename], self._request_window_actions[curFilename], self._request_window_rewards[curFilename],  self._request_window_cur_values[curFilename]

        for curFilename in self._eviction_window_counters.keys():                                             
            if self._eviction_window_actions[curFilename] == 0:
                self._eviction_window_rewards[curFilename] = - 1
            else:
                self._eviction_window_rewards[curFilename] = + 1
            evict_memory_vector.append(np.array([self._eviction_window_cur_values[curFilename], self._eviction_window_actions[curFilename], self._eviction_window_rewards[curFilename],  self._eviction_window_cur_values[curFilename])
            del self._eviction_window_cur_values[curFilename], self._eviction_window_actions[curFilename], self._eviction_window_rewards[curFilename],  self._eviction_window_cur_values[curFilename]


    def get_this_request_stats(self):
        if (self.curRequest + 1) == self.df_length:
            self.write_stats()
            self.reset_stats()
            self.curDay += 1
            self.curRequest = 0
            self.get_dataframe(self.curDay)
        hit = self._cache.check(self.df.loc[self.curRequest, 'Filename'])
        filename = self.df.loc[self.curRequest, 'Filename']
        size = self.df.loc[self.curRequest, 'Size']
        #datatype = self.df.loc[self.curRequest, 'DataType']
        filestats = self._cache.before_request_retrieve(filename, hit, size, self.curRequest)
        cputime = self.df.loc[self.curRequest, 'CPUTime']
        walltime = self.df.loc[self.curRequest, 'WrapWC']
        return hit, filename, size, filestats, cputime, walltime

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
    
    def get_this_request_values(self):
        filestats = self.get_this_request_stats()[3]
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

    def get_next_request_values(self):
        filestats = self.get_next_request_stats()[3]
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

        self.curValues = np.asarray(l)

        return self.curValues
    
    def get_this_file_in_cache_values(self):
        filename = self._cache._filesLRUkeys[self._filesLRU_index]
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

        self.curValues = np.asarray(l)

        return self.curValues

    def get_next_file_in_cache_values(self):
        self._filesLRU_index += 1
        filename = self._cache._filesLRUkeys[self._filesLRU_index]
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

        self.curValues = np.asarray(l)

        return self.curValues

    def get_filename_and_size_of_current_request(self):
        filename = self.df.loc[self.curRequest, 'Filename']
        filestats = self._cache._stats._files[filename]
        return filename, filestats._size
    
    def get_filename_and_size_of_current_cache_file(self):
        filename = self._cache._filesLRUkeys[self._filesLRU_index]
        filestats = self._cache._stats._files[filename]
        return filename, filestats._size
    
    def add_request(self, action):
        hit, filename, _, filestats, cputime, walltime = self.get_this_request_stats()
        if hit == False:
            self._cache._WALLtime_miss += walltime
            self._cache._CPUtime_miss += cputime
        if hit == True:
            self._cache._WALLtime_hit += walltime
            self._cache._CPUtime_hit += cputime
        self._cache.update_recency()
        added = self._cache.update_policy(filename, filestats, hit, action)
        self._cache.after_request(filestats, hit, added)
        print('Request: ' + str(self.curRequest) + ' / ' + str(self.df_length) + '  -  Occupancy: ' + str(round(self._cache.capacity,2)) + '%  -  ' + 'Hit rate: ' + str(round(self._cache._hit/(self._cache._hit + self._cache._miss)*100,2)) +'%' + ' ACTION: ' +  str(action) + ' ' + str(self._cache._get_mean_size(self.curRequest,self.curDay) * len(self._cache._filesLRU) / self._cache._max_size))

    def purge(self):
        for key, value in self._cache._stats._files.items():
            keys_to_delete=[]
            if value._recency > purge_delta:
                keys_to_delete.append(key)
        for key_ in keys_to_delete:        
            del self._cache._stats._files[key_]
