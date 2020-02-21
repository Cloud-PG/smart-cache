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
time_slots = 1000


class FileStats(object):

    __slots__ = ["_size", "_hit", "_miss", "_last_request", "recency"]

    def __init__(self, size: float):
        self._size: float = size
        self._hit: int = 0
        self._miss: int = 0
        self._last_request: int = 0
        self.recency: int = 0

    def update_retrieve(self, size: float, hit: bool = False):
        self._size = size
        self.recency = 0

    def update(self, size: float, hit: bool = False):
        self._size = size
        if hit:
            self._hit += 1
        else:
            self._miss += 1

        self.recency = 0

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
        self._filesLFU = OrderedDict()
        self._filesSize = OrderedDict()

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

    def before_request(self, filename, hit: bool, size, request: int) -> 'FileStats':
        stats = self._stats.get_or_set(filename, size, request)
        stats.update(size, hit)
        return stats

    def before_request_retrieve(self, filename, hit: bool, size, request: int) -> 'FileStats':
        stats = self._stats.get_or_set(filename, size, request)
        stats.update_retrieve(size, hit)
        return stats

    def update_policy(self, filename, file_stats, hit: bool, action: int) -> bool:
        if not hit and (action == 1 or action == 2 or action == 3 or action == 4):
            if self._size + file_stats.size <= self._max_size:
                self._filesLRU[filename] = file_stats
                self._size += file_stats.size
                return True

            else:
                self.__free(file_stats.size, action)
                self._filesLRU[filename] = file_stats
                self._size += file_stats.size
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
            self._written_data += fileStats.size

        self._read_data += fileStats.size

    def __free(self, amount: float, action: int, percentage: bool = False):
        if not percentage:
            size_to_remove = amount
        else:
            size_to_remove = amount * (self._max_size / 100.)
        tot_removed = 0.0
        print("I have to remove with action " + str(action))
        while tot_removed < size_to_remove:
            if action == 1:                                                                         
                _, file_stats = self._filesLRU.popitem(last = False)
                print("removed with action 1 - " + str(self.capacity)  +'%')
            elif action == 2:
                filesLFU =  OrderedDict(sorted(self._filesLRU.items(), key=lambda t: t[1].tot_requests))
                _, file_stats = filesLFU.popitem(last = False)
                del self._filesLRU[_]
                print("removed with action 2 - " + str(self.capacity)  +'%')
            elif action == 3:
                filesSize =  OrderedDict(sorted(self._filesLRU.items(), key=lambda t: t[1]._size))
                _, file_stats = filesSize.popitem(last = False)
                del self._filesLRU[_]
                print("removed with action 3 - " + str(self.capacity)  +'%')
            elif action == 4:
                filesSize =  OrderedDict(sorted(self._filesLRU.items(), key=lambda t: t[1]._size))
                _, file_stats = filesSize.popitem(last = True)
                del self._filesLRU[_]
                print("removed with action 4 - " + str(self.capacity)  +'%')              
            tot_removed += file_stats.size
            self._size -= file_stats.size
            self._deleted_data += file_stats.size

    def check_watermark_and_free(self, action):
        if self.capacity >= self._h_watermark and action != 0:
            #to_free = math.ceil(self.capacity - self._l_watermark)
            to_free = self.capacity - self._l_watermark
            print(to_free)
            self.__free(amount = to_free, action = action, percentage=True)
    
    def update_recency(self):
        for _, value in self._filesLRU.items():
            value.recency += 1

    def _get_mean_recency(self, curRequest):
        if curRequest == 0:
            return 0.
        else:
            list_=[]
            for _,v in self._filesLRU.items():
                list_.append(v.recency)
            return np.array(list_).mean()
    
    def _get_mean_frequency(self, curRequest):
        if curRequest == 0:
            return 0.
        else:
            list_=[]
            for _,v in self._filesLRU.items():
                list_.append(v.tot_requests)
            return np.array(list_).mean()
    
    def _get_mean_size(self, curRequest):
        if curRequest == 0:
            return 0.
        else:
            list_=[]
            for _,v in self._filesLRU.items():
                list_.append(v._size)
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
        # print(keys)
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
            with open('../dQleviction_100T_it_results_shuffle_{}_startmonth{}_endmonth{}.csv'.format('onehot'+ str(self._one_hot),self._startMonth,self._endMonth), 'w', newline='') as file:
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

        with open('../dQleviction_100T_it_results_shuffle_{}_startmonth{}_endmonth{}.csv'.format('onehot'+ str(self._one_hot),self._startMonth,self._endMonth), 'a', newline='') as file:
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
        '''
        directory = "/home/ubuntu/source2018"
        file_ = sorted(os.listdir(directory))[i]
        with gzip.open(directory + '/' + str(file_)) as f:
            df_ = pd.read_csv(f)
            df_['region'] = df_.SiteName.str.split("_", expand=True)[1]
            df_['Size'] = df_['Size']/1.049e+6
            df_ = df_[df_['region'] == 'IT']
            df_ = df_.reset_index()
            self.df = df_
            self.df_length = len(self.df)
        print(file_)
        '''
        directory = "/home/ubuntu/source2018_numeric_it_shuffle_42"
        file_ = sorted(os.listdir(directory))[i]
        with gzip.open(directory + '/' + str(file_)) as f:
            df_ = pd.read_csv(f)
            df_['Size'] = df_['Size']/1.049e+6
            self.df = df_
            self.df_length = len(self.df)
        print(file_)

    # functions that create the input vector combining request and cache information
    def get_simple_values(self, df_line, LRU, filestats):
        l = []
        l.append(df_line['Size'])
        l.append(filestats.tot_requests)
        l.append(self.curRequest - filestats._last_request)
        l.append(self._cache._size/self._cache._max_size)
        datatype = (df_line['DataType'])
        #if datatype == 'data':
        if datatype == 0:
            l.append(0.)
        else:
            l.append(1.)

        l.append(self._cache._get_mean_recency(self.curRequest))
        l.append(self._cache._get_mean_frequency(self.curRequest))
        l.append(self._cache._get_mean_size(self.curRequest))
        
        #print(l)
        return np.asarray(l)

    def get_one_hot(self, df_line, LRU, filestats):
        l = []
        l.append(df_line['Size'])
        l.append(filestats.tot_requests)
        l.append(self.curRequest - filestats._last_request)
        l.append(self._cache._size/self._cache._max_size)
        l.append(df_line['DataType'])
        return from_list_to_one_hot(l)
        # return np.zeros(18)

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
        self.action_space = gym.spaces.Discrete(5)
        if self._one_hot == True:
            self.observation_space = gym.spaces.Box(
                low=0, high=1, shape=(16,), dtype=np.float16)
        else:
            self.observation_space = gym.spaces.Box(
                low=0, high=1, shape=(8,), dtype=np.float16)            

        print('Environment initialized')

    def step(self, action):
        
        if action == 0:
            #print('NOADD')
            toadd = False
        elif action == 1:
            #print('LRU')
            toadd = True
        elif action == 2:
            #print('LFU')
            toadd = True
        elif action == 3:
            #print('Size small')
            toadd = True
        elif action == 4:
            #print('Size big')
            toadd = True

        # retrieve the updated stats before choice for this request
        hit = self._cache.check(self.df.loc[self.curRequest, 'Filename'])
        filename = self.df.loc[self.curRequest, 'Filename']
        size = self.df.loc[self.curRequest, 'Size']
        filestats = self._cache.before_request_retrieve(
            self.df.loc[self.curRequest, 'Filename'], hit, self.df.loc[self.curRequest, 'Size'], self.curRequest)
        cputime = self.df.loc[self.curRequest, 'CPUTime']
        walltime = self.df.loc[self.curRequest, 'WrapWC']

        # modify cache and update stats according to the chosen action
        added = self._cache.update_policy(filename, filestats, hit, action)

        self._cache.after_request(filestats, hit, added)
        #print('Before check: ' + str(self._cache.capacity))
        self._cache.check_watermark_and_free(action)
        #print('After check: ' + str(self._cache.capacity))
        #print()

        # compute the reward


        if hit == False:
            self._cache._WALLtime_miss += walltime
            self._cache._CPUtime_miss += cputime
            if toadd == True:
                reward = 0
                if self._cache._dailyReadOnMiss >= bandwidthLimit:
                    reward -= float(size)
                else:
                    reward += float(size)
            if toadd == False:
                reward = 0
                if self._cache._dailyReadOnHit < self._cache._dailyReadOnMiss/2.0 or self._cache._dailyReadOnMiss > bandwidthLimit:
                    reward -= float(size)
                else:
                    reward += float(size)
        if hit == True:
            self._cache._WALLtime_hit += walltime
            self._cache._CPUtime_hit += cputime
            reward = 0.0
            if self._cache._dailyReadOnHit >= self._cache._dailyReadOnMiss/2.0:
                reward += float(size)
            else:
                reward -= float(size)

        # get to next request and update stats
        self.curRequest += 1
        self._cache.update_recency()

        # if the day is over, go to the next day, saving and resetting LRU stats
        done = False
        self.size_tot +=size
        print('Request: ' + str(self.curRequest) + ' / ' + str(self.df_length) + '  -  Occupancy: ' + str(round(self._cache.capacity,2)) + '%  -  ' + 'Hit rate: ' + str(round(self._cache._hit/(self._cache._hit + self._cache._miss)*100,2)) +'%', end="\r")
        if (self.curRequest + 1) == self.df_length:
            self.write_stats()
            self.reset_stats()
            self.curDay += 1
            self.curRequest = 0
            self.get_dataframe(self.curDay)

        # update stats about the new request
        if done == False:
            hit = self._cache.check(self.df.loc[self.curRequest, 'Filename'])
            filestats = self._cache.before_request(
                self.df.loc[self.curRequest, 'Filename'], hit, self.df.loc[0, 'Size'], self.curRequest)

        with open('reward.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow([reward])

        with open('eviction_policies.csv', 'a') as file:
            writer = csv.writer(file)
            if action == 0:
                writer.writerow(['NOADD'])
            elif action == 1:
                writer.writerow(['LRU'])
            elif action == 2:
                writer.writerow(['LFU'])
            elif action == 3:
                writer.writerow(['Size small'])
            elif action == 4:
                writer.writerow(['Size big'])

        if self._one_hot == True:
            return np.array(self.get_one_hot(self.df.loc[self.curRequest], self._cache, filestats)), reward, done, {}
        else:
            return np.array(self.get_simple_values(self.df.loc[self.curRequest], self._cache, filestats)), reward, done, {}
    
    def reset(self):

        with open('reward.csv', 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['reward'])

        with open('eviction_policies.csv', 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['eviction_policy'])

        # create cache
        self._cache = cache()

        self.size_tot=0

        self.curRequest = 0

        self.get_dataframe(self.curDay)

        hit = self._cache.check(self.df.loc[self.curRequest, 'Filename'])

        # update stats before choice
        filestats = self._cache.before_request(
            self.df.loc[self.curRequest, 'Filename'], hit, self.df.loc[0, 'Size'], self.curRequest)

        if self._one_hot == True:
            return np.array(self.get_one_hot(self.df.loc[0], self._cache, filestats))
        
        else:
            return np.array(self.get_simple_values(self.df.loc[0], self._cache, filestats))
        

