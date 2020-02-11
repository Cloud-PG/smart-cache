import csv
import gzip
import json
import os
from collections import OrderedDict
from datetime import datetime

import numpy as np
import pandas as pd

import gym


class FileStats(object):

    def __init__(self, size: float):
        self._size: float = size
        self._hit: int = 0
        self._miss: int = 0
        self._last_request: int = 0

    def update(self, size: float, hit: bool = False):
        self._size = size
        if hit:
            self._hit += 1
        else:
            self._miss += 1

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

class LRU(object):

    def __init__(self, size: float = 104857600):
        """Initialize cache.
        Args:
            size (float): cache size in MB. Default = 10T
        """
        self._size: float = 0.0
        self._max_size = size
        self._files = OrderedDict()

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

    def hit_rate(self) -> float:
        if self._hit:
            return self._hit / (self._hit + self._miss)
        return 0.0

    def check(self, filename: str) -> bool:
        return filename in self._files


    def before_request(self, filename, hit: bool, size, request: int) -> 'FileStats':
        stats = self._stats.get_or_set(filename, size, request)
        stats.update(size, hit)
        return stats

    def update_policy(self, filename, file_stats, hit: bool, toadd: bool) -> bool:
        if not hit and toadd==True:
            #print(self._size + file_stats.size)
            #print(self._max_size)
            if self._size + file_stats.size <= self._max_size:
                self._files[filename] = file_stats
                self._size += file_stats.size
                return True
        
            else:
                while self._size + file_stats.size > self._max_size:
                    _, file_size = self._files.popitem(False)
                    self._size -= file_size
                    self._deleted_data += file_size
                else:
                    self._files[filename] = file_stats
                    self._size += file_stats.size
                    return True
        
        elif hit:
            self._files.move_to_end(filename)
        
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
        

###############################################################################################################
##############################################################################################################
#################################################################################################################

def from_list_to_one_hot(list_):
    with open('features.json') as f:
        features = json.load(f)
    
    features_list=["size","numReq","deltaNumLastRequest","cacheUsage","dataType"]
    one_hot_tot=np.zeros(0)
    for j in range(len(features_list)-1):

        keys=features[features_list[j]]['keys']
        #print(keys)
        n=len(keys)
        one_hot=np.zeros(n+1)
        not_max=False
        
        for i in range(0,n):
            if list_[i]<=float(keys[i]):
                one_hot[i]=1.0
                not_max=True
                break
        if not_max==False:
            one_hot[n]=1.0
        one_hot_tot=np.concatenate((one_hot_tot,one_hot))
        
    if list_[len(features)-1]=='data':
        one_hot_tot=np.concatenate((one_hot_tot,np.zeros(1)))
    else:
        one_hot_tot=np.concatenate((one_hot_tot,np.ones(1)))
        
    return one_hot_tot

class CacheEnv(gym.Env):

    def write_stats(self):
        if self.curDay==0:
            with open('../dQl_100T_it_results.csv', 'w', newline='') as file:
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

        with open('../dQl_100T_it_results.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
            [datetime.fromtimestamp(self.df.loc[0,'reqDay']),
            self._LRU._size,
            self._LRU.hit_rate(),
            self._LRU._hit/self._LRU._miss,
            0,
            self._LRU._written_data,
            self._LRU._read_data,
            self._LRU._dailyReadOnHit,
            self._LRU._dailyReadOnMiss,
            self._LRU._deleted_data,
            (self._LRU._CPUtime_hit + self._LRU._CPUtime_miss)/(self._LRU._WALLtime_hit + self._LRU._WALLtime_miss * 1.15),
            (self._LRU._CPUtime_hit)/(self._LRU._WALLtime_hit),
            (self._LRU._CPUtime_miss)/(self._LRU._WALLtime_miss * 1.15),
            self._LRU._written_data + self._LRU._read_data + self._LRU._deleted_data])

        return 

    def reset_stats(self):
        self._LRU._hit: int = 0
        self._LRU._miss: int = 0
        self._LRU._written_data: float = 0.0
        self._LRU._deleted_data: float = 0.0
        self._LRU._read_data: float = 0.0

        self._LRU._dailyReadOnHit: float = 0.0
        self._LRU._dailyReadOnMiss: float = 0.0

        return
        
    def get_dataframe(self, i):
        directory = "/Users/tommasotedeschi/Documents/Dottorato/ML/deepQlearning/results"
        file_ = sorted(os.listdir(directory))[i]
        with gzip.open(directory+ '/' + str(file_)) as f:
            df_=pd.read_csv(f)
            df_['region']=df_.SiteName.str.split("_",expand=True)[1]
            df_['Size']=df_['Size']/1.049e+6
            df_=df_[df_['region']=='IT']
            df_=df_.reset_index()
            self.df = df_
            self.df_length = len(self.df)

    #function that creates the input vector combining request and cache information
    def get_one_hot(self,df_line,LRU,filestats):
        l=[]
        l.append(df_line['Size'])
        l.append(filestats.tot_requests)
        l.append(self.curRequest - filestats._last_request)
        l.append(self._LRU._size/self._LRU._max_size)
        l.append(df_line['DataType'])
        return from_list_to_one_hot(l)
        #return np.zeros(18)
    
    def __init__(self,total_days):

        self.curRequest = 0
        self._totalDays = total_days
        #self.df = df
        #self.curTotalDailyRequest = len(df) 
        self.reward_range = (-1,1)

        #define action and observations spaces  
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(low=0, high=1, shape=(18,), dtype=np.float16)

        print('Environment initialized')

    def step(self,action):
        
        #get action from NN
        if action==0:
            toadd=True
        else:
            toadd=False
        
        #retrieve the updated stats before choice for this request
        hit = self._LRU.check(self.df.loc[self.curRequest,'Filename'])
        filename = self.df.loc[self.curRequest,'Filename']
        size = self.df.loc[self.curRequest,'Size']
        filestats = self._LRU._stats.get_or_set(filename, size, self.curRequest)
        cputime = self.df.loc[self.curRequest, 'CPUTime']
        walltime =  self.df.loc[self.curRequest, 'WrapWC']

        print(cputime)
        print(walltime)
        
        #modify cache and update stats according to the chosen action
        added = self._LRU.update_policy(filename, filestats, hit, toadd)
        self._LRU.after_request(filestats, hit, added)

        #compute the reward
        if hit==False:
            self._LRU._WALLtime_miss += walltime
            self._LRU._CPUtime_miss += cputime
            if toadd==True:
                reward = 0
                if filestats.tot_requests > 1 or (self.curRequest - filestats._last_request) < 10000 or (self._LRU._dailyReadOnHit/self._LRU._dailyReadOnMiss) < (2./3.):
                    reward -= float(size)
                else:
                    reward += float(size)
            if toadd==False:		
                reward = 0
                if filestats.tot_requests < 100 and (self.curRequest - filestats._last_request) > 10000 and (self._LRU._dailyReadOnHit/self._LRU._dailyReadOnMiss) >= (2./3.):
                    reward -= float(size)
                else:
                    reward += float(size)
        if hit==True:
            self._LRU._WALLtime_hit += walltime
            self._LRU._CPUtime_hit += cputime
            reward = 0.0
            if (self._LRU._dailyReadOnHit / self._LRU._dailyReadOnMiss) >= (2. / 3.):
                reward += float(size)
            else:
                reward -= float(size)

        #get to next request and update stats
        self.curRequest +=1

        #if the day is over, go to the next day, saving and resetting LRU stats 
        done = False

        print(self.curRequest)
        print(self.df_length)
        #print(self.curDay+1)
        #print(self._totalDays)

        if (self.curRequest + 1) == self.df_length:
            self.write_stats()
            self.reset_stats()

            if (self.curDay+1) == self._totalDays:
                done = True
            else:          
                self.curDay += 1  
                self.curRequest = 0
                self.get_dataframe(self.curDay)

        #update stats about the new request
        if done == False:
            hit = self._LRU.check(self.df.loc[self.curRequest,'Filename'])
            filestats = self._LRU.before_request(self.df.loc[self.curRequest,'Filename'],hit,self.df.loc[0,'Size'], self.curRequest)   
        
        with open('reward.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow([reward])
        
        print('day ' + str(self.curDay) + ' / request ' + str(self.curRequest))
        
        return np.array(self.get_one_hot(self.df.loc[self.curRequest],self._LRU,filestats)) , reward, done, {}

    def reset(self):

        with open('reward.csv', 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['reward'])

        #create cache
        self._LRU = LRU()

        #begin with first request
        self.curRequest = 0
        self.curDay = 0

        self.get_dataframe(self.curDay)

        hit = self._LRU.check(self.df.loc[self.curRequest,'Filename'])

        #update stats before choice
        filestats = self._LRU.before_request(self.df.loc[self.curRequest,'Filename'],hit,self.df.loc[0,'Size'], self.curRequest)        

        return np.array(self.get_one_hot(self.df.loc[0],self._LRU,filestats))
