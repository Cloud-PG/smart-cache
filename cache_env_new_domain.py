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
# time_span = 20000
# purge_delta = 20000
it_cpueff_diff = 19
us_cpueff_diff = 10

it_mean_size = 3397.512895452965
it_stdev_size = 2186.2590964080405
it_limsup_size = it_mean_size + it_stdev_size
it_liminf_size = it_mean_size - it_stdev_size
it_delta_size = it_limsup - it_liminf
it_max_size = 47585.251
it_min_size = 0.105
#it_delta_size = 47585.146
it_total_sites = 12
it_total_campaigns = 128

class FileStats(object):
    '''
    object that contains all information about a file: size, number of hits,
     number of miss, last request id, recency (distance from last request), datatype (mc or data)
    '''

    __slots__ = ["_size", "_hit", "_miss", 
                 "_last_request", "_datatype", "_campain", "_sites"]

    def __init__(self, size: float, datatype: int, campain, site):
        self._size: float = size
        self._hit: int = 0
        self._miss: int = 0
        self._last_request: int = 0
        self._datatype: int = datatype
        self._campain: int = campain
        self._sites = set([site])

    def update(self, site, hit: bool = False,):
        '''add hit or miss '''
        if hit:
            self._hit += 1
        else:
            self._miss += 1

    @property
    def tot_requests(self):
        '''returns total number of requests'''
        return self._hit + self._miss

    @property
    def hit(self):
        '''returns total number of hits'''
        return self._hit

    @property
    def miss(self):
        '''returns total number of miss'''
        return self._miss

    @property
    def size(self):
        '''returns size'''
        return self._size

class Stats(object):
    ''' object that contains a list of all filestats '''
    def __init__(self):
        self._files = {}
    
    #def get_or_set_without_updating_stats_list(self, filename: str, size: float, datatype: int, request: int) -> 'FileStats':
    #    '''return filestat for that file: if no stat for that file is in list, creates a new stat and adds it to stats, otherwise it simply gets it '''
    #    stats = None
    #    if filename not in self._files:
    #        stats = FileStats(size, datatype, campain, site)
    #        stats._last_request = request    #if it's the first time, the last request is set to this request in order to have a 0 recency
    #    else:
    #        stats = self._files[filename]
    #        # stats._datatype = datatype
    #        # stats._last_request = request
    #    return stats

    def get_or_set(self, filename: str, size: float, datatype: int, request: int, campain, site) -> 'FileStats':
        '''return filestat for that file: if no stat for that file is in list, creates a new stat and adds it to stats, otherwise it simply gets it '''
        stats = None
        if filename not in self._files:
            stats = FileStats(size, datatype, campain, site)
            stats._last_request = request      #if it's the first time, the last request is set to this request in order to have a 0 recency
            self._files[filename] = stats
        else:
            stats = self._files[filename]
            #stats._last_request = request
            stats._sites |= set([site])
        return stats

class cache(object):
    '''object that emulates a cache'''
    def __init__(self, size: float = 104857600, h_watermark: float = 95., l_watermark: float = 75.):
        self._size: float = 0.0
        self._max_size = size

        self._cached_files = set()
        self._cached_files_keys = []

        self._stats = Stats()

        # Stat attributes
        self._hit: int = 0
        self._miss: int = 0
        self._written_data: float = 0.0
        self._deleted_data: float = 0.0
        self._read_data: float = 0.0

        self._dailyReadOnHit: float = 0.0
        self._dailyReadOnMiss: float = 0.0

        self.daily_anomalous_CPUeff_counter = 0

        self._CPUeff: float = 0.0

        self._h_watermark: float = h_watermark
        self._l_watermark: float = l_watermark

    @property
    def capacity(self) -> float:
        ''' gets current cache occupancy '''
        return (self._size / self._max_size) * 100.

    def hit_rate(self) -> float:
        ''' gets current hit rate '''
        if self._hit:
            return self._hit / (self._hit + self._miss)
        return 0.0

    def check(self, filename: str) -> bool:
        ''' check if a file is in cache '''
        return filename in self._cached_files

    def before_request(self, filename, hit: bool, size, datatype, request: int, campain, site) -> 'FileStats':
        ''' get stats for that file and update it '''
        stats = self._stats.get_or_set(filename, size, datatype, request, campain, site)
        stats.update(site, hit)       
        return stats    

    def update_policy(self, filename, file_stats, hit: bool, action: int) -> bool:
        ''' 
        updates filestats in stats,
        if is not in cache and AI choose to keep it, the filestats are added to cache,
        if it is already in cache, move it to end of cache
        '''
        self._stats._files[filename] = file_stats
        if not hit and action == 0:
            self._cached_files.add(filename)
            return True
        elif hit:
            self._cached_files.add(filename)
            return False

    def after_request(self, fileStats, hit: bool, added: bool):
        ''' update daily cache stats (number of hit/miss, read on hit/miss, written, read) '''
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

    def _get_mean_recency(self, curRequest_from_start):
        ''' returns mean recency of files in cache '''

        if len(self._cached_files) == 0:
            return 0.
        else:
            mean = 0
            counter = 0
            for filename in self._cached_files:
                mean += curRequest_from_start - self._stats._files[filename]._last_request
                counter += 1
            return mean/float(counter)
    
    def _get_mean_frequency(self):
        ''' returns mean total number of requests of files in cache '''

        if len(self._cached_files) == 0:
            return 0.
        else:
            mean = 0
            counter = 0
            for filename in self._cached_files:
                mean += self._stats._files[filename].tot_requests
                counter += 1
            return mean/float(counter)

    
    def _get_mean_size(self):
        ''' returns mean size of files in cache '''

        if len(self._cached_files) == 0:
            return 0.
        else:
            mean = 0
            counter = 0
            for filename in self._cached_files:
                mean += self._stats._files[filename]._size
                counter += 1
            return mean/float(counter)

class dfWrapper(object):
    
    def __init__(self, df):
        for column in df:
            setattr(self, column, df[column].to_numpy())

class WindowElement(object):

    __slots__ = ['counter', 'cur_values', 'reward', 'action', 'next_values']

    def __init__(self, counter=-1, cur_values=[], reward=-1, action=-1, next_values=[]):
        self.counter = counter
        self.cur_values = cur_values
        self.reward = reward
        self.action = action
        self.next_values = next_values

    def concat(self):
        return np.reshape(np.concatenate((
            self.cur_values,
            [self.action],
            [self.reward],
            self.next_values,
        )), (1, 2 * (7 + it_total_sites + it_total_campaigns) + 1 + 1))


class env:
    ''' object that emulates cache mechanism '''

    def __init__(self, start_month, end_month, directory, out_directory, out_name, time_span, purge_delta):
        # print('STO USANDO LA VERSIONE PIU AGGIORNATA')
        # set period
        self._startMonth = start_month
        self._endMonth = end_month
        self._directory = directory
        self._out_directory = out_directory
        self._out_name = out_name
        self._time_span = time_span 
        self._purge_delta = purge_delta

        start = datetime(2018, 1, 1)
        delta = timedelta(days=1)
        idx_start = 0
        cur = start
        while cur.month != start_month:
            idx_start += 1
            cur = start + delta*idx_start
        idx_end = idx_start
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
        self.time_span = time_span
        self.size_tot = 0

        # dictonaries containing actions waiting to be rewarded and then added to memory
        self._request_window_filenames = {}
        self._request_window_elements = {}

        self._eviction_window_filenames = {}
        self._eviction_window_elements = {}

        # initialize experience replay memory vectors
        self.add_memory_vector = np.empty((1, 2 * (7 + it_total_sites + it_total_campaigns) + 1 + 1))
        self.evict_memory_vector = np.empty((1, 2 * (7 + it_total_sites + it_total_campaigns) + 1 + 1))

        # begin reading data
        self.curRequest = -1
        self.curRequest_from_start = -1
        self.get_dataframe(self.curDay)
        self._cached_files_index = -1

        # get first request values setting as curvalues
        self.curRequest += 1
        self.curRequest_from_start += 1
        if (self.curRequest + 1) == self.df_length:
            self.write_stats()
            self.reset_stats()
            self.curDay += 1
            self.curRequest = 0
            self.get_dataframe(self.curDay)

        hit = self._cache.check(self.df.Filename[self.curRequest])
        filename = self.df.Filename[self.curRequest]
        size = self.df.Size[self.curRequest]
        datatype = self.df.DataType[self.curRequest]
        campain = self.df.Campain[self.curRequest]
        site = self.df.SiteName[self.curRequest]
        
        filestats = self._cache.before_request(
            filename, hit, size, datatype, self.curRequest_from_start, campain, site)
        
        #filestats = self._cache._stats.get_or_set_without_updating_stats_list(
        #    filename, size, datatype, self.curRequest_from_start)
        
        one_hot_site = np.zeros(it_total_sites)
        one_hot_campain = np.zeros(it_total_campaigns)
        
        for site in filestats._sites:
            one_hot_site[int(site)] = 1.0
        one_hot_campain[int(filestats._campain)] = 1.0         

        abs_values = np.array([
            filestats._size,
            filestats.tot_requests,
            self.curRequest_from_start - filestats._last_request,
            0. if filestats._datatype == 0 else 1.,
            self._cache._get_mean_recency(
                self.curRequest_from_start),
            self._cache._get_mean_frequency(),
            self._cache._get_mean_size(),
        ])
        
        values = np.concatenate([abs_values, one_hot_site, one_hot_campain])
        self.curValues = values

        print('Environment initialized')

    def write_stats(self):
        ''' write daily stats to .csv file '''
        if self.curDay == self._idx_start:
            with open(self._out_directory + '/' + self._out_name, 'w', newline='') as file:
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
                     'CPU efficiency upper bound',
                     'CPU efficiency lower bound'
                     ])

        with open(self._out_directory + '/' + self._out_name, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
                [str(datetime.fromtimestamp(self.df.reqDay[0])) + ' +0000 UTC',
                 self._cache._size,
                 self._cache.hit_rate() * 100.0,
                 self._cache._hit/self._cache._miss * 100.0,
                 0,
                 self._cache._written_data,
                 self._cache._read_data,
                 self._cache._dailyReadOnHit,
                 self._cache._dailyReadOnMiss,
                 self._cache._deleted_data,
                 self._cache._CPUeff /
                 (self.df_length-self._cache.daily_anomalous_CPUeff_counter),
                 0,
                 0,
                 0,
                 0
                 ])

        return

    def reset_stats(self):
        ''' set all daily stats to zero ''' 
        self._cache._hit: int = 0
        self._cache._miss: int = 0
        self._cache._written_data: float = 0.0
        self._cache._deleted_data: float = 0.0
        self._cache._read_data: float = 0.0

        self._cache._dailyReadOnHit: float = 0.0
        self._cache._dailyReadOnMiss: float = 0.0

        self._cache._CPUeff: float = 0.0

        self._cache.daily_anomalous_CPUeff_counter: int = 0

        return

    def get_dataframe(self, i):
        ''' set the current dataframe to i-th dataframe '''
        file_ = sorted(os.listdir(self._directory))[i]
        with gzip.open(self._directory + '/' + str(file_)) as f:
            df_ = pd.read_csv(f)
            df_['Size'] = df_['Size']/1048576.
            df_ = df_[df_['JobSuccess'] == True]
            df_ = df_[(df_['DataType'] == 0) | (df_['DataType'] == 1)]
            df_.reset_index(drop=True, inplace=True)
            self.df = dfWrapper(df_)
            self.df_length = len(df_.index)          
        print()
        print(file_)
    
    def update_windows_getting_eventual_rewards(self, adding_or_evicting, curFilename, curValues, nextValues, action):
        '''
            - if you are in adding mode, this function updates counters of adding and evicting windows, then
        searches for this filename in both windows: if it finds it, gives reward and removes it from window. 
        Then adding window is updated with new curvalues, nextvalues, action and counter is restarted
            - if you are in evicting mode, this function simply adds this values to eviciton window which should be
            empty when eviction starts
        '''

        if adding_or_evicting == 0:
            size = curValues[0]
            if size <= it_liminf_size:
                coeff = 0 
            elif size >= it_limsup_size:
                coeff = 1 
            else:
                coeff = (size - it_liminf_size)/it_delta_size
            ############################ GIVING REWARD TO ADDITION IF IT IS IN WINDOW AND ADD TO WINDOW ########################################################################
            if curFilename in self._request_window_elements:  # if is in queue                
                obj = self._request_window_elements[curFilename]

                if obj.counter >= self._time_span:
                    if obj.action == 0:
                        obj.reward = - 1 * coeff
                    else:
                        obj.reward = + 1 * coeff
                else:  # is not invalidated yet
                    if obj.action == 0:
                        obj.reward = + 1 * coeff
                    else:
                        obj.reward = - 1 * coeff

                to_add = obj.concat()
                self.add_memory_vector = np.vstack(
                    (self.add_memory_vector, to_add))

            for obj in self._request_window_elements.values():  # increment counters
                obj.counter += 1
            for obj in self._eviction_window_elements.values():  # increment counters
                obj.counter += 1

            self._request_window_elements[curFilename] = WindowElement(
                1, curValues, 0, action, nextValues
            )
            
            ######### GIVING REWARD TO EVICTION AND REMOVING FROM WINDOW ################################################################################################
            if curFilename in self._eviction_window_elements:  # if is in queue
                obj = self._eviction_window_elements[curFilename]
                if obj.counter >= self._time_span:   # is invalidated
                    if obj.action == 0:
                        obj.reward = - 1 * coeff
                    else:
                        obj.reward = + 1 * coeff
                else:  # is not invalidated yet
                    if obj.action == 0:
                        obj.reward = + 1 * coeff
                    else:           
                        obj.reward = - 1 * coeff

                to_add = obj.concat()
                self.evict_memory_vector = np.vstack(
                    (self.evict_memory_vector, to_add))

                del self._eviction_window_elements[curFilename]            
            
        elif adding_or_evicting == 1:
            self._eviction_window_elements[curFilename] = WindowElement(
                1, curValues, 0, action, nextValues
            )

    def clear_remaining_evict_window(self):
        '''gives reward to every action in evict window and deletes it'''

        for obj in self._eviction_window_elements.values():
            size = obj.cur_values[0]
            if size <= it_liminf_size:
                coeff = 0 
            elif size >= it_limsup_size:
                coeff = 1 
            else:
                coeff = (size - it_liminf_size)/it_delta_size
            if obj.action == 0:
                obj.reward = - 1 * coeff
            else:
                obj.reward = + 1 * coeff
            to_add = obj.concat()
            self.evict_memory_vector = np.vstack(
                (self.evict_memory_vector, to_add))

        self._eviction_window_elements.clear()

    def look_for_invalidated_add(self):
        ''' looks for invalidated actions in add window, gives rewards and deletes them'''
        toDelete = set()
        for curFilename, obj in self._request_window_elements.items():
            size = obj.cur_values[0]
            if size <= it_liminf_size:
                coeff = 0 
            elif size >= it_limsup_size:
                coeff = 1 
            else:
                coeff = (size - it_liminf_size)/it_delta_size
            if obj.counter > self._time_span:
                if obj.action == 0:
                    obj.reward = - 1 * coeff
                else:               
                    obj.reward = + 1 * coeff
                to_add = obj.concat()
                self.add_memory_vector = np.vstack(
                    (self.add_memory_vector, to_add))
                toDelete |= set([curFilename,])

        for filename in toDelete:
            del self._request_window_elements[filename]

    def get_next_request_values(self):    
        ''' 
        gets the values of next request to be feeded to AI (from global stats), and sets them as curvalues (and returns them):
        (SIZE - TOT REQUESTS - LAST REQUEST - DATATYPE - MEAN RECENCY - MEAN FREQUENCY - MEAN SIZE)
        '''
        self.curRequest += 1
        self.curRequest_from_start += 1
        if (self.curRequest + 1) > self.df_length:
            self.write_stats()
            self.reset_stats()
            self.curDay += 1
            self.curRequest = 0
            self.get_dataframe(self.curDay)
        
        filename = self.df.Filename[self.curRequest]
        hit = self._cache.check(filename)
        size = self.df.Size[self.curRequest]
        datatype = self.df.DataType[self.curRequest]
        campain = self.df.Campain[self.curRequest]
        site = self.df.SiteName[self.curRequest]
        
        filestats = self._cache.before_request(
            filename, hit, size, datatype, self.curRequest_from_start, campain, site)
        #filestats = self._cache._stats.get_or_set_without_updating_stats_list(
            #filename, size, datatype, self.curRequest_from_start)
        
        one_hot_site = np.zeros(it_total_sites)
        one_hot_campain = np.zeros(it_total_campaigns)
        
        for site in filestats._sites:
            one_hot_site[int(site)] = 1.0
        one_hot_campain[int(filestats._campain)] = 1.0        

        abs_values = np.array([
            filestats._size,
            filestats.tot_requests,
            self.curRequest_from_start - filestats._last_request,
            0. if filestats._datatype == 0 else 1.,
            self._cache._get_mean_recency(
                self.curRequest_from_start),
            self._cache._get_mean_frequency(),
            self._cache._get_mean_size(),
        ])
        
        values = np.concatenate([abs_values, one_hot_site, one_hot_campain])
        self.curValues = values

        return self.curValues

    def get_next_file_in_cache_values(self):
        ''' 
        gets the values of next file in cache (from cache stats) to be feeded to AI and sets them as curvalues (and returns them):
        (SIZE - TOT REQUESTS - LAST REQUEST - DATATYPE - MEAN RECENCY - MEAN FREQUENCY - MEAN SIZE)
        '''
        self._cached_files_index += 1
        filename = self._cache._cached_files_keys[self._cached_files_index]
        filestats = self._cache._stats._files[filename]

        one_hot_site = np.zeros(it_total_sites)
        one_hot_campain = np.zeros(it_total_campaigns)

        for site in filestats._sites:
            one_hot_site[int(site)] = 1.0
        one_hot_campain[int(filestats._campain)] = 1.0      

        abs_values = np.array([
            filestats._size,
            filestats.tot_requests,
            self.curRequest_from_start - filestats._last_request,
            0. if filestats._datatype == 0 else 1.,
            self._cache._get_mean_recency(
                self.curRequest_from_start),
            self._cache._get_mean_frequency(),
            self._cache._get_mean_size(),
        ])

        values = np.concatenate([abs_values, one_hot_site, one_hot_campain])
        
        self.curValues = values

        return self.curValues

    def get_filename_and_size_of_current_request(self):
        ''' returns filename and size of current request '''
        filename = self.df.Filename[self.curRequest]
        filestats = self._cache._stats._files[filename]
        return filename, filestats._size
    
    def get_filename_and_size_of_current_cache_file(self):
        ''' returns filename and size of current file in cache '''
        filename = self._cache._cached_files_keys[self._cached_files_index]
        filestats = self._cache._stats._files[filename]
        return filename, filestats._size
    
    def add_request(self, action):
        ''' update filestats in stats, add to cache if necesary.  update daily stats'''

        hit = self._cache.check(self.df.Filename[self.curRequest])
        filename = self.df.Filename[self.curRequest]
        size = self.df.Size[self.curRequest]
        datatype = self.df.DataType[self.curRequest]
        #filestats = self._cache.before_request(
        #    filename, hit, size, datatype, self.curRequest_from_start)
        filestats = self._cache._stats._files[filename]
        
        filestats._last_request = self.curRequest_from_start
        added = self._cache.update_policy(filename, filestats, hit, action)
        self._cache.after_request(filestats, hit, added)

        #COMPUTE CPU EFFICIENCY        
        cputime = self.df.CPUTime[self.curRequest]
        walltime = self.df.WrapWC[self.curRequest]
        protocol = self.df.Protocol[self.curRequest]
        
        if walltime != 0:
            if hit == False:
                if protocol == 1:               # LOCAL
                    self._cache._CPUeff += cputime/walltime * 100 - it_cpueff_diff
                if protocol == 0:               # REMOTE
                    self._cache._CPUeff += cputime/walltime * 100 
            if hit == True:
                if protocol == 1:               # LOCAL
                    self._cache._CPUeff += cputime/walltime * 100 
                if protocol == 0:               # REMOTE
                    self._cache._CPUeff += cputime/walltime * 100 + it_cpueff_diff

    def purge(self):
        ''' remove data (from stats) whose recency is more than purge_delta '''
        keys_to_delete = set()
        for key, value in self._cache._stats._files.items():
            if (self.curRequest_from_start - value._last_request) > self._purge_delta and key not in self._cache._cached_files:
                keys_to_delete |= set([key,])
        for key_ in keys_to_delete:        
            del self._cache._stats._files[key_]
    
    def current_cpueff_is_anomalous(self):
        '''checks if current request has non valid values'''
        cputime = self.df.CPUTime[self.curRequest]
        walltime = self.df.WrapWC[self.curRequest]     
        cpueff = cputime/walltime * 100
        if cpueff < 0.:
            self._cache.daily_anomalous_CPUeff_counter += 1
            return True
        elif cpueff > 100.:
            self._cache.daily_anomalous_CPUeff_counter += 1
            return True
        elif math.isnan(cpueff) == True:
            self._cache.daily_anomalous_CPUeff_counter += 1
            return True
        elif math.isinf(cpueff) == True:
            self._cache.daily_anomalous_CPUeff_counter += 1
            return True
        else:
            return False

    def check_if_current_is_hit(self):
        return self._cache.check(self.df.Filename[self.curRequest])