import keras.optimizers
import numpy as np
import pandas as pd
import math
import random
#import cache_env_new
import cache_env_cpp
from model_architectures import *
import csv
import array
import os
import argparse
import time
import sys
import gzip
from datetime import datetime, timedelta
#import cacheenvnewcython as cache_env_new
np.set_printoptions(threshold=sys.maxsize)

input_len = 6
bandwidthLimit = (1000000. / 8.) * 60. * 60. * 24
# time_span = 20000
# purge_delta = 20000
it_cpueff_diff = 19
us_cpueff_diff = 10
it_maxsize = 47585.251
it_minsize = 0.105
#it_delta_size = 47585.146
it_mean_size = 3397.512895452965
it_stdev_size = 2186.2590964080405
it_limsup_size = it_mean_size + it_stdev_size
it_liminf_size = it_mean_size - it_stdev_size
it_delta_size = it_limsup_size - it_liminf_size

it_total_sites = 12
it_total_campaigns = 128
us_total_sites = 21
us_total_campaigns = 192

parser = argparse.ArgumentParser()
parser.add_argument('--data', type=str, 
                    default='/home/ubuntu/source2018_numeric_it_with_avro_order')
parser.add_argument('--time_span_add', type=int, default=30000)
parser.add_argument('--time_span_evict', type=int, default=30000)
parser.add_argument('--start_month', type=int, default=1)
parser.add_argument('--end_month', type=int, default=2)
parser.add_argument('--batch_size', type=int, default=32)
parser.add_argument('--out_dir', type=str,
                    default='results/results_ok_stats_async_quick_cleaned_huberTF')
parser.add_argument('--lr', type=float, default=0.00001, help='learning rate')
parser.add_argument('--memory', type=int, default=1000000)
parser.add_argument('--annealing_type', type=str, default='exponential')
parser.add_argument('--slope_add', type=float, default=0.00001)
parser.add_argument('--slope_evict', type=float, default=0.00001)
parser.add_argument('--decay_rate_add', type=float, default=0.00001)
parser.add_argument('--decay_rate_evict', type=float, default=0.00001)
parser.add_argument('--warm_up_steps_add', type=int, default=60000)
parser.add_argument('--warm_up_steps_evict', type=int, default=60000)
parser.add_argument('--eps_add_max', type=float, default=1.0)
parser.add_argument('--eps_add_min', type=float, default=0.1)
parser.add_argument('--eps_evict_max', type=float, default = 1.0)
parser.add_argument('--eps_evict_min', type=float, default = 0.1)
parser.add_argument('--gamma', type=float, default = 0.5)
parser.add_argument('--mm_omega', type=float, default = 1.0)
parser.add_argument('--load_weights_from_file', type = bool, default = False)
parser.add_argument('--in_add_weights', type = str, default = 'weights_add.h5')
parser.add_argument('--in_evict_weights', type = str, default = 'weights_evict.h5')
parser.add_argument('--out_add_weights', type = str, default = 'weights_add.h5')
parser.add_argument('--out_evict_weights', type = str, default = 'weights_evict.h5')
parser.add_argument('--debug', type = bool, default = False)
parser.add_argument('--low_watermark', type = float, default = 0.)
parser.add_argument('--purge_delta', type = int, default = 50000)
parser.add_argument('--purge_frequency', type = int, default = 7)
parser.add_argument('--use_target_model', type = bool, default = False)
parser.add_argument('--target_update_frequency_add', type = int, default = 10000)
parser.add_argument('--target_update_frequency_evict', type = int, default = 10000)
parser.add_argument('--region', type = str, default = 'it')
parser.add_argument('--output_activation', type = str, default = 'sigmoid')
parser.add_argument('--cache_size', type = int, default = 104857600)
parser.add_argument('--report_choices', type = bool, default = False)
parser.add_argument('--report_particular_choices', type = bool, default = False)
parser.add_argument('--bandwidth', type = float, default = 10.)
parser.add_argument('--write_everything', type = bool, default = False)
parser.add_argument('--timing', type = bool, default = False)
parser.add_argument('--invalidated_search_frequency', type = int, default = 30000)
parser.add_argument('--test', type = bool, default = False)
parser.add_argument('--seed', type = int, default = 2019)
parser.add_argument('--eviction_frequency', type = int, default = 50000)


args = parser.parse_args()

if args.region == 'it':
    data_directory = args.data
    total_sites = it_total_sites
    total_campaigns = it_total_campaigns
elif args.region == 'us':
    data_directory = '/home/ubuntu/source2018_numeric_us_with_avro_order'
    total_sites = us_total_sites
    total_campaigns = us_total_campaigns
else:
    data_directory = args.data

print(data_directory)

input_len = 6
#input_len = 7 + total_campaigns + total_sites

out_directory = args.out_dir
BATCH_SIZE = args.batch_size
learning_rate = args.lr
startMonth = args.start_month
endMonth = args.end_month
memory = args.memory
annealing_type = args.annealing_type
slope_add = args.slope_add
slope_evict = args.slope_evict
decay_rate_add = args.decay_rate_add
decay_rate_evict = args.decay_rate_evict
warm_up_steps_add = args.warm_up_steps_add
warm_up_steps_evict = args.warm_up_steps_evict
eps_add_max = args.eps_add_max
eps_evict_max = args.eps_evict_max
eps_add_min = args.eps_add_min
eps_evict_min = args.eps_evict_min
gamma = args.gamma
mm_omega = args.mm_omega
time_span_add = args.time_span_add
time_span_evict = args.time_span_evict
debug = args.debug
low_watermark = args.low_watermark
purge_delta = args.purge_delta
purge_frequency = args.purge_frequency
use_target_model = args.use_target_model
target_update_frequency_add = args.target_update_frequency_add
target_update_frequency_evict = args.target_update_frequency_evict
output_activation = args.output_activation
cache_size = args.cache_size
report_choices = args.report_choices
report_particular_choices = args.report_particular_choices
bandwidth = args.bandwidth
write_everything = args.write_everything
timing = args.timing
test = args.test
eviction_frequency = args.eviction_frequency

out_name = out_directory.split("/")[1] + '_results.csv'


if not os.path.isdir(out_directory):
    os.makedirs(out_directory)

if use_target_model == True:
    print('USING TARGET MODEL')

###### FIXED PARAMETERS ############################################################################################

nb_actions = 2
observation_shape = (input_len,)
seed_ = args.seed
DailyBandwidth1Gbit = bandwidth * (1000. / 8.) * 60. * 60. * 24.       #MB in a day with <bandwidth> Gbit/s

############## DEFINE ADD AND EVICT MODELS ##########################################################################
model_add = small_dense(input_len, output_activation, nb_actions, learning_rate, seed_)
model_evict = small_dense(input_len, output_activation, nb_actions, learning_rate, seed_ + 1)
if use_target_model == True:
    target_model_add = small_dense(input_len, output_activation, nb_actions, learning_rate, seed_ + 2)
    target_model_evict = small_dense(input_len, output_activation, nb_actions, learning_rate, seed_ + 3)

if args.load_weights_from_file == True:
    #model_add.load_weights(out_directory + '/' + args.in_add_weights)
    model_add.load_weights(args.in_add_weights)
    print('ADD WEIGHTS LOADED')
if args.load_weights_from_file == True:
    #model_evict.load_weights(out_directory + '/' + args.in_evict_weights)
    model_evict.load_weights(args.in_evict_weights)
    print('EVICT WEIGHTS LOADED')
###### START LOOPING #######################################################################################################

if args.region == 'it':
    environment = cache_env_cpp.env(
        startMonth, endMonth, data_directory, out_directory, out_name, time_span_add, time_span_evict, purge_delta, output_activation, cache_size, 2019)


def write_stats():
    ''' write daily stats to .csv file '''
    if environment._curDay == environment._idx_start:
        with open(environment._out_directory + '/' + environment._out_name, 'w', newline='') as file:
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
                    'CPU efficiency lower bound',
                    ])

    with open(environment._out_directory + '/' + environment._out_name, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(
            [str(datetime.fromtimestamp(dataframe_.df.reqDay[0])) + ' +0000 UTC',
                environment._cache._size,
                environment._cache.hit_rate() * 100.0,
                environment._cache._hit/environment._cache._miss * 100.0,
                0,
                environment._cache._written_data,
                environment._cache._read_data,
                environment._cache._dailyReadOnHit,
                environment._cache._dailyReadOnMiss,
                environment._cache._deleted_data,
                environment._cache._CPUeff /
                (environment._df_length-environment._cache._daily_anomalous_CPUeff_counter),
                0,
                0,
                0,
                0,                    
                ])

    return

def reset_stats():
    ''' set all daily stats to zero ''' 
    environment._cache._hit = 0
    environment._cache._miss = 0
    environment._cache._written_data = 0.0
    environment._cache._deleted_data = 0.0
    environment._cache._read_data = 0.0

    environment._cache._dailyReadOnHit = 0.0
    environment._cache._dailyReadOnMiss = 0.0
    environment._cache._daily_rewards_add = []
    environment._cache._daily_rewards_evict = []
    environment._cache._CPUeff = 0.0

    environment._cache._daily_anomalous_CPUeff_counter = 0

    return

class dfWrapper(object):
    
    def __init__(self, df):
        for column in df:
            setattr(self, column, df[column].to_numpy())

class dataframe(object):
    def __init__(self):
        self.df = None
    def get_dataframe(self,i):
        ''' set the current dataframe to i-th dataframe '''
        file_ = sorted(os.listdir(environment._directory))[i]
        with gzip.open(environment._directory + '/' + str(file_)) as f:
            df_ = pd.read_csv(f)
            df_['Size'] = df_['Size']/1048576.
            df_ = df_[df_['JobSuccess'] == True]
            df_ = df_[(df_['DataType'] == 0) | (df_['DataType'] == 1)]
            df_.reset_index(drop=True, inplace=True)
            self.df = dfWrapper(df_)
            environment._df_length = len(df_.index)          
        print()
        print(file_)

def get_next_request_values():    
    ''' 
    gets the values of next request to be feeded to AI (from global stats), and sets them as curvalues (and returns them):
    (SIZE - TOT REQUESTS - LAST REQUEST - DATATYPE - MEAN RECENCY - MEAN FREQUENCY - MEAN SIZE)
    '''
    environment._curRequest += 1
    environment._curRequest_from_start += 1
    if (environment._curRequest + 1) > environment._df_length:
        write_stats()
        reset_stats()
        environment._curDay += 1
        environment._curRequest = 0
        dataframe_.get_dataframe(environment._curDay)
    
    filename = dataframe_.df.Filename[environment._curRequest]
    hit = environment.check_in_cache(filename)
    size = dataframe_.df.Size[environment._curRequest]
    datatype = dataframe_.df.DataType[environment._curRequest]
    filestats = environment._cache.before_request(
        filename, hit, size, datatype, environment._curRequest_from_start)
    
    environment.set_curValues(filestats._size/1000,filestats._hit + filestats._miss, environment._curRequest_from_start - filestats._last_request,0. if filestats._datatype == 0 else 1., environment._cache.capacity()/100.,environment._cache.hit_rate())

def get_next_size():    
    ''' 
    gets the values of next request to be feeded to AI (from global stats), and sets them as curvalues (and returns them):
    (SIZE - TOT REQUESTS - LAST REQUEST - DATATYPE - MEAN RECENCY - MEAN FREQUENCY - MEAN SIZE)
    '''
    tmp_req = environment._curRequest
    tmp_day = environment._curDay
    tmp_req += 1
    tmp_df = dataframe_.df

    if (tmp_req + 1) > environment._df_length:
        tmp_day += 1
        tmp_req = 0
        file_ = sorted(os.listdir(environment._directory))[tmp_day]
        with gzip.open(environment._directory + '/' + str(file_)) as f:
            df_ = pd.read_csv(f)
            df_['Size'] = df_['Size']/1048576.
            df_ = df_[df_['JobSuccess'] == True]
            df_ = df_[(df_['DataType'] == 0) | (df_['DataType'] == 1)]
            df_.reset_index(drop=True, inplace=True)
            tmp_df = dfWrapper(df_)  

    return tmp_df.Size[tmp_req]


def get_next_file_in_cache_values():
    ''' 
    gets the values of next file in cache (from cache stats) to be feeded to AI and sets them as curvalues (and returns them):
    (SIZE - TOT REQUESTS - LAST REQUEST - DATATYPE - MEAN RECENCY - MEAN FREQUENCY - MEAN SIZE)
    '''
    environment._cached_files_index += 1
    #filename = environment._cache._cached_files_keys[environment._cached_files_index]
    filename = environment.get_filename_from_cache(environment._cached_files_index)
    filestats = environment.get_stats(filename)
    #filestats = environment._cache._stats._files[filename]
    
    environment.set_curValues(filestats._size/1000,filestats._hit + filestats._miss, environment._curRequest_from_start - filestats._last_request,0. if filestats._datatype == 0 else 1., environment._cache.capacity()/100.,environment._cache.hit_rate())


def get_filename_and_size_of_current_request():
    ''' returns filename and size of current request '''
    filename = dataframe_.df.Filename[environment._curRequest]
    filestats = environment.get_stats(filename)
    #filestats = environment._cache._stats._files[filename]
    return filename, filestats._size

def get_filename_and_size_of_current_cache_file():
    ''' returns filename and size of current file in cache '''
    #filename = environment._cache._cached_files_keys[environment._cached_files_index]
    filename = environment.get_filename_from_cache(environment._cached_files_index)
    #filestats = environment._cache._stats._files[filename]
    filestats = environment.get_stats(filename)
    return filename, filestats._size

def add_request(action):
    ''' update filestats in stats, add to cache if necesary.  update daily stats'''

    hit = environment.check_in_cache(dataframe_.df.Filename[environment._curRequest])
    filename = dataframe_.df.Filename[environment._curRequest]
    size = dataframe_.df.Size[environment._curRequest]
    datatype = dataframe_.df.DataType[environment._curRequest]
    filestats = environment.get_stats(filename)
    #filestats = environment._cache._stats._files[filename]
    filestats._last_request = environment._curRequest_from_start
    added = environment._cache.update_policy(filename, filestats, hit, action)
    environment._cache.after_request(filestats, hit, added)

    #COMPUTE CPU EFFICIENCY        
    cputime = dataframe_.df.CPUTime[environment._curRequest]
    walltime = dataframe_.df.WrapWC[environment._curRequest]
    protocol = dataframe_.df.Protocol[environment._curRequest]
    
    if walltime != 0:
        if hit == False:
            if protocol == 1:               # LOCAL
                environment._cache._CPUeff += cputime/walltime * 100 - it_cpueff_diff
            if protocol == 0:               # REMOTE
                environment._cache._CPUeff += cputime/walltime * 100 
        if hit == True:
            if protocol == 1:               # LOCAL
                environment._cache._CPUeff += cputime/walltime * 100 
            if protocol == 0:               # REMOTE
                environment._cache._CPUeff += cputime/walltime * 100 + it_cpueff_diff

def current_cpueff_is_anomalous():
    '''checks if current request has non valid values'''
    cputime = dataframe_.df.CPUTime[environment._curRequest]
    walltime = dataframe_.df.WrapWC[environment._curRequest]     
    cpueff = cputime/walltime * 100
    if cpueff < 0.:
        environment._cache._daily_anomalous_CPUeff_counter += 1
        return True
    elif cpueff > 100.:
        environment._cache._daily_anomalous_CPUeff_counter += 1
        return True
    elif math.isnan(cpueff) == True:
        environment._cache._daily_anomalous_CPUeff_counter += 1
        return True
    elif math.isinf(cpueff) == True:
        environment._cache._daily_anomalous_CPUeff_counter += 1
        return True
    else:
        return False

#def check_if_current_is_hit(self):
#   return environment._cache.check(self.df.Filename[self.curRequest])








environment._adding_or_evicting = 0
environment._curRequest = -1
environment._curRequest_from_start = -1
dataframe_ = dataframe() 
dataframe_.get_dataframe(environment._curDay)
environment._cached_files_index = -1

environment._curRequest += 1
environment._curRequest_from_start += 1
if (environment._curRequest + 1) == environment._df_length:
    write_stats()
    reset_stats()
    environment._curDay += 1
    environment._curRequest = 0
    dataframe_.get_dataframe(environment._curDay)

hit = environment.check_in_cache(dataframe_.df.Filename[environment._curRequest])
filename = dataframe_.df.Filename[environment._curRequest]
size = dataframe_.df.Size[environment._curRequest]
datatype = dataframe_.df.DataType[environment._curRequest]

filestats = environment._cache.before_request(
    filename, hit, size, datatype, environment._curRequest_from_start)

environment.set_curValues(filestats._size/1000,filestats._hit + filestats._miss, environment._curRequest_from_start - filestats._last_request,0. if filestats._datatype == 0 else 1., environment._cache.capacity()/100.,environment._cache.hit_rate())

random.seed(seed_)
np.random.seed(seed_)
environment._adding_or_evicting = 0
step_add = 0
step_evict = 0

eps_add = eps_add_max
eps_evict = eps_evict_max

step_add_decay = 0
step_evict_decay = 0

addition_counter = 0
eviction_counter = 0

daily_reward = []
daily_add_actions = []
daily_evict_actions = []
daily_res_add_actions = []
daily_notres_add_actions = []
daily_res_evict_actions = []
daily_notres_evict_actions = []

res = [116731, 828304, 832129, 834347, 901365, 908566, 911003, 1170936, 1171350, 1354776]

with open(out_directory + '/occupancy.csv', 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['occupancy'])

end = False
if timing == True:
        now = time.time()
        with open(out_directory + '/timing.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['time', 
            'num_stats_files', 
            'num_cached_files', 
            'add_memory_length', 
            'add_pending_length',
            'evict_memory_length', 
            'evict_pending_length'])

end_of_cache = False
while end == False and test == False:
    #print(len(environment._cache._cached_files))
    #print(environment._curRequest_from_start)
    #print(step_add)
    ######## REPORT TIMING ##################################################################################################
    if timing == True:
        before = now
        now = time.time()
        with open(out_directory + '/timing.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow([now - before, 
            environment.get_stats_len,
            environment.get_num_files_in_cache(),
            environment.get_add_memory_size(),
            environment.get_add_window_size(),
            environment.get_evict_memory_size(),
            environment.get_evict_window_size()])

    ######## ADDING ###########################################################################################################
    if environment._adding_or_evicting == 0:
        end_of_cache = False
        if use_target_model == True:
            if step_add % target_update_frequency_add == 0:
                target_model_add.set_weights(model_add.get_weights()) 

        # UPDATE STUFF
        step_add += 1
        if eps_add > eps_add_min and step_add > warm_up_steps_add:
            step_add_decay += 1
            if annealing_type == 'exponential':
                eps_add = eps_add_max * math.exp(- decay_rate_add * step_add_decay)
            elif annealing_type == 'linear':
                eps_add = eps_add_max - slope_add * step_add_decay
        cur_values = environment._curValues
        if debug == True:
            print(cur_values)
        if step_add % 10000000 == 0:
            print('epsilon = ' + str(eps_add))
                
        # GET ACTION
        rnd_eps = random.random()
        if rnd_eps < eps_add or step_add < warm_up_steps_add:
            rnd = random.random()
            if rnd < 0.5:
                action = 0
            else:
                action = 1
        else:
            cur_values_ = np.reshape(cur_values, (1, input_len))
            action = np.argmax(model_add.predict(cur_values_))
        if write_everything == True:
            action = 0

        hit = environment.check_in_cache(dataframe_.df.Filename[environment._curRequest])
        anomalous = current_cpueff_is_anomalous()


        # GET THIS REQUEST
        if anomalous == False:
            if environment._cache._dailyReadOnMiss / DailyBandwidth1Gbit * 100 < 95. or hit == True:
                add_request(action)
                curFilename, curSize = get_filename_and_size_of_current_request()

        if report_choices == True:
            daily_add_actions.append(action) 
            if curFilename in res:
                daily_res_add_actions.append(action)
            else:
                daily_notres_add_actions.append(action) 
            #if environment.curRequest == 0 and environment.curDay > 0:
            if environment._curRequest + 1 == environment._df_length  and environment._curDay > 0:      
                with open(out_directory + '/rewards_add_{}.csv'.format(environment._curDay), 'w') as file:
                    writer = csv.writer(file)
                    writer.writerow(['reward','eps_add'])
                    for i in range(0,len(environment._cache._daily_rewards_add)):
                        writer.writerow([environment._cache._daily_rewards_add[i], eps_add])
                with open(out_directory + '/rewards_evict_{}.csv'.format(environment._curDay), 'w') as file:
                    writer = csv.writer(file)
                    writer.writerow(['reward', 'eps_evict'])
                    for i in range(0,len(environment._cache._daily_rewards_evict)):
                        writer.writerow([environment._cache._daily_rewards_evict[i], eps_evict])
                with open(out_directory + '/addition_choices_{}.csv'.format(environment._curDay), 'w') as file:
                    writer = csv.writer(file)
                    writer.writerow(['addition_choice'])
                    for i in range(0,len(daily_add_actions)):
                        writer.writerow([daily_add_actions[i]])
                with open(out_directory + '/eviction_choices_{}.csv'.format(environment._curDay), 'w') as file:
                    writer = csv.writer(file)
                    writer.writerow(['eviction_choice'])
                    for i in range(0,len(daily_evict_actions)):
                        writer.writerow([daily_evict_actions[i]])
                if report_particular_choices == True:
                    with open(out_directory + '/res_addition_choices_{}.csv'.format(environment._curDay), 'w') as file:
                        writer = csv.writer(file)
                        writer.writerow(['addition_choice'])
                        for i in range(0,len(daily_res_add_actions)):
                            writer.writerow([daily_res_add_actions[i]])
                    with open(out_directory + '/notres_addition_choices_{}.csv'.format(environment._curDay), 'w') as file:
                        writer = csv.writer(file)
                        writer.writerow(['addition_choice'])
                        for i in range(0,len(daily_notres_add_actions)):
                            writer.writerow([daily_notres_add_actions[i]])
                    with open(out_directory + '/res_eviction_choices_{}.csv'.format(environment._curDay), 'w') as file:
                        writer = csv.writer(file)
                        writer.writerow(['eviction_choice'])
                        for i in range(0,len(daily_res_evict_actions)):
                            writer.writerow([daily_res_evict_actions[i]])
                    with open(out_directory + '/notres_eviction_choices_{}.csv'.format(environment._curDay), 'w') as file:
                        writer = csv.writer(file)
                        writer.writerow(['eviction_choice'])
                        for i in range(0,len(daily_notres_evict_actions)):
                            writer.writerow([daily_notres_evict_actions[i]])
                daily_add_actions.clear()
                daily_evict_actions.clear()
                daily_res_add_actions.clear()
                daily_notres_add_actions.clear()                
                daily_res_evict_actions.clear()
                daily_notres_evict_actions.clear()
        
        if debug == True and step_add % 1 == 0 and hit == False:
            print('Request: ' + str(environment._curRequest) + ' / ' + str(environment._df_length) + ' - ACTION: ' +  str(action) + '  -  Occupancy: ' + str(round(environment._cache.capacity(),2)) 
                + '%  -  ' + 'Hit rate: ' + str(round(environment._cache._hit/(environment._cache._hit + environment._cache._miss)*100,2)) +'%', '\r' )
            print()
        
        elif debug == True and step_add % 1 == 0 and hit == True:
            print('Request: ' + str(environment._curRequest) + ' / ' + str(environment._df_length) + ' - HIT' + ' -  Occupancy: ' + str(round(environment._cache.capacity(),2)) 
                + '%  -  ' + 'Hit rate: ' + str(round(environment._cache._hit/(environment._cache._hit + environment._cache._miss)*100,2)) +'%' , '\r')
            print()

        #PURGE UNUSED STATS
        if (environment._curDay+1) % purge_frequency == 0 and environment._curRequest == 0:
            environment.purge()
        if anomalous == False:
            if environment._cache._dailyReadOnMiss / DailyBandwidth1Gbit * 100 < 95. or hit == True:
                environment.update_windows_getting_eventual_rewards_accumulate(curFilename, action)
        
        # IF ADDING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        next_size = get_next_size()
        next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
        current_occupancy = environment._cache.capacity()
        if current_occupancy <= environment._cache._h_watermark and next_occupancy < 100.:
            get_next_request_values()
        
        # REMOVE THE FIRST DUMMY ELEMENT IN MEMORY
        #if step_add == 1:
        #    environment.add_memory_vector = np.delete(environment.add_memory_vector, 0, 0)

        # LOOK PERIODICALLY FOR INVALIDATED PENDING ADDING AND EVICTING ACTIONS
        if step_add % args.invalidated_search_frequency == 0:
            print('LOOKING FOR INVALIDATED FILES')
            environment.look_for_invalidated_add_evict_accumulate()

        # KEEP MEMORY LENGTH LESS THAN LIMIT
        while(environment.get_add_memory_size() > memory):
            environment.delete_first_add_memory()
    
        # TRAIN NETWORK
        if step_add > warm_up_steps_add and test == False:
            #print('started training')
            batch = np.asarray(environment.get_random_batch(BATCH_SIZE))    
            #batch = environment.add_memory_vector[np.random.randint(0, environment.add_memory_vector.shape[0], BATCH_SIZE), :]
            train_cur_vals ,train_actions, train_rewards, train_next_vals = np.split(batch, [input_len, input_len + 1, input_len + 2] , axis = 1)
            #train_cur_vals ,train_actions, train_rewards = np.split(batch, [input_len, input_len + 1] , axis = 1)
            target = model_add.predict_on_batch(train_cur_vals)
            if debug == True and step_add % 1 == 0:
            #if debug == True and step_add == warm_up_steps_add + 1:
                print('PREDICT ON BATCH')
                print(batch)
                print(target)
                print()
            if use_target_model == False:
                predictions = model_add.predict_on_batch(train_next_vals)
                #predictions = model_add.predict_on_batch(train_cur_vals)
                for i in range(0,BATCH_SIZE):
                    action_ = int(train_actions[i])
                    target[i,action_] = train_rewards[i] + gamma * mellowmax(mm_omega, predictions[i])   
            else:
                predictions = target_model_add.predict_on_batch(train_next_vals)
                #predictions = target_model_add.predict_on_batch(train_cur_vals)
                for i in range(0,BATCH_SIZE):
                    action_ = int(train_actions[i])               
                    target[i,action_] = train_rewards[i] + gamma * max(predictions[i])  
            
            model_add.train_on_batch(train_cur_vals, target)

    ### EVICTING #############################################################################################################
    elif environment._adding_or_evicting == 1: 
        
        if use_target_model == True:
            if step_evict % target_update_frequency_evict == 0:
                target_model_evict.set_weights(model_evict.get_weights()) 
        
        # UPDATE STUFF
        step_evict += 1
        if eps_evict > eps_evict_min and step_evict > warm_up_steps_evict:
            step_evict_decay += 1
            if annealing_type == 'exponential':
                eps_evict = eps_evict_max * math.exp(- decay_rate_evict * step_evict_decay)
            elif annealing_type == 'linear':
                eps_evict = eps_evict_max - slope_evict * step_evict_decay
        cur_values = environment._curValues
        if debug == True:
            print(cur_values)
        if step_evict % 100000000 == 0:
            print('epsilon = ' + str(eps_evict))
        
        # GET ACTION
        rnd_eps = random.random()
        if rnd_eps < eps_evict or step_evict < warm_up_steps_evict:
            rnd = random.random()
            if rnd < 0.5:
                action = 0
            else:
                action = 1
        else:
            cur_values_ = np.reshape(cur_values, (1, input_len))
            action = np.argmax(model_evict.predict(cur_values_))
        
        # IF ADDING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        curFilename, curSize = get_filename_and_size_of_current_cache_file()
        if action == 1:
            #environment._cache._cached_files.remove(curFilename)
            environment.remove_from_cache(curFilename)
            environment._cache._size -= curSize
            environment._cache._deleted_data += curSize
        
        if report_choices == True:
            daily_evict_actions.append(action)
            if curFilename in res:
                daily_res_evict_actions.append(action)
            else:
                daily_notres_evict_actions.append(action) 

        if debug == True and step_evict % 1 == 0:
            print('Freeing memory ' + str(environment._cached_files_index) + '/' + str(len(environment._cache._cached_files_keys)) +  ' - action: ' + str(action)
                    + '  -  Occupancy: ' + str(round(environment._cache.capacity(),2)) + '%') 
            print()

        end_of_cache = False
        if environment._cached_files_index + 1 == len(environment._cache._cached_files_keys):
            end_of_cache = True        
        environment.update_windows_getting_eventual_rewards_accumulate(curFilename,action)

        # IF EVICTING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        if end_of_cache == False and environment._cache.capacity() >= low_watermark:
            get_next_file_in_cache_values()

        # REMOVE THE FIRST DUMMY ELEMENT IN MEMORY
        #if step_evict == 1:
        #    environment.evict_memory_vector = np.delete(environment.evict_memory_vector, 0, 0)

        # KEEP MEMORY LENGTH LESS THAN LIMIT
        while(environment.get_evict_memory_size() > memory):
            environment.delete_first_evict_memory()
        
        # TRAIN NETWORK
        if step_evict > warm_up_steps_evict and test == False:
            batch = np.asarray(environment.get_random_batch(BATCH_SIZE))
            #batch = environment.evict_memory_vector[np.random.randint(0, environment.evict_memory_vector.shape[0], BATCH_SIZE), :]
            train_cur_vals ,train_actions, train_rewards, train_next_vals = np.split(batch, [input_len, input_len+1, input_len+2] , axis = 1)
            #train_cur_vals ,train_actions, train_rewards = np.split(batch, [input_len, input_len+1] , axis = 1)
            target = model_evict.predict_on_batch(train_cur_vals)
            
            if debug == True and step_evict % 100 == 0:
                print('PREDICT ON BATCH')
                print(batch)
                print(target)
                print()
            if use_target_model == False:
                predictions = model_evict.predict_on_batch(train_next_vals)
                #predictions = model_evict.predict_on_batch(train_cur_vals)
                for i in range(0,BATCH_SIZE):  
                    action_ = int(train_actions[i])
                    target[i,action_] = train_rewards[i] + gamma * mellowmax(mm_omega, predictions[i])  
            else:
                predictions = target_model_add.predict_on_batch(train_next_vals)
                #predictions = target_model_add.predict_on_batch(train_cur_vals)
                for i in range(0,BATCH_SIZE):  
                    action_ = int(train_actions[i])
                    target[i,action_] = train_rewards[i] + gamma * max(predictions[i])  
            
            model_evict.train_on_batch(train_cur_vals, target)

    #### STOP ADDING ##############################################################################################
    next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
    current_occupancy = environment._cache.capacity()
    if environment._adding_or_evicting == 0 and (current_occupancy > environment._cache._h_watermark or next_occupancy > 100. or (step_add % eviction_frequency == 0 and step_add != 0)):
        print('START EVICTION')
        environment._adding_or_evicting = 1 
        addition_counter += 1
        environment.create_cached_files_keys_list()
        #environment._cache._cached_files_keys = list(environment._cache._cached_files)
        #random.shuffle(environment._cache._cached_files_keys)
        #to_print = np.asarray(environment._cache._cached_files_keys)
        environment._cached_files_index = -1
        get_next_file_in_cache_values()
        
    ### STOP EVICTING ##########################################################################################
    if environment._adding_or_evicting == 1 and (end_of_cache == True or environment._cache.capacity() < low_watermark):
        print('STOP EVICTION')
        with open(out_directory + '/occupancy.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow([environment._cache.capacity])
        next_size = get_next_size()
        next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
        if environment._cache.capacity() < environment._cache._h_watermark and next_occupancy < 100.:
            environment._adding_or_evicting = 0
            eviction_counter += 1
            get_next_request_values()
        else:
            environment._cache._cached_files_keys = list(environment._cache._cached_files)
            random.shuffle(environment._cache._cached_files_keys)
            to_print = np.asarray(environment._cache._cached_files_keys)
            environment._cached_files_index = -1
            get_next_file_in_cache_values()

    ### END #####################################################################################################
    if environment._curDay == environment._idx_end:
        end = True
        #model_add.save_weights(out_directory + '/' + args.out_add_weights)
        #model_evict.save_weights(out_directory + '/' + args.out_evict_weights)
        model_add.save_weights(args.out_add_weights)
        model_evict.save_weights(args.out_evict_weights)
        print('WEIGHTS SAVED')

###################################
###############################
############################à
############################
###########################
#########################


while end == False and test == True:
    end_of_cache = False
    ######## REPORT TIMING ##################################################################################################
    if timing == True:
        before = now
        now = time.time()
        with open(out_directory + '/timing.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow([now - before, 
            len(environment._cache._stats._files),
            len(environment._cache._cached_files), 
            environment.add_memory_vector.shape[0],
            len(environment._request_window_elements),
            environment.evict_memory_vector.shape[0],
            len(environment._eviction_window_elements)])

    ######## ADDING ###########################################################################################################
    if environment._adding_or_evicting == 0:

        cur_values = environment.curValues
        cur_values_ = np.reshape(cur_values, (1, input_len))
        print(model_add.predict(cur_values_))
        action = np.argmax(model_add.predict(cur_values_))
        hit = environment.check_in_cache()
        anomalous = environment.current_cpueff_is_anomalous()
        #action = 0

        # GET THIS REQUEST
        if anomalous == False:
            if environment._cache._dailyReadOnMiss / DailyBandwidth1Gbit * 100 < 95. or hit == True:
                add_request(action)
                curFilename, curSize = environment.get_filename_and_size_of_current_request()

        if report_choices == True:
            daily_add_actions.append(action) 
            if curFilename in res:
                daily_res_add_actions.append(action)
            else:
                daily_notres_add_actions.append(action) 
            if environment.curRequest == 0 and environment.curDay > 0:   
                with open(out_directory + '/rewards_{}.csv'.format(environment.curDay), 'w') as file:
                    writer = csv.writer(file)
                    writer.writerow(['reward'])
                    for i in range(0,len(daily_add_actions)):
                        writer.writerow([daily_add_actions[i]])
                with open(out_directory + '/addition_choices_{}.csv'.format(environment.curDay), 'w') as file:
                    writer = csv.writer(file)
                    writer.writerow(['addition_choice'])
                    for i in range(0,len(daily_add_actions)):
                        writer.writerow([daily_add_actions[i]])
                with open(out_directory + '/eviction_choices_{}.csv'.format(environment.curDay), 'w') as file:
                    writer = csv.writer(file)
                    writer.writerow(['eviction_choice'])
                    for i in range(0,len(daily_evict_actions)):
                        writer.writerow([daily_evict_actions[i]])
                if report_particular_choices == True:
                    with open(out_directory + '/res_addition_choices_{}.csv'.format(environment.curDay), 'w') as file:
                        writer = csv.writer(file)
                        writer.writerow(['addition_choice'])
                        for i in range(0,len(daily_res_add_actions)):
                            writer.writerow([daily_res_add_actions[i]])
                    with open(out_directory + '/notres_addition_choices_{}.csv'.format(environment.curDay), 'w') as file:
                        writer = csv.writer(file)
                        writer.writerow(['addition_choice'])
                        for i in range(0,len(daily_notres_add_actions)):
                            writer.writerow([daily_notres_add_actions[i]])
                    with open(out_directory + '/res_eviction_choices_{}.csv'.format(environment.curDay), 'w') as file:
                        writer = csv.writer(file)
                        writer.writerow(['eviction_choice'])
                        for i in range(0,len(daily_res_evict_actions)):
                            writer.writerow([daily_res_evict_actions[i]])
                    with open(out_directory + '/notres_eviction_choices_{}.csv'.format(environment.curDay), 'w') as file:
                        writer = csv.writer(file)
                        writer.writerow(['eviction_choice'])
                        for i in range(0,len(daily_notres_evict_actions)):
                            writer.writerow([daily_notres_evict_actions[i]])
                daily_add_actions.clear()
                daily_evict_actions.clear()
                daily_res_add_actions.clear()
                daily_notres_add_actions.clear()                
                daily_res_evict_actions.clear()
                daily_notres_evict_actions.clear()
        
        if step_add % 1 == 0:
            print('Request: ' + str(environment.curRequest) + ' / ' + str(environment._df_length) + '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) 
                + '%  -  ' + 'Hit rate: ' + str(round(environment._cache._hit/(environment._cache._hit + environment._cache._miss)*100,2)) +'%' + ' ACTION: ' +  str(action))
            print()
        
        #PURGE UNUSED STATS
        if (environment.curDay+1) % purge_frequency == 0 and environment.curRequest == 0:
            environment.purge()
        
        # IF ADDING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        next_size = environment.get_next_size()
        next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
        current_occupancy = environment._cache.capacity
        if current_occupancy <= environment._cache._h_watermark and next_occupancy < 100.:
            next_values = environment.get_next_request_values()
    
    ### EVICTING #############################################################################################################
    elif environment._adding_or_evicting == 1: 
        
        # GET ACTION
        rnd_eps = random.random()
        if rnd_eps < eps_evict:
            rnd = random.random()
            if rnd < 0.5:
                action = 0
            else:
                action = 1
        else:
            cur_values_ = np.reshape(cur_values, (1, input_len))
            action = np.argmax(model_evict.predict(cur_values_))
        print(model_evict.predict(cur_values_))
        #cur_values = environment.curValues
        #cur_values_ = np.reshape(cur_values, (1, input_len))
        #print(model_evict.predict(cur_values_))
        #action = np.argmax(model_evict.predict(cur_values_))
        #action = 1
        # IF ADDING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        curFilename, curSize = environment.get_filename_and_size_of_current_cache_file()
        if action == 1:
            environment._cache._cached_files.remove(curFilename)
            environment._cache._size -= curSize
            environment._cache._deleted_data += curSize
        
        if report_choices == True:
            daily_evict_actions.append(action)
            if curFilename in res:
                daily_res_evict_actions.append(action)
            else:
                daily_notres_evict_actions.append(action) 

        if step_evict % 1 == 0:
            print('Freeing memory ' + str(environment._cached_files_index) + '/' + str(len(environment._cache._cached_files_keys)) + 
                    '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) + '%  - action: ' + str(action))
            print()
        

        end_of_cache = False
        if environment._cached_files_index + 1 == len(environment._cache._cached_files_keys):
            end_of_cache = True

        # IF EVICTING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        #if environment._cached_files_index + 1 != len(environment._cache._cached_files_keys) and environment._cache.capacity >= low_watermark:
        if end_of_cache == False and environment._cache.capacity >= low_watermark:
            next_values = environment.get_next_file_in_cache_values()

    #### STOP ADDING ##############################################################################################
    #if adding_or_evicting == 0 and environment._cache.capacity > environment._cache._h_watermark:
    #next_occupancy = (environment._cache._size + next_values[0]) / environment._cache._max_size * 100
    next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
    current_occupancy = environment._cache.capacity
    
    if environment._adding_or_evicting == 0 and (current_occupancy > environment._cache._h_watermark or next_occupancy > 100.):
        environment._adding_or_evicting = 1 
        addition_counter += 1
        environment._cache._cached_files_keys = list(environment._cache._cached_files)
        random.shuffle(environment._cache._cached_files_keys)
        to_print = np.asarray(environment._cache._cached_files_keys)
        environment._cached_files_index = -1
        cur_values = environment.get_next_file_in_cache_values()
        #print('STARTED EVICTION AT: ' + str(step_add) + ' - files in cache are ' + str(np.sort(to_print)))
        #print()

        
    ### STOP EVICTING ##########################################################################################
   #if adding_or_evicting == 1 and (environment._cached_files_index + 1 == len(environment._cache._cached_files_keys) or environment._cache.capacity < low_watermark):
    if environment._adding_or_evicting == 1 and (end_of_cache == True or environment._cache.capacity < low_watermark):
        
        with open(out_directory + '/occupancy.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow([environment._cache.capacity])
        next_size = environment.get_next_size()
        next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
        if environment._cache.capacity < environment._cache._h_watermark and next_occupancy < 100.:
            environment._adding_or_evicting = 0
            eviction_counter += 1
            cur_values = environment.get_next_request_values()
        else:
            environment._cache._cached_files_keys = list(environment._cache._cached_files)
            random.shuffle(environment._cache._cached_files_keys)
            to_print = np.asarray(environment._cache._cached_files_keys)
            environment._cached_files_index = -1
            cur_values = environment.get_next_file_in_cache_values()







