#from keras.models import Sequential
#from keras.layers import Input, Dense, Activation, Flatten
#import cache_env_new_us
#import cache_env_new_linear
#import tensorflow as tf

import keras.optimizers
import numpy as np
import pandas as pd
import math
import random
import cache_env_new
from model_architectures import *
import csv
import array
import os
import argparse
import time

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
parser.add_argument('--invalidated_search_frequency', type = int, default = 100000)
parser.add_argument('--test', type = bool, default = False)


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

input_len = 4 
#input_len = 7 + total_campaigns + total_sites


out_directory = args.out_dir
BATCH_SIZE = args.batch_size
learning_rate = args.lr
_startMonth = args.start_month
_endMonth = args.end_month
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

out_name = out_directory.split("/")[1] + '_results.csv'


if not os.path.isdir(out_directory):
    os.makedirs(out_directory)

if use_target_model == True:
    print('USING TARGET MODEL')

###### FIXED PARAMETERS ############################################################################################

nb_actions = 2
observation_shape = (input_len,)
seed_ = 2019
DailyBandwidth1Gbit = bandwidth * (1000. / 8.) * 60. * 60. * 24.       #MB in a day with <bandwidth> Gbit/s

############## DEFINE ADD AND EVICT MODELS ##########################################################################

model_add = small_dense(input_len, output_activation, nb_actions, learning_rate)
model_evict = small_dense(input_len, output_activation, nb_actions, learning_rate)
if use_target_model == True:
    target_model_add = small_dense(input_len, output_activation, nb_actions, learning_rate)
    target_model_evict = small_dense(input_len, output_activation, nb_actions, learning_rate)


if args.load_weights_from_file == True:
    model_add.load_weights(out_directory + '/' + args.in_add_weights)
    print('ADD WEIGHTS LOADED')
if args.load_weights_from_file == True:
    model_evict.load_weights(out_directory + '/' + args.in_evict_weights)
    print('EVICT WEIGHTS LOADED')


###### START LOOPING #######################################################################################################

if args.region == 'it':
    environment = cache_env_new.env(
        _startMonth, _endMonth, data_directory, out_directory, out_name, time_span_add, time_span_evict, purge_delta, output_activation, cache_size)

#elif args.region == 'us' and output_activation == 'sigmoid':
#    environment = cache_env_new_us.env(
#        _startMonth, _endMonth, data_directory, out_directory, out_name, time_span, purge_delta)

#elif args.region == 'us' and output_activation == 'linear':
#    environment = cache_env_new_us_linear.env(
#        _startMonth, _endMonth, data_directory, out_directory, out_name, time_span, purge_delta)

random.seed(seed_)
adding_or_evicting = 0
step_add = 0
step_evict = 0

eps_add = eps_add_max
eps_evict = eps_evict_max

step_add_decay = 0
step_evict_decay = 0

addition_counter = 0
eviction_counter = 0

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
    if adding_or_evicting == 0:
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
        cur_values = environment.curValues
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

        hit = environment.check_if_current_is_hit()
        anomalous = environment.current_cpueff_is_anomalous()


        # GET THIS REQUEST
        if anomalous == False:
            if environment._cache._dailyReadOnMiss / DailyBandwidth1Gbit * 100 < 95. or hit == True:
                environment.add_request(action)
                curFilename, curSize = environment.get_filename_and_size_of_current_request()

        if report_choices == True:
            daily_add_actions.append(action) 
            if curFilename in res:
                daily_res_add_actions.append(action)
            else:
                daily_notres_add_actions.append(action) 
            if environment.curRequest == 0 and environment.curDay > 0:   
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
        
        if step_add % 1000000 == 0 and hit == False:
            print('Request: ' + str(environment.curRequest) + ' / ' + str(environment.df_length) + ' - ACTION: ' +  str(action) + '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) 
                + '%  -  ' + 'Hit rate: ' + str(round(environment._cache._hit/(environment._cache._hit + environment._cache._miss)*100,2)) +'%' )
            print()
        
        #else: 
        #    print('Request: ' + str(environment.curRequest) + ' / ' + str(environment.df_length) + ' - HIT' + ' -  Occupancy: ' + str(round(environment._cache.capacity,2)) 
        #        + '%  -  ' + 'Hit rate: ' + str(round(environment._cache._hit/(environment._cache._hit + environment._cache._miss)*100,2)) +'%' )
        #    print()
        '''
        # IF IT'S ADDING IS OVER, GIVE REWARD TO ALL EVICTION ACTIONS
        if environment._cache.capacity > environment._cache._h_watermark:
            environment.clear_remaining_evict_window()
        '''

        #PURGE UNUSED STATS
        if (environment.curDay+1) % purge_frequency == 0 and environment.curRequest == 0:
            environment.purge()
        if anomalous == False:
            if environment._cache._dailyReadOnMiss / DailyBandwidth1Gbit * 100 < 95. or hit == True:
                environment.update_windows_getting_eventual_rewards_waiting_no_next_values_accumulate(
                    adding_or_evicting, curFilename, cur_values, action)
        
        # IF ADDING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        #if environment._cache.capacity <= environment._cache._h_watermark:
        next_size = environment.get_next_size()
        next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
        current_occupancy = environment._cache.capacity
        #print(next_size)
        #print(current_occupancy)
        #print(next_occupancy)
        if current_occupancy <= environment._cache._h_watermark and next_occupancy < 100.:
            next_values = environment.get_next_request_values()
            #if anomalous == False:
            #    if environment._cache._dailyReadOnMiss / DailyBandwidth1Gbit * 100 < 95. or hit == True:
                    #environment.update_windows_getting_eventual_rewards_waiting(
                        #adding_or_evicting, curFilename, cur_values, next_values, action)
                    #environment.update_windows_getting_eventual_rewards_waiting_no_next_values(
                        #adding_or_evicting, curFilename, cur_values, action)
                #environment.update_windows_getting_eventual_rewards_waiting_no_next_values_accumulate(
                    #adding_or_evicting, curFilename, cur_values, action)
        
        # REMOVE THE FIRST DUMMY ELEMENT IN MEMORY
        if step_add == 1:
            environment.add_memory_vector = np.delete(
                environment.add_memory_vector, 0, 0)

        # LOOK PERIODICALLY FOR INVALIDATED PENDING ADDING AND EVICTING ACTIONS
        if step_add % args.invalidated_search_frequency == 0:
            #environment.look_for_invalidated_add_evict()
            environment.look_for_invalidated_add_evict_accumulate()

        # KEEP MEMORY LENGTH LESS THAN LIMIT
        while(environment.add_memory_vector.shape[0] > memory):
            environment.add_memory_vector = np.delete(
                environment.add_memory_vector, 0, 0)
    

        # TRAIN NETWORK
        if step_add > warm_up_steps_add and test == False:
            batch = environment.add_memory_vector[np.random.randint(0, environment.add_memory_vector.shape[0], BATCH_SIZE), :]
            #train_cur_vals ,train_actions, train_rewards, train_next_vals = np.split(batch, [input_len, input_len + 1, input_len + 2] , axis = 1)
            train_cur_vals ,train_actions, train_rewards = np.split(batch, [input_len, input_len + 1] , axis = 1)
            target = model_add.predict_on_batch(train_cur_vals)
            if debug == True:
                print('PREDICT ON BATCH')
                print(batch)
                print(target)
                print()
            if use_target_model == False:
                #predictions = model_add.predict_on_batch(train_next_vals)
                predictions = model_add.predict_on_batch(train_cur_vals)
                for i in range(0,BATCH_SIZE):
                    action_ = int(train_actions[i])
                    target[i,action_] = train_rewards[i] + gamma * mellowmax(mm_omega, predictions[i])   
            else:
                #predictions = target_model_add.predict_on_batch(train_next_vals)
                predictions = target_model_add.predict_on_batch(train_cur_vals)
                for i in range(0,BATCH_SIZE):
                    action_ = int(train_actions[i])               
                    target[i,action_] = train_rewards[i] + gamma * max(predictions[i])  
            
            model_add.train_on_batch(train_cur_vals, target)

    ### EVICTING #############################################################################################################
    elif adding_or_evicting == 1: 
        
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
        cur_values = environment.curValues
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
        curFilename, curSize = environment.get_filename_and_size_of_current_cache_file()
        if action == 1:
            #del environment._cache._cached_files[curFilename]
            environment._cache._cached_files.remove(curFilename)
            environment._cache._size -= curSize
            environment._cache._deleted_data += curSize
        
        if report_choices == True:
            daily_evict_actions.append(action)
            if curFilename in res:
                daily_res_evict_actions.append(action)
            else:
                daily_notres_evict_actions.append(action) 


        if step_evict % 10000000 == 0:
            print('Freeing memory ' + str(environment._cached_files_index) + '/' + str(len(environment._cache._cached_files_keys)) + 
                    '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) + '%  - action: ' + str(action))
            print()
        
        #print(environment._cached_files_index)
        #print(len(environment._cache._cached_files_keys))

        end_of_cache = False
        if environment._cached_files_index + 1 == len(environment._cache._cached_files_keys):
            end_of_cache = True
        
        #print(end_of_cache)
        environment.update_windows_getting_eventual_rewards_waiting_no_next_values_accumulate(
            adding_or_evicting, curFilename, cur_values, action)

        # IF EVICTING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        #if environment._cached_files_index + 1 != len(environment._cache._cached_files_keys) and environment._cache.capacity >= low_watermark:
        if end_of_cache == False and environment._cache.capacity >= low_watermark:
            next_values = environment.get_next_file_in_cache_values()
            #environment.update_windows_getting_eventual_rewards_waiting(
                #adding_or_evicting, curFilename, cur_values, next_values, action)
            #environment.update_windows_getting_eventual_rewards_waiting_no_next_values(
                #adding_or_evicting, curFilename, cur_values, action)
            #environment.update_windows_getting_eventual_rewards_waiting_no_next_values_accumulate(
                #adding_or_evicting, curFilename, cur_values, action)
        
        # REMOVE THE FIRST DUMMY ELEMENT IN MEMORY
        if step_evict == 1:
            environment.evict_memory_vector = np.delete(environment.evict_memory_vector, 0, 0)

        # KEEP MEMORY LENGTH LESS THAN LIMIT
        while(environment.evict_memory_vector.shape[0] > memory):
            environment.evict_memory_vector = np.delete(environment.evict_memory_vector, 0, 0)
        
        # TRAIN NETWORK
        if step_evict > warm_up_steps_evict and test == False:
            batch = environment.evict_memory_vector[np.random.randint(0, environment.evict_memory_vector.shape[0], BATCH_SIZE), :]
            #train_cur_vals ,train_actions, train_rewards, train_next_vals = np.split(batch, [input_len, input_len+1, input_len+2] , axis = 1)
            train_cur_vals ,train_actions, train_rewards = np.split(batch, [input_len, input_len+1] , axis = 1)
            target = model_evict.predict_on_batch(train_cur_vals)
            
            if debug == True:
                print('PREDICT ON BATCH')
                print(batch)
                print(target)
                print()
            if use_target_model == False:
                #predictions = model_evict.predict_on_batch(train_next_vals)
                predictions = model_evict.predict_on_batch(train_cur_vals)
                for i in range(0,BATCH_SIZE):  
                    action_ = int(train_actions[i])
                    target[i,action_] = train_rewards[i] + gamma * mellowmax(mm_omega, predictions[i])  
            else:
                #predictions = target_model_add.predict_on_batch(train_next_vals)
                predictions = target_model_add.predict_on_batch(train_cur_vals)
                for i in range(0,BATCH_SIZE):  
                    action_ = int(train_actions[i])
                    target[i,action_] = train_rewards[i] + gamma * max(predictions[i])  
            
            model_evict.train_on_batch(train_cur_vals, target)

    #### STOP ADDING ##############################################################################################
    #if adding_or_evicting == 0 and environment._cache.capacity > environment._cache._h_watermark:
    #print(environment._cache._size)
    #orint(next_values[0])
    #next_occupancy = (environment._cache._size + next_values[0]*1000) / environment._cache._max_size * 100
    next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
    current_occupancy = environment._cache.capacity
    #print(current_occupancy)
    #print(next_occupancy)
    if adding_or_evicting == 0 and (current_occupancy > environment._cache._h_watermark or next_occupancy > 100.):
        adding_or_evicting = 1 
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
    if adding_or_evicting == 1 and (end_of_cache == True or environment._cache.capacity < low_watermark):
        with open(out_directory + '/occupancy.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow([environment._cache.capacity])
        #print('oooooooooooo')
        next_size = environment.get_next_size()
        next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
        if environment._cache.capacity < environment._cache._h_watermark and next_occupancy < 100.:
            adding_or_evicting = 0
            eviction_counter += 1
            cur_values = environment.get_next_request_values()
        else:
            environment._cache._cached_files_keys = list(environment._cache._cached_files)
            random.shuffle(environment._cache._cached_files_keys)
            to_print = np.asarray(environment._cache._cached_files_keys)
            environment._cached_files_index = -1
            cur_values = environment.get_next_file_in_cache_values()
        #adding_or_evicting = 0
        #eviction_counter += 1
        #cur_values = environment.get_next_request_values()

    ### END #####################################################################################################
    if environment.curDay == environment._idx_end:
        end = True
        model_add.save_weights(out_directory + '/' + args.out_add_weights)
        model_evict.save_weights(out_directory + '/' + args.out_evict_weights)
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
    if adding_or_evicting == 0:

        cur_values = environment.curValues
        cur_values_ = np.reshape(cur_values, (1, input_len))
        print(model_add.predict(cur_values_))
        action = np.argmax(model_add.predict(cur_values_))
        hit = environment.check_if_current_is_hit()
        anomalous = environment.current_cpueff_is_anomalous()


        # GET THIS REQUEST
        if anomalous == False:
            if environment._cache._dailyReadOnMiss / DailyBandwidth1Gbit * 100 < 95. or hit == True:
                environment.add_request(action)
                curFilename, curSize = environment.get_filename_and_size_of_current_request()

        if report_choices == True:
            daily_add_actions.append(action) 
            if curFilename in res:
                daily_res_add_actions.append(action)
            else:
                daily_notres_add_actions.append(action) 
            if environment.curRequest == 0 and environment.curDay > 0:   
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
            print('Request: ' + str(environment.curRequest) + ' / ' + str(environment.df_length) + '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) 
                + '%  -  ' + 'Hit rate: ' + str(round(environment._cache._hit/(environment._cache._hit + environment._cache._miss)*100,2)) +'%' + ' ACTION: ' +  str(action))
            print()
        
        #PURGE UNUSED STATS
        if (environment.curDay+1) % purge_frequency == 0 and environment.curRequest == 0:
            environment.purge()
        
        # IF ADDING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        #if environment._cache.capacity <= environment._cache._h_watermark:
        next_size = environment.get_next_size()
        next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
        current_occupancy = environment._cache.capacity
        #print(next_size)
        #print(current_occupancy)
        #print(next_occupancy)
        if current_occupancy <= environment._cache._h_watermark and next_occupancy < 100.:
            next_values = environment.get_next_request_values()
    
    ### EVICTING #############################################################################################################
    elif adding_or_evicting == 1: 
        
        cur_values = environment.curValues
        cur_values_ = np.reshape(cur_values, (1, input_len))
        print(model_evict.predict(cur_values_))
        action = np.argmax(model_evict.predict(cur_values_))
        
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
    
    if adding_or_evicting == 0 and (current_occupancy > environment._cache._h_watermark or next_occupancy > 100.):
        adding_or_evicting = 1 
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
    if adding_or_evicting == 1 and (end_of_cache == True or environment._cache.capacity < low_watermark):
        with open(out_directory + '/occupancy.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow([environment._cache.capacity])
        next_size = environment.get_next_size()
        next_occupancy = (environment._cache._size + next_size) / environment._cache._max_size * 100
        if environment._cache.capacity < environment._cache._h_watermark and next_occupancy < 100.:
            adding_or_evicting = 0
            eviction_counter += 1
            cur_values = environment.get_next_request_values()
        else:
            environment._cache._cached_files_keys = list(environment._cache._cached_files)
            random.shuffle(environment._cache._cached_files_keys)
            to_print = np.asarray(environment._cache._cached_files_keys)
            environment._cached_files_index = -1
            cur_values = environment.get_next_file_in_cache_values()







