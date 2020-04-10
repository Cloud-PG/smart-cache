from keras.models import Sequential
from keras.layers import Input, Dense, Activation, Flatten
import tensorflow as tf
import numpy as np
import pandas as pd
import math
import random
import cache_env_async_quick_set
import csv
import array
import os
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument('--data', type=str, 
                    default='/home/ubuntu/source2018_numeric_it_with_avro_order')
parser.add_argument('--time_span', type=int, default=30000)
parser.add_argument('--start_month', type=int, default=1)
parser.add_argument('--end_month', type=int, default=2)
parser.add_argument('--batch_size', type=int, default=32)
parser.add_argument('--out_dir', type=str,
                    default='results/results_ok_stats_async_quick_cleaned_huberTF')
parser.add_argument('--out_name', type=str, default='dQL_add_evic.csv')
parser.add_argument('--lr', type=float, default=0.01, help='learning rate')
parser.add_argument('--memory', type=int, default=30000)
parser.add_argument('--decay_rate_add', type=float, default=0.00001)
parser.add_argument('--decay_rate_evict', type=float, default=0.00001)
parser.add_argument('--warm_up_steps', type=int, default=30000)
parser.add_argument('--eps_add_max', type=float, default=1.0)
parser.add_argument('--eps_add_min', type=float, default=0.1)
parser.add_argument('--eps_evict_max', type=float, default = 1.0)
parser.add_argument('--eps_evict_min', type=float, default = 0.1)
parser.add_argument('--gamma', type=float, default = 0.5)
parser.add_argument('--mm_omega', type=float, default = 1.0)
parser.add_argument('--load_add_weights_from_file', type = str, default = None)
parser.add_argument('--load_evict_weights_from_file', type = str, default = None)
parser.add_argument('--out_add_weights', type = str, default = 'weights_add.h5')
parser.add_argument('--out_evict_weights', type = str, 
                    default = 'weights_evict.h5')
parser.add_argument('--debug', type = bool, default = False)
parser.add_argument('--low_watermark', type = float, default = 0.)
parser.add_argument('--purge_delta', type = int, default = 50000)
parser.add_argument('--purge_frequency', type = int, default = 2)
parser.add_argument('--use_target_model', type = bool, default = False)
parser.add_argument('--target_update_frequency_add', type = int, default = 10000)
parser.add_argument('--target_update_frequency_evict', type = int, default = 10000)

args = parser.parse_args()

BATCH_SIZE = args.batch_size
data_directory = args.data
_startMonth = args.start_month
_endMonth = args.end_month
memory = args.memory
decay_rate_add = args.decay_rate_add
decay_rate_evict = args.decay_rate_evict
no_training_steps = args.warm_up_steps
eps_add = args.eps_add_max
eps_evict = args.eps_evict_max
eps_add_min = args.eps_add_min
eps_evict_min = args.eps_evict_min
gamma = args.gamma
mm_omega = args.mm_omega
time_span = args.time_span
debug = args.debug
low_watermark = args.low_watermark
purge_delta = args.purge_delta
purge_frequency = args.purge_frequency
use_target_model = args.use_target_model
target_update_frequency_add = args.target_update_frequency_add
target_update_frequency_evict = args.target_update_frequency_evict

out_directory = args.out_dir
out_name = args.out_name

if not os.path.isdir(out_directory):
    os.makedirs(out_directory)

if use_target_model == True:
    print('USING TARGET MODEL')
###### FIXED PARAMETERS ####################################################################################################################################

nb_actions = 2
observation_shape = (7,)
seed_ = 2019
DailyBandwidth1Gbit = 10. * (1000. / 8.) * 60. * 60. * 24.       #MB in a day with 10 Gbit/s

####### EXTRA FUNCTION DEFINITIONS ##################################################################################################################
def mellowmax(omega, x):
    sum_ = sum((math.exp(omega * val) for val in x))
    return math.log(sum_/len(x))/omega

def huber_loss(y_true, y_pred, clip_delta=1.0):
    error = y_true - y_pred
    cond  = tf.keras.backend.abs(error) < clip_delta

    squared_loss = 0.5 * tf.keras.backend.square(error)
    linear_loss  = clip_delta * (tf.keras.backend.abs(error) - 0.5 * clip_delta)

    return tf.where(cond, squared_loss, linear_loss)

def huber_loss_mean(y_true, y_pred, clip_delta=1.0):
    return tf.keras.backend.mean(huber_loss(y_true, y_pred, clip_delta))

############## DEFINE ADD AND EVICT MODELS #########################################################################################################

print('USING HUBER LOSS FROM TENSORFLOW')

model_evict = Sequential()
model_evict.add(Dense(16, input_dim=7))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(32))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(64))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(128))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(64))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(32))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(nb_actions))
model_evict.add(Activation('sigmoid'))
print(model_evict.summary())
#model_evict.compile(optimizer = 'adam', loss = huber_loss_mean)
model_evict.compile(optimizer='adam', loss=tf.keras.losses.Huber())

if use_target_model == True:
    target_model_evict = Sequential()
    target_model_evict.add(Dense(16, input_dim=7))
    target_model_evict.add(Activation('sigmoid'))
    target_model_evict.add(Dense(32))
    target_model_evict.add(Activation('sigmoid'))
    target_model_evict.add(Dense(64))
    target_model_evict.add(Activation('sigmoid'))
    target_model_evict.add(Dense(128))
    target_model_evict.add(Activation('sigmoid'))
    target_model_evict.add(Dense(64))
    target_model_evict.add(Activation('sigmoid'))
    target_model_evict.add(Dense(32))
    target_model_evict.add(Activation('sigmoid'))
    target_model_evict.add(Dense(nb_actions))
    target_model_evict.add(Activation('sigmoid'))
    print(model_evict.summary())
    #target_model_evict.compile(optimizer = 'adam', loss = huber_loss_mean)
    target_model_evict.compile(optimizer='adam', loss=tf.keras.losses.Huber())

if args.load_evict_weights_from_file is None == False:
    model_evict.load_weights(args.load_evict_weights_from_file)
        
model_add = Sequential()
model_add.add(Dense(16,input_dim = 7))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(32))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(64))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(128))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(64))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(32))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(nb_actions))
model_add.add(Activation('sigmoid'))
print(model_add.summary())
#model_add.compile(optimizer = 'adam', loss = huber_loss_mean)
model_add.compile(optimizer='adam', loss=tf.keras.losses.Huber())

if use_target_model == True:
    target_model_add = Sequential()
    target_model_add.add(Dense(16,input_dim = 7))
    target_model_add.add(Activation('sigmoid'))
    target_model_add.add(Dense(32))
    target_model_add.add(Activation('sigmoid'))
    target_model_add.add(Dense(64))
    target_model_add.add(Activation('sigmoid'))
    target_model_add.add(Dense(128))
    target_model_add.add(Activation('sigmoid'))
    target_model_add.add(Dense(64))
    target_model_add.add(Activation('sigmoid'))
    target_model_add.add(Dense(32))
    target_model_add.add(Activation('sigmoid'))
    target_model_add.add(Dense(nb_actions))
    target_model_add.add(Activation('sigmoid'))
    print(model_add.summary())
    #target_model_add.compile(optimizer = 'adam', loss = huber_loss_mean)
    target_model_add.compile(optimizer='adam', loss=tf.keras.losses.Huber())

if args.load_add_weights_from_file is None == False:
    model_add.load_weights(args.load_add_weights_from_file)

###### START LOOPING ############################################################################################################################
environment = cache_env_async_quick_set.env(
    _startMonth, _endMonth, data_directory, out_directory, out_name, time_span, purge_delta)
random.seed(seed_)
adding_or_evicting = 0
step_add = 0
step_evict = 0
step_add = 0
step_evict = 0

# addition_counter = 0
# eviction_counter = 0

with open(out_directory + '/stats.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['time', 'stats_files', 'cache_files'])

with open(out_directory + '/occupancy.csv', 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['occupancy'])

end = False
now = time.time()
while end == False:
    before = now
    now = time.time()
    with open(out_directory + '/stats.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow([now - before, len(environment._cache._stats._files), len(environment._cache._cached_files)])

    if (environment.curDay+1) % purge_frequency == 0 and environment.curRequest == 0:
        environment.purge()

    ######## ADDING ###########################################################################################################
    if adding_or_evicting == 0:
        if use_target_model == True:
            if step_add % target_update_frequency_add == 0:
                target_model_add.set_weights(model_add.get_weights()) 
        # UPDATE STUFF
        step_add += 1
        if eps_add > eps_add_min:
            eps_add = math.exp(- decay_rate_add * step_add)
        cur_values = environment.curValues
        if step_add % 1000 == 0:
            print('epsilon = ' + str(eps_add))
        
        # GET ACTION
        rnd_eps = random.random()
        if rnd_eps < eps_add or step_add < no_training_steps:
            rnd = random.random()
            if rnd < 0.5:
                action = 0
            else:
                action = 1
        else:
            cur_values_ = np.reshape(cur_values, (1, 7))
            action = np.argmax(model_add.predict(cur_values_))

        hit = environment.check_if_current_is_hit()
        anomalous = environment.current_cpueff_is_anomalous()

        if debug == True and anomalous == False:
            print('ADDING-------------------------------------------------------------------------------------')
            print('CURVALUES')
            cur_values_ = np.reshape(cur_values, (1, 7))
            print(cur_values)

        # GET THIS REQUEST
        if anomalous == False:
            if environment._cache._dailyReadOnMiss / DailyBandwidth1Gbit * 100 < 95. or hit == True:
                environment.add_request(action)
                curFilename, curSize = environment.get_filename_and_size_of_current_request()

        if step_add % 1000 == 0:
            print('Request: ' + str(environment.curRequest) + ' / ' + str(environment.df_length) + '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) 
                + '%  -  ' + 'Hit rate: ' + str(round(environment._cache._hit/(environment._cache._hit + environment._cache._miss)*100,2)) +'%' + ' ACTION: ' +  str(action))
        
        # IF IT'S ADDING IS OVER, GIVE REWARD TO ALL EVICTION ACTIONS
        if environment._cache.capacity > environment._cache._h_watermark:
            environment.clear_remaining_evict_window()
        
        # IF ADDING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        if environment._cache.capacity <= environment._cache._h_watermark:
            next_values = environment.get_next_request_values()
            if anomalous == False:
                if environment._cache._dailyReadOnMiss / DailyBandwidth1Gbit * 100 < 95. or hit == True:
                    environment.update_windows_getting_eventual_rewards(
                        adding_or_evicting, curFilename, cur_values, next_values, action)
        
        # REMOVE THE FIRST DUMMY ELEMENT IN MEMORY
        if step_add == 1000:
            environment.add_memory_vector = np.delete(
                environment.add_memory_vector, 0, 0)

        # LOOK PERIODICALLY FOR INVALIDATED PENDING ADDING ACTIONS
        if step_add % 5000:
            environment.look_for_invalidated_add()

        # KEEP MEMORY LENGTH LESS THAN LIMIT
        while(environment.add_memory_vector.shape[0] > memory):
            environment.add_memory_vector = np.delete(
                environment.add_memory_vector, 0, 0)

        # TRAIN NETWORK
        if step_add > no_training_steps:
            batch = environment.add_memory_vector[np.random.randint(0, environment.add_memory_vector.shape[0], BATCH_SIZE), :]
            train_cur_vals ,train_actions, train_rewards, train_next_vals = np.split(batch, [7,8,9] , axis = 1)
            target = model_add.predict_on_batch(train_cur_vals)
            if debug == True:
                print('BATCH')
                print(batch)
                print()
                print('PREDICT ON BATCH')
                print(target)
                print()
            if use_target_model == False:
                predictions = model_add.predict_on_batch(train_next_vals)
                for i in range(0,BATCH_SIZE):
                    action_ = int(train_actions[i])
                    target[i,action_] = train_rewards[i] + gamma * mellowmax(mm_omega, predictions[i])   
            else:
                predictions = target_model_add.predict_on_batch(train_next_vals)
                for i in range(0,BATCH_SIZE):
                    action_ = int(train_actions[i])               
                    target[i,action_] = train_rewards[i] + gamma * max(predictions[i])  
            
            model_add.train_on_batch(train_cur_vals, target)
            if debug == True:
                print('TARGET')
                print(target)
                print()
        
    ### EVICTING #############################################################################################################
    elif adding_or_evicting == 1: 
        if use_target_model == True:
            if step_evict % target_update_frequency_evict == 0:
                target_model_evict.set_weights(model_evict.get_weights()) 
        
        # UPDATE STUFF
        step_evict += 1
        if eps_evict > eps_evict_min:
            eps_evict = math.exp(- decay_rate_evict * step_evict)
        cur_values = environment.curValues
        if step_evict % 1000 == 0:
            print('epsilon = ' + str(eps_evict))
        
        # GET ACTION
        rnd_eps = random.random()
        if rnd_eps < eps_add or step_evict < no_training_steps:
            rnd = random.random()
            if rnd < 0.5:
                action = 0
            else:
                action = 1
        else:
            cur_values_ = np.reshape(cur_values, (1, 7))
            action = np.argmax(model_evict.predict(cur_values_))
        if debug == True:
            print('FREEING-------------------------------------------------------------------------------------')
            print('CURVALUES')
        if debug == True:
            cur_values_ = np.reshape(cur_values, (1, 7))
            print(cur_values_)
            print()

        # IF ADDING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        curFilename, curSize = environment.get_filename_and_size_of_current_cache_file()
        if action == 1:
            #del environment._cache._cached_files[curFilename]
            environment._cache._cached_files.remove(curFilename)
            environment._cache._size -= curSize
            environment._cache._deleted_data += curSize
        
        if step_evict % 1000 == 0:
            print('Freeing memory ' + str(environment._cached_files_index) + '/' + str(len(environment._cache._cached_files_keys)) + 
                    '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) + '%  - action: ' + str(action))
        
        # IF EVICTING IS NOT OVER, GET NEXT VALUES AND PREPARE ACTION TO BE REWARDED, GIVING EVENTUAL REWARD
        if environment._cached_files_index + 1 != len(environment._cache._cached_files_keys) and environment._cache.capacity >= low_watermark:
            next_values = environment.get_next_file_in_cache_values()
            environment.update_windows_getting_eventual_rewards(
                adding_or_evicting, curFilename, cur_values, next_values, action)
        
        # REMOVE THE FIRST DUMMY ELEMENT IN MEMORY
        if step_evict == 1000:
            environment.evict_memory_vector = np.delete(environment.evict_memory_vector, 0, 0)

        # KEEP MEMORY LENGTH LESS THAN LIMIT
        while(environment.evict_memory_vector.shape[0] > memory):
            environment.evict_memory_vector = np.delete(environment.evict_memory_vector, 0, 0)

        # TRAIN NETWORK
        if step_evict > no_training_steps + 5000:
            #print('TRAINING_EVICTION')

            #print('PREDICTION - EVICTING')
            #print(model_evict.predict(cur_values_))
            #print()
            batch = environment.evict_memory_vector[np.random.randint(0, environment.evict_memory_vector.shape[0], BATCH_SIZE), :]
            train_cur_vals ,train_actions, train_rewards, train_next_vals = np.split(batch, [7,8,9] , axis = 1)
            target = model_evict.predict_on_batch(train_cur_vals)

            if debug == True:
                print('BATCH')
                print(batch)
                print()
                print('PREDICT ON BATCH')
                print(target)
                print()
            if use_target_model == False:
                predictions = model_evict.predict_on_batch(train_next_vals)
                for i in range(0,BATCH_SIZE):  
                    action_ = int(train_actions[i])
                    target[i,action_] = train_rewards[i] + gamma * mellowmax(mm_omega, predictions[i])  
            else:
                predictions = target_model_add.predict_on_batch(train_next_vals)
                for i in range(0,BATCH_SIZE):  
                    action_ = int(train_actions[i])
                    target[i,action_] = train_rewards[i] + gamma * max(predictions[i])  
            
            model_evict.train_on_batch(train_cur_vals, target)
            if debug == True:
                print('TARGET')
                print(target)
                print()


    #### STOP ADDING ################################################################################################################################
    if adding_or_evicting == 0 and environment._cache.capacity > environment._cache._h_watermark:
        adding_or_evicting = 1 
        #addition_counter += 1
        #print('STOP ADDING')
        #environment._cache._cached_files_keys = list(environment._cache._cached_files.keys())
        environment._cache._cached_files_keys = list(environment._cache._cached_files)
        random.shuffle(environment._cache._cached_files_keys)
        environment._cached_files_index = -1
        cur_values = environment.get_next_file_in_cache_values()
        
    ### STOP EVICTING ################################################################################################################################
    if adding_or_evicting == 1 and (environment._cached_files_index + 1 == len(environment._cache._cached_files_keys) or environment._cache.capacity < low_watermark):
        with open(out_directory + '/occupancy.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow([environment._cache.capacity])
        adding_or_evicting = 0
        #eviction_counter += 1
        cur_values = environment.get_next_request_values()

    ### END ####################################################################################################################################
    if environment.curDay == environment._idx_end:
        end = True
        model_add.save_weights(out_directory + args.out_add_weights)
        model_evict.save_weights(out_directory + args.out_evict_weights)



