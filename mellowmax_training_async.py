from keras.models import Sequential
from keras.layers import Input, Dense, Activation, Flatten
import tensorflow as tf
import numpy as np
import pandas as pd
import math
import random
import cache_env_async
import csv
import array


###### PARAMETERS ####################################################################################################################################
BATCH_SIZE = 32
_startMonth = 1
_endMonth = 2
memory = 30000
nb_actions = 2
observation_shape = (7,)
decay_rate = 0.00001
no_training_steps = 30000
eps_add = 1.0
eps_evict = 1.0
eps_add_min = 0.1
eps_evict_min = 0.1
gamma = 0.50
seed_ = 2019

####### EXTRA FUNCTION DEFINITIONS ##################################################################################################################
def mellowmax(omega, x):
    N = len(x)
    sum_ = 0
    for i in range(0,N):
        sum_ += math.exp(omega * x[i])
    return math.log(sum_/N)/omega
 
def huber_loss(y_true, y_pred, clip_delta=1.0):
  error = y_true - y_pred
  cond  = tf.keras.backend.abs(error) < clip_delta

  squared_loss = 0.5 * tf.keras.backend.square(error)
  linear_loss  = clip_delta * (tf.keras.backend.abs(error) - 0.5 * clip_delta)

  return tf.where(cond, squared_loss, linear_loss)

def huber_loss_mean(y_true, y_pred, clip_delta=1.0):
  return tf.keras.backend.mean(huber_loss(y_true, y_pred, clip_delta))

############## DEFINE ADD AND EVICT MODELS #########################################################################################################
model_evict = Sequential()
model_evict.add(Dense(16, input_dim =7))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(32))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(64))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(32))
model_evict.add(Activation('sigmoid'))
model_evict.add(Dense(nb_actions))
#model_evict.add(Activation('linear'))
model_evict.add(Activation('sigmoid'))
print(model_evict.summary())
model_evict.compile(optimizer = 'adam', loss = huber_loss_mean)
        
model_add = Sequential()
model_add.add(Dense(16,input_dim = 7))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(32))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(64))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(32))
model_add.add(Activation('sigmoid'))
model_add.add(Dense(nb_actions))
#model_add.add(Activation('linear'))
model_add.add(Activation('sigmoid'))
print(model_add.summary())
model_add.compile(optimizer = 'adam', loss = huber_loss_mean)

###### START LOOPING ############################################################################################################################
environment = cache_env_async.env(_startMonth, _endMonth, directory = '/home/ubuntu/source2018_numeric_it_with_avro_order')
random.seed(seed_)
adding_or_evicting = 0
step_add = 0
step_evict = 0
add_memory_vector = []
evict_memory_vector = []
step_add = 0
step_evict = 0
addition_counter = 0
eviction_counter = 0


with open('results/results_ok_stats_{}/occupancy.csv'.format(str(environment.time_span)), 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['occupancy'])
#next_values = np.zeros(7)

end = False
while end == False:
    print()
    if (environment.curDay+1)%7 == 0:
        environment.purge()

    print('len _filesLRU = ' + str(len(environment._cache._filesLRU)))
    print('len _filesLRUkeys = ' + str(len(environment._cache._filesLRUkeys)))
    print('_filesLRU index = ' + str(environment._filesLRU_index))

############ ADDING ###########################################################################################################
    if adding_or_evicting == 0:
        
        #UPDATE STUFF
        step_add += 1
        if eps_add > eps_add_min:
            eps_add = math.exp(- decay_rate * step_add)
        cur_values = environment.curValues
        print('epsilon = ' + str(eps_add))


        #GET ACTION
        rnd_eps = random.random()
        if rnd_eps < eps_add or step_add < BATCH_SIZE :
            rnd = random.random()
            if rnd < 0.5:
                action = 0
            else:
                action = 1
        else:
            cur_values_ = np.reshape(cur_values, (1,7))
            action = np.argmax(model_add.predict(cur_values_))
        
        with open('results/results_ok_stats_{}/addition_choices_{}.csv'.format(str(environment.time_span), addition_counter), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([action])
        
        #UPDATE STUFF, GET REWARD AND NEXT STATE AND PUT INTO MEMORY
        environment.add_request(action)
        curFilename, curSize = environment.get_filename_and_size_of_current_request()
        
        if environment._cache.capacity > environment._cache._h_watermark:
            environment.clear_window()
        
        if environment._cache.capacity <= environment._cache._h_watermark:
            next_values = environment.get_next_request_values()
            environment.update_windows_getting_eventual_rewards(curFilename, cur_values, next_values, action, add_memory_vector, evict_memory_vector)
        
        if len(add_memory_vector) > memory:
            del add_memory_vector[0]

        #TRAIN NETWORK
        if step_add > no_training_steps:
            randomlist = random.sample(range(1, len(add_memory_vector)), BATCH_SIZE)
            state_action_vector = []
            train_cur_vals = []
            train_actions = []
            train_rewards = []
            train_next_vals = []
            for i in randomlist:
                state_action_vector.append(add_memory_vector[i])

                train_cur_vals.append(add_memory_vector[i][0])
                train_actions.append(add_memory_vector[i][1])
                train_rewards.append(add_memory_vector[i][2])
                train_next_vals.append(add_memory_vector[i][3])
            
            train_cur_vals = np.array(train_cur_vals)
            train_actions = np.array(train_actions)
            train_rewards = np.array(train_rewards)
            train_next_vals = np.array(train_next_vals)

            state_action_vector = np.array(state_action_vector)
            target = model_add.predict_on_batch(train_cur_vals)
            predictions = model_add.predict_on_batch(train_next_vals)
            for i in range(0,len(state_action_vector)):
                target[i,train_actions[i]] = train_rewards[i] + gamma * mellowmax(1, predictions[i])   
            #TRAIN
            model_add.train_on_batch(train_cur_vals, target)

            #next_values = cur_values
        
####### EVICTING #############################################################################################################
    elif adding_or_evicting == 1: 
        
        #UPDATE STUFF
        step_evict += 1
        if eps_evict > eps_evict_min:
            eps_evict = math.exp(- decay_rate * step_evict)
        cur_values = environment.curValues
        print('epsilon = ' + str(eps_evict))
        
        #GET ACTION
        rnd_eps = random.random()
        if rnd_eps < eps_add or step_evict < BATCH_SIZE :
            rnd = random.random()
            if rnd < 0.5:
                action = 0
            else:
                action = 1
        else:
            cur_values_ = np.reshape(cur_values, (1,7))
            action = np.argmax(model_evict.predict(cur_values_))
        with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(environment.time_span), eviction_counter), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([action])
        
        #UPDATE STUFF, GET REWARD AND NEXT STATE AND PUT INTO MEMORY
        curFilename, curSize = environment.get_filename_and_size_of_current_cache_file()
        if action == 1:
            del environment._cache._filesLRU[curFilename]
            environment._cache._size -= curSize
            environment._cache._deleted_data += curSize
        print('Freeing memory ' + str(environment._filesLRU_index) + '/' + str(len(environment._cache._filesLRUkeys)) + '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) + '%  - action: ' + str(action) + ' ' + str(environment._cache._get_mean_size(environment.curRequest, environment.curDay) * len(environment._cache._filesLRU) / environment._cache._max_size))
        with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(environment.time_span), eviction_counter), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([action])
        
        if environment._filesLRU_index + 1 != len(environment._cache._filesLRU):
            next_values = environment.get_next_file_in_cache_values()
            environment.update_windows_getting_eventual_rewards(curFilename, cur_values, next_values, action, add_memory_vector, evict_memory_vector)
        
        if len(evict_memory_vector) > memory:
            del evict_memory_vector[0]

        #TRAIN NETWORK
        if step_evict > no_training_steps:
            randomlist = random.sample(range(1, len(evict_memory_vector)), BATCH_SIZE)
            state_action_vector = []
            train_cur_vals = []
            train_actions = []
            train_rewards = []
            train_next_vals = []
            for i in randomlist:
                state_action_vector.append(evict_memory_vector[i])

                train_cur_vals.append(evict_memory_vector[i][0])
                train_actions.append(evict_memory_vector[i][1])
                train_rewards.append(evict_memory_vector[i][2])
                train_next_vals.append(evict_memory_vector[i][3])
            
            train_cur_vals = np.array(train_cur_vals)
            train_actions = np.array(train_actions)
            train_rewards = np.array(train_rewards)
            train_next_vals = np.array(train_next_vals)
            
            state_action_vector = np.array(state_action_vector)
            
            #GET TARGET
            target = model_evict.predict_on_batch(train_cur_vals)
            predictions = model_evict.predict_on_batch(train_next_vals)
            for i in range(0,len(state_action_vector)):  
                target[i,train_actions[i]] = train_rewards[i] + gamma * mellowmax(1, predictions[i])   
                

            #TRAIN
            model_evict.train_on_batch(train_cur_vals, target)

            #cur_values = next_values

######## STOP ADDING ################################################################################################################################
    if adding_or_evicting == 0 and environment._cache.capacity > environment._cache._h_watermark:
        adding_or_evicting = 1 
        addition_counter += 1
        environment.start_of_a_new_evicting = True
        environment._cache._filesLRUkeys = list(environment._cache._filesLRU.keys())
        environment._filesLRU_index = -1
        cur_values = environment.get_next_file_in_cache_values()
        with open('results/results_ok_stats_{}/addition_choices_{}.csv'.format(str(environment.time_span), addition_counter), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['addition choice'])
        
####### STOP EVICTING ################################################################################################################################
    if adding_or_evicting == 1 and environment._filesLRU_index + 1 == len(environment._cache._filesLRU):
        with open('results/results_ok_stats_{}/occupancy.csv'.format(str(environment.time_span)), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([environment._cache.capacity])
        adding_or_evicting = 0
        eviction_counter += 1
        with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(environment.time_span), eviction_counter), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['eviction choice'])
        cur_values = environment.get_this_request_values()



