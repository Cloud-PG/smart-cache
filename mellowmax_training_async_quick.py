from keras.models import Sequential
from keras.layers import Input, Dense, Activation, Flatten
import tensorflow as tf
import numpy as np
import pandas as pd
import math
import random
import cache_env_async_quick
import csv
import array
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--data', type=str, default='/home/ubuntu/source2018_numeric_it_with_avro_order')
parser.add_argument('--start_month', type=int, default=1)
parser.add_argument('--end_month', type=int, default=2)
parser.add_argument('--batch_size', type=int, default=32)
parser.add_argument('--out_dir', type=str, default='results/results_ok_stats_async_quick')
parser.add_argument('--out_name', type=str, default='dQL_add_evic.csv')
parser.add_argument('--lr', type=float, default=0.01, help='learning rate')
parser.add_argument('--memory', type=int, default=30000)
parser.add_argument('--decay_rate', type=float, default=0.00001)
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
parser.add_argument('--out_evict_weights', type = str, default = 'weights_evict.h5')

args = parser.parse_args()

BATCH_SIZE = args.batch_size
data_directory = args.data
_startMonth = args.start_month
_endMonth = args.end_month
memory = args.memory
decay_rate = args.decay_rate
no_training_steps = args.warm_up_steps
eps_add = args.eps_add_max
eps_evict = args.eps_evict_max
eps_add_min = args.eps_add_min
eps_evict_min = args.eps_evict_min
gamma = args.gamma
mm_omega = args.mm_omega

out_directory = args.out_dir
out_name = args.out_name

if not os.path.isdir(out_directory):
    os.makedirs(out_directory)

###### FIXED PARAMETERS ####################################################################################################################################

nb_actions = 2
observation_shape = (7,)
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
model_evict.add(Activation('sigmoid'))
print(model_evict.summary())
model_evict.compile(optimizer = 'adam', loss = huber_loss_mean)

if args.load_evict_weights_from_file is None == False:
     model_evict.load_weights(args.load_evict_weights_from_file)
        
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
model_add.add(Activation('sigmoid'))
print(model_add.summary())
model_add.compile(optimizer = 'adam', loss = huber_loss_mean)

if args.load_add_weights_from_file is None == False:
     model_add.load_weights(args.load_add_weights_from_file)

###### START LOOPING ############################################################################################################################
environment = cache_env_async_quick.env(_startMonth, _endMonth, data_directory, out_directory, out_name)
random.seed(seed_)
adding_or_evicting = 0
step_add = 0
step_evict = 0
step_add = 0
step_evict = 0
addition_counter = 0
eviction_counter = 0


with open(out_directory + '/occupancy.csv', 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['occupancy'])

end = False

while end == False:
    if (step_add % 1000 == 0 or step_evict % 1000 == 0) and step_evict != 0:
        print()
    if (environment.curDay+1)%7 == 0:
        environment.purge()

    ######## ADDING ###########################################################################################################
    if adding_or_evicting == 0:
        
        #UPDATE STUFF
        step_add += 1
        if eps_add > eps_add_min:
            eps_add = math.exp(- decay_rate * step_add)
        cur_values = environment.curValues
        if step_add%1000 == 0:
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
            #if cur_values[1] == 1:
            #    print('recency = ' + str(cur_values[2]))
            action = np.argmax(model_add.predict(cur_values_))
        
        #UPDATE STUFF, GET REWARD AND NEXT STATE AND PUT INTO MEMORY
        environment.add_request(action)
        curFilename, curSize = environment.get_filename_and_size_of_current_request()

        if step_add%1000 == 0:
            print('Request: ' + str(environment.curRequest) + ' / ' + str(environment.df_length) + '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) 
                + '%  -  ' + 'Hit rate: ' + str(round(environment._cache._hit/(environment._cache._hit + environment._cache._miss)*100,2)) +'%' + ' ACTION: ' +  str(action))
        
        if environment._cache.capacity > environment._cache._h_watermark:
            environment.clear_window()
        
        if environment._cache.capacity <= environment._cache._h_watermark:
            next_values = environment.get_next_request_values()
            environment.update_windows_getting_eventual_rewards(adding_or_evicting, curFilename, cur_values, next_values, action)
        
        #print(environment.add_memory_vector.shape, end = '\r')
        #if step_add == 1:
        #    add_memory_vector = np.delete(add_memory_vector, 0)

        if environment.add_memory_vector.shape[0] > memory:
            environment.add_memory_vector = np.delete(environment.add_memory_vector, 0)

        #TRAIN NETWORK
        if step_add > no_training_steps:
            
            batch = environment.add_memory_vector[np.random.randint(0, environment.add_memory_vector.shape[0], BATCH_SIZE), :]
            train_cur_vals ,train_actions, train_rewards, train_next_vals = np.split(batch, [7,8,9] , axis = 1)
            target = model_add.predict_on_batch(train_cur_vals)
            predictions = model_add.predict_on_batch(train_next_vals)

            for i in range(0,BATCH_SIZE):
                action_ = int(train_actions[i])
                #print(action_)
                target[i,action_] = train_rewards[i] + gamma * mellowmax(mm_omega, predictions[i])   
            #TRAIN
            model_add.train_on_batch(train_cur_vals, target)
        
    ### EVICTING #############################################################################################################
    elif adding_or_evicting == 1: 
        
        #UPDATE STUFF
        step_evict += 1
        if eps_evict > eps_evict_min:
            eps_evict = math.exp(- decay_rate * step_evict)
        cur_values = environment.curValues
        if step_evict%1000 == 0:
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
        
        #UPDATE STUFF, GET REWARD AND NEXT STATE AND PUT INTO MEMORY
        curFilename, curSize = environment.get_filename_and_size_of_current_cache_file()
        if action == 1:
            del environment._cache._filesLRU[curFilename]
            environment._cache._size -= curSize
            environment._cache._deleted_data += curSize
        
        if step_evict%1000 == 0:
            print('Freeing memory ' + str(environment._filesLRU_index) + '/' + str(len(environment._cache._filesLRUkeys)) + 
                                        '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) + '%  - action: ' + str(action))
                
        if environment._filesLRU_index + 1 != len(environment._cache._filesLRU):
            next_values = environment.get_next_file_in_cache_values()
            environment.update_windows_getting_eventual_rewards(adding_or_evicting, curFilename, cur_values, next_values, action)
        
        #if step_add == 1:
        #    evict_memory_vector = np.delete(evict_memory_vector, 0)

        if environment.evict_memory_vector.shape[0] > memory:
            environment.evict_memory_vector = np.delete(environment.evict_memory_vector, 0)

        #TRAIN NETWORK
        if step_evict > no_training_steps:
            
            batch = environment.evict_memory_vector[np.random.randint(0, environment.evict_memory_vector.shape[0], BATCH_SIZE), :]
            train_cur_vals ,train_actions, train_rewards, train_next_vals = np.split(batch, [7,8,9] , axis = 1)

            #GET TARGET
            target = model_evict.predict_on_batch(train_cur_vals)
            predictions = model_evict.predict_on_batch(train_next_vals)
            for i in range(0,BATCH_SIZE):  
                action_ = int(train_actions[i])
                #print(action_)
                target[i,train_actions[i]] = train_rewards[i] + gamma * mellowmax(mm_omega, predictions[i])   
                
            #TRAIN
            model_evict.train_on_batch(train_cur_vals, target)

    #### STOP ADDING ################################################################################################################################
    if adding_or_evicting == 0 and environment._cache.capacity > environment._cache._h_watermark:
        adding_or_evicting = 1 
        addition_counter += 1
        environment.start_of_a_new_evicting = True
        environment._cache._filesLRUkeys = list(environment._cache._filesLRU.keys())
        environment._filesLRU_index = -1
        cur_values = environment.get_next_file_in_cache_values()
        
    ### STOP EVICTING ################################################################################################################################
    if adding_or_evicting == 1 and environment._filesLRU_index + 1 == len(environment._cache._filesLRU):
        with open(out_directory + '/occupancy.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow([environment._cache.capacity])
        adding_or_evicting = 0
        eviction_counter += 1
        cur_values = environment.get_next_request_values()

    ### END ####################################################################################################################################
    if environment.curDay == environment._idx_end:
        end = True
        model_add.save_weights(args.out_add_weights)
        model_evict.save_weights(args.out_evict_weights)



