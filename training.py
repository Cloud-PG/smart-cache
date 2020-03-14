from keras.models import Sequential
from keras.layers import Input, Dense, Activation, Flatten
import tensorflow as tf
import numpy as np
import pandas as pd
import math
import random
import cache_env
import csv
import array

BATCH_SIZE = 32
_startMonth = 1
_endMonth = 2
memory = 30000
nb_actions = 2
observation_shape = (7,)
decay_rate = 0.00001
no_training_steps = 30000

def mellowmax(omega, x):
    N = len(x)
    sum_ = 0
    for i in range(0,N):
        sum_ += math.exp(omega * x[i])
    return math.log(sum_/N)/omega
 
#HUBER LOSS 
def huber_loss(y_true, y_pred, clip_delta=1.0):
  error = y_true - y_pred
  cond  = tf.keras.backend.abs(error) < clip_delta

  squared_loss = 0.5 * tf.keras.backend.square(error)
  linear_loss  = clip_delta * (tf.keras.backend.abs(error) - 0.5 * clip_delta)

  return tf.where(cond, squared_loss, linear_loss)

'''
 ' Same as above but returns the mean loss.
'''
def huber_loss_mean(y_true, y_pred, clip_delta=1.0):
  return tf.keras.backend.mean(huber_loss(y_true, y_pred, clip_delta))

#DEFINE ADD AND EVICT MODELS AND THEIR TARGET MODELS

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
model_evict.add(Activation('linear'))
print(model_evict.summary())
model_evict.compile(optimizer = 'adam', loss = huber_loss_mean)
        
model_evict_target = Sequential()
model_evict_target.add(Dense(16,input_dim = 7))
model_evict_target.add(Activation('sigmoid'))
model_evict_target.add(Dense(32))
model_evict_target.add(Activation('sigmoid'))
model_evict_target.add(Dense(64))
model_evict_target.add(Activation('sigmoid'))
model_evict_target.add(Dense(32))
model_evict_target.add(Activation('sigmoid'))
model_evict_target.add(Dense(nb_actions))
model_evict_target.add(Activation('linear'))
print(model_evict_target.summary())
model_evict_target.compile(optimizer = 'adam', loss = huber_loss_mean)

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
model_add.add(Activation('linear'))
print(model_add.summary())
model_add.compile(optimizer = 'adam', loss = huber_loss_mean)
        
model_add_target = Sequential()
model_add_target.add(Dense(16, input_dim = 7))
model_add_target.add(Activation('sigmoid'))
model_add_target.add(Dense(32))
model_add_target.add(Activation('sigmoid'))
model_add_target.add(Dense(64))
model_add_target.add(Activation('sigmoid'))
model_add_target.add(Dense(32))
model_add_target.add(Activation('sigmoid'))
model_add_target.add(Dense(nb_actions))
model_add_target.add(Activation('linear'))
print(model_add_target.summary())
model_add_target.compile(optimizer = 'adam', loss = huber_loss_mean)

eps_add = 1.0
eps_evict = 1.0
eps_add_min = 0.1
eps_evict_min = 0.1
adding_or_evicting = 0
step_add = 0
step_evict = 0
gamma = 0.99
add_memory_vector = []
evict_memory_vector = []

random.seed(2019)

environment = cache_env.env(start_month = 1, end_month = 2)


#START LOOPING ON REQUESTS AND EVICTING

step_add = 0
step_evict = 0
end = False

addition_counter = 0
eviction_counter = 0

next_values = np.zeros(7)

while end == False:
        if (environment.curDay+1)%7 == 0:
            environment.purge()

        #IF ADDING, GET NEXT REQUEST (ADD IT OR NOT) AND GET REWARD
        if adding_or_evicting == 0:
            
            #UPDATE STUFF
            step_add += 1
            if eps_add > eps_add_min:
                eps_add = math.exp(- decay_rate * step_add)
            cur_values = environment.curValues
            print(eps_add)


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
            environment.update_window(adding_or_evicting, action, curFilename, curSize)
            reward = environment.get_reward(adding_or_evicting)
            if len(add_memory_vector) > memory:
                del add_memory_vector[0]
            next_values = environment.get_next_request_values()
            add_memory_vector.append(np.array([cur_values, action, reward, next_values]))

            #TRAIN NETWORK
            if step_add > no_training_steps:
            #if step_add >
                #print(np.array(add_memory_vector).shape)
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

                #state_action_vector = np.random.choice(np.array(add_memory_vector), BATCH_SIZE, replace = False)
                state_action_vector = np.array(state_action_vector)
                #print(state_action_vector[:,3])
                #input_ = state_action_vector[:,3]
                #print(state_action_vector[,3].shape)
                #input_ = np.reshape(state_action_vector[:,3], (32, 7))
                #input_ = np.array([[1, 2, 3, 4, 5 , 6, 7], [1, 2, 3, 4, 5, 6, 7]])
                #np.transpose(input_)
                #input_ = state_action_vector[0,3]
               # print(input_.shape)
                
                #print(train_cur_vals)
                #print(train_next_vals)
                #GET TARGET
                #print(train_cur_vals)
                target = model_add.predict_on_batch(train_cur_vals)
                if step_add == no_training_steps + 1 or step_add == no_training_steps + 2:
                    print(target)
                    print(model_add_target.predict_on_batch(train_cur_vals))
                predictions = model_add_target.predict_on_batch(train_next_vals)
                #print(predictions)
                #print(target.shape)
                for i in range(0,len(state_action_vector)):
                    #target[i,state_action_vector[i,2]] = state_action_vector[i] + gamma * max(predictions[i])   
                    target[i,train_actions[i]] = train_rewards[i] + gamma * max(predictions[i])   
                #print(target)
                #TRAIN
                model_add.train_on_batch(train_cur_vals, target)
            
        #IF EVICTING, GET NEXT FILE IN CACHE (EVICT IT OR NOT) AND GET REWARD
        elif adding_or_evicting == 1:
            
            #UPDATE STUFF
            step_evict += 1
            if eps_evict > eps_evict_min:
                eps_evict = math.exp(- decay_rate * step_evict)
            cur_values = environment.curValues
            print(eps_evict)
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
            
            with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(environment.time_span), addition_counter), 'a') as file:
                writer = csv.writer(file)
                writer.writerow([action])
            
            #UPDATE STUFF, GET REWARD AND NEXT STATE AND PUT INTO MEMORY
            if action == 1:
                del environment._cache._filesLRU[curFilename]
                environment._cache._size -= curSize
                environment._cache._deleted_data += curSize
            print('Freeing memory ' + str(environment._filesLRU_index) + '/' + str(len(environment._cache._filesLRUkeys)) + '  -  Occupancy: ' + str(round(environment._cache.capacity,2)) + '%  - action: ' + str(action) + ' ' + str(environment._cache._get_mean_size(environment.curRequest, environment.curDay) * len(environment._cache._filesLRU) / environment._cache._max_size))
            with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(environment.time_span), eviction_counter), 'a') as file:
                writer = csv.writer(file)
                writer.writerow([action])
            environment.update_window(adding_or_evicting, action, curFilename, curSize)
            reward = environment.get_reward(adding_or_evicting)
            if len(evict_memory_vector) > memory:
                del evict_memory_vector[0]
            next_values = environment.get_next_file_in_cache_values()
            evict_memory_vector.append(np.array([cur_values, action, reward, next_values]))
            
            #TRAIN NETWORK
            if step_evict > no_training_steps:
                #print(np.array(add_memory_vector).shape)
                randomlist = random.sample(range(1, len(evict_memory_vector)), BATCH_SIZE)
                state_action_vector = []
                train_cur_vals = []
                train_actions = []
                train_rewards = []
                train_next_vals = []
                for i in randomlist:
                    state_action_vector.append(evict_memory_vector[i])

                    train_cur_vals.append(add_memory_vector[i][0])
                    train_actions.append(add_memory_vector[i][1])
                    train_rewards.append(add_memory_vector[i][2])
                    train_next_vals.append(add_memory_vector[i][3])
                
                train_cur_vals = np.array(train_cur_vals)
                train_actions = np.array(train_actions)
                train_rewards = np.array(train_rewards)
                train_next_vals = np.array(train_next_vals)
                
                state_action_vector = np.array(state_action_vector)
                
                #GET TARGET
                target = model_evict.predict_on_batch(train_cur_vals)
                predictions = model_evict_target.predict_on_batch(train_next_vals)
                for i in range(0,len(state_action_vector)):  
                    target[i,train_actions[i]] = train_rewards[i] + gamma * max(predictions[i])   
                print(target)
                #TRAIN
                model_evict.train_on_batch(train_cur_vals, target)

        #STOP ADDING

        if adding_or_evicting == 0 and environment._cache.capacity > environment._cache._h_watermark:
            adding_or_evicting = 1 
            #environment.update_time_span_filenames_list()
            environment._filesLRUkeys = list(environment._cache._filesLRU.keys())
            environment._filesLRU_index = -1
            cur_values = environment.get_next_file_in_cache_values()
            addition_counter += 1
            with open('results/results_ok_stats_{}/addition_choices_{}.csv'.format(str(environment.time_span), addition_counter), 'w') as file:
                writer = csv.writer(file)
                writer.writerow(['addition choice'])
            
        #STOP EVICTING
        if adding_or_evicting == 1 and environment._filesLRU_index + 1 == len(environment._cache._filesLRUkeys) :
            with open('results/results_ok_stats_{}/occupancy.csv'.format(str(environment.time_span)), 'a') as file:
                writer = csv.writer(file)
                writer.writerow([environment._cache.capacity])
            adding_or_evicting = 0
            eviction_counter += 1
            with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(environment.time_span), eviction_counter), 'w') as file:
                writer = csv.writer(file)
                writer.writerow(['eviction choice'])

        
           

       
'''
    if adding_or_evicting == 0:
        
        #UPDATE STUFF
        step_add += 1
        eps_addic = math.exp(decay_rate * step_add)
        cur_values = environment.get_next_request()
        
        #GET ACTION
        rnd_eps = random.random()
        if rnd_eps < eps_addic:
            rnd = random.random()
            if rnd < 0.5:
                action = 0
            else:
                action = 1
        else:
            action = np.argmax(model_add.predict(cur_values))
        
        with open('results/results_ok_stats_{}/addition_choices_{}.csv'.format(str(environment.time_span), addition_counter), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([action])
        
        #GET REWARD AND NEXT STATE AND PUT INTO MEMORY
        reward = environment.get_reward(adding_or_evicting)
        environment.add_request(action)
        if len(add_memory_vector) > memory:
            del add_memory_vector[0]
        next_values = environment.get_next_request_values()
        add_memory_vector.append(np.array([cur_values, action, reward, next_values]))

        #TRAIN NETWORK
        state_action_vector = np.random.choice(add_memory_vector, BATCH_SIZE, replace = False)
        target = model_add.predict_on_batch(state_action_vector[:,0])
        target[action] = state_action_vector[2] + gamma * max(model_add_target.predict_on_batch(state_action_vector[:,3]))   
        model_add.train_on_batch(state_action_vector[:,0], target)

    if adding_or_evicting == 1:
        
        #UPDATE STUFF
        step_evict += 1
        eps_evict = math.exp(decay_rate * step_evict)
        cur_values = environment.get_filename_and_size_of_current_cache_file
        
        #GET ACTION
        rnd_eps = random.random()
        if rnd_eps < eps_addic:
            rnd = random.random()
            if rnd < 0.5:
                action = 0
            else:
                action = 1
        else:
            action = np.argmax(model_evict.predict(cur_values))
        
        with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(environment.time_span), addition_counter), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([action])
        
        #GET REWARD AND NEXT STATE AND PUT INTO MEMORY
        reward = environment.get_reward(adding_or_evicting)
        if len(evict_memory_vector) > memory:
            del evict_memory_vector[0]
        next_values = environment.get_next_file_in_cache_values()
        evict_memory_vector.append(np.array([cur_values, action, reward, next_values]))
        
        #TRAIN NETWORK
        state_action_vector = np.random.choice(evict_memory_vector, BATCH_SIZE, replace = False)
        target = model_evict.predict_on_batch(state_action_vector[:,0])
        target[action] = state_action_vector[2] + gamma * max(model_evict_target.predict_on_batch(state_action_vector[:,3]))   
        model_evict.train_on_batch(state_action_vector[:,0], target)

    #IF NECESSARY STOP ADDING AND SWITCH TO EVICTION 
    if adding_or_evicting == 0 and environment._cache.capacity > environment._cache._h_watermark:
        adding_or_evicting = 1 
        _filesLRUkeys = list(environment._cache._filesLRU.keys())
        _filesLRU_index = -1
        next_file_values = environment.get_next_file_in_cache_values()
        addition_counter += 1
        with open('results/results_ok_stats_{}/addition_choices_{}.csv'.format(str(environment.time_span), addition_counter), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['addition choice'])
        
    #IF NECESSARY STOP EVICTING AND SWITCH TO ADDITION
    if adding_or_evicting == 1 and _filesLRU_index + 1 == len(_filesLRUkeys) :
        with open('results/results_ok_stats_{}/occupancy.csv'.format(str(environment.time_span)), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([environment._cache.capacity])
        adding_or_evicting = 0
        eviction_counter += 1
        with open('results/results_ok_stats_{}/eviction_choices_{}.csv'.format(str(environment.time_span), eviction_counter), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['eviction choice'])

    #UPDATE WINDOW AND 
    environment.update_time_span_filenames_list()
    if adding_or_evicting == 0:
        next_file_values = environment.get_next_request_values()
    if adding_or_evicting == 1:
        next_file_values = environment.get_next_file_in_cache_values()
'''