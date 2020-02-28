import numpy as np
import gym
#import custom_gym
import tensorflow as tf 
import pandas as pd
import gzip

from keras.models import Sequential
from keras.layers import Dense, Activation, Flatten
from keras.optimizers import Adam

from rl.agents.dqn import DQNAgent
from rl.policy import EpsGreedyQPolicy
from rl.memory import SequentialMemory
from rl.policy import ExponentialAnnealedPolicy 

tf.compat.v1.disable_eager_execution()

ENV_NAME = 'CacheEnv-v0'

#with gzip.open("results_2018-01-01.csv.gz") as f:
#    df_=pd.read_csv(f)
#df_['region']=df_.SiteName.str.split("_",expand=True)[1]
#df_['Size']=df_['Size']/1.049e+6
#df_=df_[df_['region']=='IT']
#df_=df_.reset_index()

#env = gym.make(ENV_NAME,df = df_)

env = gym.make(ENV_NAME, one_hot = False, start_month = 5, end_month = 9)

nb_actions = env.action_space.n
print(nb_actions)
print(env.observation_space.shape)

model = Sequential()
model.add(Flatten(input_shape=(1,) + env.observation_space.shape))
model.add(Dense(16))
model.add(Activation('relu'))
model.add(Dense(32))
model.add(Activation('relu'))
model.add(Dense(64))
model.add(Activation('relu'))
model.add(Dense(32))
model.add(Activation('relu'))
model.add(Dense(nb_actions))
model.add(Activation('linear'))
print(model.summary())


'''
policy = EpsGreedyQPolicy()
test_policy = EpsGreedyQPolicy(eps = 0.1)
memory = SequentialMemory(limit=50000, window_length=1)

#dqn = DQNAgent(model=model, nb_actions=nb_actions, memory=memory, nb_steps_warmup=10, target_model_update=1e-2, policy=policy)
dqn = DQNAgent(model=model, nb_actions=nb_actions, memory=memory, nb_steps_warmup=1000, policy=policy)
#dqn.compile(Adam(lr=1e-3), metrics=['mae'])
dqn.compile(Adam(lr=1e-1), metrics=['mae'])

dqn.fit(env, nb_steps=3000000, visualize=False, verbose=2)

#dqn.test(env, nb_episodes=1, visualize=False)
'''

#monthly steps
monthly_requests_list = [1469830, 1036928,  795906,  956234,  807966, 1217021, 1914421, 1085366, 871269, 1355477,  788335,  654657]
sum_12 = monthly_requests_list[0] + monthly_requests_list[1]
sum_312 = monthly_requests_list[2] + monthly_requests_list[3] + monthly_requests_list[4] + monthly_requests_list[5] + monthly_requests_list[6] + monthly_requests_list[7] + monthly_requests_list[8] + monthly_requests_list[9] + monthly_requests_list[10] + monthly_requests_list[11]

#filepath of saved weights
filepath = './weights.hdf5'

#policies for training and testing
train1_policy = ExponentialAnnealedPolicy(decay_rate = None, attr = 'eps', value_max = 1, value_min = 0.1, inner_policy = EpsGreedyQPolicy(), value_test = 0.5, nb_steps = monthly_requests_list[0])
train2_policy = ExponentialAnnealedPolicy(decay_rate = None, attr = 'eps', value_max = 1, value_min = 0.1, inner_policy = EpsGreedyQPolicy(), value_test = 0.5, nb_steps = monthly_requests_list[1])
train3_policy = ExponentialAnnealedPolicy(decay_rate = None, attr = 'eps', value_max = 1, value_min = 0.1, inner_policy = EpsGreedyQPolicy(), value_test = 0.5, nb_steps = sum_12)
train4_policy = ExponentialAnnealedPolicy(decay_rate = None, attr = 'eps', value_max = 1, value_min = 0.1, inner_policy = EpsGreedyQPolicy(), value_test = 0.5, nb_steps = sum_312)
test_policy = EpsGreedyQPolicy(eps = 0.1)

#memory?
memory = SequentialMemory(limit=50000000, window_length=1)

#create the model for the first time
dqn = DQNAgent(model=model, nb_actions=nb_actions, nb_steps_warmup=10, memory=memory, policy=train1_policy)
dqn.compile(Adam(lr=1e-3), metrics=['mae'])

#training phase 1, save weights:
print('TRAINING PHASE 1')
env = gym.make(ENV_NAME, one_hot = False, start_month = 1, end_month = 1)
dqn.fit(env, nb_steps=monthly_requests_list[0], visualize=False, verbose=2)
dqn.save_weights(filepath, overwrite=True)

#load weights, training phase 2, save weights:
print('TRAINING PHASE 2')
dqn.load_weights(filepath)
env = gym.make(ENV_NAME, one_hot = False, start_month = 2, end_month = 2)
dqn.fit(env, nb_steps=monthly_requests_list[1], visualize=False, verbose=2)
dqn.save_weights(filepath, overwrite=True)

#load weights, training phase 3, save weights:
print('TRAINING PHASE 3')
dqn.load_weights(filepath)
env = gym.make(ENV_NAME, one_hot = False, start_month = 1, end_month = 2)
dqn.fit(env, nb_steps=sum_12, visualize=False, verbose=2)
dqn.save_weights(filepath, overwrite=True)

#load weights, training phase 4, save weights:
print('TRAINING PHASE 4')
dqn.load_weights(filepath)
env = gym.make(ENV_NAME, one_hot = False, start_month = 1, end_month = 2)
dqn.fit(env, nb_steps=sum_12, visualize=False, verbose=2)
dqn.save_weights(filepath, overwrite=True)

#new model with test policy, load weights, test_phase, save_weights
print('TESTING PHASE')
dqn = DQNAgent(model=model, nb_actions=nb_actions, memory=memory, policy=test_policy)
dqn.compile(Adam(lr=1e-3), metrics=['mae'])
dqn.load_weights(filepath)
env = gym.make(ENV_NAME, one_hot = False, start_month = 3, end_month = 12)
dqn.fit(env, nb_steps=sum_312, visualize=False, verbose=2)
dqn.save_weights(filepath, overwrite=True)
