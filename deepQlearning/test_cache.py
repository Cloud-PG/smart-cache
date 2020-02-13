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

tf.compat.v1.disable_eager_execution()

ENV_NAME = 'CacheEnv-v0'

#with gzip.open("results_2018-01-01.csv.gz") as f:
#    df_=pd.read_csv(f)
#df_['region']=df_.SiteName.str.split("_",expand=True)[1]
#df_['Size']=df_['Size']/1.049e+6
#df_=df_[df_['region']=='IT']
#df_=df_.reset_index()

#env = gym.make(ENV_NAME,df = df_)

env = gym.make(ENV_NAME, total_days = 90)

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

policy = EpsGreedyQPolicy()
memory = SequentialMemory(limit=50000, window_length=1)

#dqn = DQNAgent(model=model, nb_actions=nb_actions, memory=memory, nb_steps_warmup=10, target_model_update=1e-2, policy=policy)
dqn = DQNAgent(model=model, nb_actions=nb_actions, memory=memory, nb_steps_warmup=1000, policy=policy)
#dqn.compile(Adam(lr=1e-3), metrics=['mae'])
dqn.compile(Adam(lr=1e-1), metrics=['mae'])

dqn.fit(env, nb_steps=3000000, visualize=False, verbose=2)

#dqn.test(env, nb_episodes=1, visualize=False)