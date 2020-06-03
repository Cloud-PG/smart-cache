from keras.models import Sequential
from keras.layers import Input, Dense, Activation, Flatten
import keras.optimizers
import tensorflow as tf
import math

def dense(input_len, output_activation, nb_actions):
    model = Sequential()
    model.add(Dense(16, input_dim=input_len))
    model.add(Activation('sigmoid'))
    model.add(Dense(32))
    model.add(Activation('sigmoid'))
    model.add(Dense(64))
    model.add(Activation('sigmoid'))
    model.add(Dense(128))
    model.add(Activation('sigmoid'))
    model.add(Dense(64))
    model.add(Activation('sigmoid'))
    model.add(Dense(32))
    model.add(Activation('sigmoid'))
    model.add(Dense(nb_actions))
    model.add(Activation(output_activation))
    print(model.summary())
    model.compile(optimizer='adam', loss=tf.keras.losses.Huber())

    return model

def small_dense(input_len, output_activation, nb_actions, lr):
    model = Sequential()
    model.add(Dense(16, input_dim=input_len))
    model.add(Activation('sigmoid'))
    model.add(Dense(32))
    model.add(Activation('sigmoid'))
    model.add(Dense(nb_actions))
    model.add(Activation(output_activation))
    print(model.summary())
    #opt = keras.optimizers.Adam(learning_rate=0.00001)
    opt = keras.optimizers.Adam(learning_rate=lr)
    model.compile(loss=tf.keras.losses.Huber(), optimizer=opt)
    #model.compile(optimizer='adam', loss=tf.keras.losses.Huber())

    return model

def big_dense(input_len, output_activation, nb_actions):
    model = Sequential()
    model.add(Dense(16, input_dim=input_len))
    model.add(Activation('sigmoid'))
    model.add(Dense(32))
    model.add(Activation('sigmoid'))
    model.add(Dense(64))
    model.add(Activation('sigmoid'))
    model.add(Dense(128))
    model.add(Activation('sigmoid'))
    model.add(Dense(256))
    model.add(Activation('sigmoid'))
    model.add(Dense(512))
    model.add(Activation('sigmoid'))
    model.add(Dense(256))
    model.add(Activation('sigmoid'))
    model.add(Dense(128))
    model.add(Activation('sigmoid'))
    model.add(Dense(64))
    model.add(Activation('sigmoid'))
    model.add(Dense(32))
    model.add(Activation('sigmoid'))
    model.add(Dense(nb_actions))
    model.add(Activation(output_activation))
    print(model.summary())
    model.compile(optimizer='adam', loss=tf.keras.losses.Huber())

    return model

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