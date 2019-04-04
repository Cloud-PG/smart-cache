import numpy as np
import tensorflow as tf
from tensorflow import keras


class CMSSimpleRecordModelGenerator(object):

    def __init__(self, epochs=10):
        self._model = keras.Sequential([
            keras.layers.Flatten(input_shape=(4, )),
            keras.layers.Dense(128, activation='relu'),
            keras.layers.Dense(128, activation='relu'),
            keras.layers.Dense(64, activation='relu'),
            keras.layers.Dense(2, activation='softmax')
        ])
        self._model.compile(optimizer='adam',
                            loss='sparse_categorical_crossentropy',
                            metrics=['accuracy'])
        self._epochs=epochs

    def train(self, dataset):
        train_set = dataset.features()
        train_labels = dataset.labels(one_hot=False)
        self._model.fit(train_set, train_labels, epochs=self._epochs)

    def predict_single(self, data):
        tmp = np.expand_dims(data,0)
        prediction = self._model.predict(tmp)
        return np.argmax(prediction[0])
    
    def predict(self, data):
        predictions = self._model.predict(data)
        return np.argmax(predictions, axis=1)
