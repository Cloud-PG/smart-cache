import numpy as np
import tensorflow as tf
from tensorflow import keras


class CMSSimpleRecordModelGenerator(object):

    def __init__(self, epochs=100):
        self._model = keras.Sequential([
            keras.layers.Flatten(input_shape=(4, )),
            keras.layers.Dense(1024, activation='relu'),
            keras.layers.Dense(512, activation='relu'),
            keras.layers.Dense(256, activation='relu'),
            keras.layers.Dense(2, activation='softmax')
        ])
        self._model.compile(optimizer='adam',
                            loss='sparse_categorical_crossentropy',
                            metrics=['accuracy'])
        self._epochs = epochs

    def train(self, dataset):
        train_data, train_labels = dataset.train_set(one_hot=False)
        self._model.fit(train_data, train_labels, epochs=self._epochs)

    def predict_single(self, data):
        tmp = np.expand_dims(data, 0)
        prediction = self._model.predict(tmp)
        return np.argmax(prediction[0])

    def predict(self, data):
        predictions = self._model.predict(data)
        return np.argmax(predictions, axis=1)

    def save(self, out_name: str):
        self._model.save("{}.h5".format(out_name))

    def load(self, out_name: str):
        self._model = keras.models.load_model("{}.h5".format(out_name))
