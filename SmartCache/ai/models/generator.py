import numpy as np
import tensorflow as tf
from tensorflow import keras


class CMSTest0ModelGenerator(object):

    def __init__(self, epochs=100):
        self._epochs = epochs
        self._model = None

    def __compile_model(self, input_size: int, output_size: int):
        self._model = keras.Sequential([
            keras.layers.Flatten(input_shape=(input_size, )),
            keras.layers.Dense(2048, activation='relu'),
            keras.layers.Dense(1024, activation='relu'),
            keras.layers.Dense(512, activation='relu'),
            keras.layers.Dense(256, activation='relu'),
            keras.layers.Dense(output_size, activation='softmax')
        ])
        self._model.compile(optimizer='adam',
                            loss='sparse_categorical_crossentropy',
                            metrics=['accuracy'])

    def train(self, dataset, normalized: bool = False, one_hot: bool = True, one_hot_labels: bool = False):
        train_data, train_labels = dataset.train_set(normalized=normalized, one_hot=one_hot, one_hot_labels=one_hot_labels)
        if self._model is None:
            self.__compile_model(train_data.shape[1], dataset.get_num_classes())
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

    def load(self, filename: str):
        self._model = keras.models.load_model("{}.h5".format(filename))
