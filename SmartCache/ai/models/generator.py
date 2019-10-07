import numpy as np
import tensorflow as tf
from tensorflow import keras


class DonkeyModel(object):

    def __init__(self, epochs: int = 10, batch_size: int = 64):
        self._batch_size = batch_size
        self._epochs = epochs
        self._model = None

    def __compile_model(self, input_size: int, output_size: int):
        self._model = keras.Sequential([
            keras.layers.Flatten(input_shape=(input_size, )),
            # keras.layers.Dense(2048, activation='sigmoid'),
            # keras.layers.Dense(1024, activation='hard_sigmoid'),
            # keras.layers.Dense(768, activation='relu'),
            keras.layers.Dense(512, activation='sigmoid'),
            keras.layers.Dense(256, activation='hard_sigmoid'),
            keras.layers.Dense(128, activation='relu'),
            keras.layers.Dense(output_size, activation='softmax')
        ])
        self._model.compile(
            optimizer=keras.optimizers.Nadam(),
            loss='sparse_categorical_crossentropy',
            metrics=['accuracy']
        )
        self._model.summary()

    def train(
        self, dataset,
        normalized: bool = False,
    ):
        for data, labels, v_data, v_labels in dataset.get_train_data():
            if not self._model:
                self.__compile_model(
                    data.shape[1],
                    2
                )
            self._model.fit(
                data,
                labels,
                batch_size=self._batch_size,
                epochs=self._epochs,
                validation_data=(v_data, v_labels)
            )

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


class CMSTest0ModelGenerator(object):

    def __init__(self, epochs=100):
        self._epochs = epochs
        self._model = None

    def __compile_model(self, input_size: int, output_size: int):
        self._model = keras.Sequential([
            keras.layers.Flatten(input_shape=(input_size, )),
            keras.layers.Dense(2048, activation='hard_sigmoid'),
            keras.layers.Dense(1024, activation='hard_sigmoid'),
            keras.layers.Dense(512, activation='hard_sigmoid'),
            keras.layers.Dense(256, activation='hard_sigmoid'),
            keras.layers.Dense(output_size, activation='softmax')
        ])
        self._model.compile(optimizer='adam',
                            loss='sparse_categorical_crossentropy',
                            metrics=['accuracy'])

    def train(
        self, dataset,
        normalized: bool = False,
        one_hot: bool = True,
        one_hot_labels: bool = False,
        k_fold: int = 0
    ):
        for train_data, train_labels, validation_set in dataset.train_set(
                normalized=normalized,
                one_hot=one_hot,
                one_hot_labels=one_hot_labels,
                k_fold=k_fold
        ):
            if self._model is None:
                self.__compile_model(
                    train_data.shape[1],
                    dataset.get_num_classes()
                )
            if validation_set is not None:
                self._model.fit(
                    train_data,
                    train_labels,
                    epochs=self._epochs,
                    validation_data=validation_set
                )
            else:
                self._model.fit(
                    train_data,
                    train_labels,
                    epochs=self._epochs
                )

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
