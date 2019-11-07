import gzip
import json
import logging
import os
from concurrent import futures
from sys import argv
from time import time

import grpc
import numpy as np
import tensorflow as tf
from tensorflow import keras

from ..service import ai_pb2, ai_pb2_grpc


class DonkeyModel(ai_pb2_grpc.AIServiceServicer):

    def __init__(self, epochs: int = 20, batch_size: int = 64):
        self._batch_size = batch_size
        self._epochs = epochs
        self._model = None
        self._server = None
        # Outputs
        self.__num_predictions = 0
        self.__start_time = time()

        # Force CPU
        os.environ['CUDA_VISIBLE_DEVICES'] = '-1'

    def __compile_model(self, input_size: int, output_size: int,
                        cnn: bool = False
                        ):
        if cnn:
            self._model = keras.Sequential([
                keras.layers.Conv1D(
                    filters=64,
                    kernel_size=64,
                    activation='sigmoid',
                    # input_shape=input_shape
                    input_shape=(input_size, 1)
                ),
                # layers.BatchNormalization(),
                keras.layers.MaxPooling1D(pool_size=8),
                keras.layers.Dropout(0.5),
                keras.layers.Conv1D(32, 16, activation='sigmoid'),
                keras.layers.MaxPooling1D(pool_size=4),
                keras.layers.Dropout(0.5),
                keras.layers.Flatten(),
                keras.layers.Dense(512, activation='sigmoid'),
                keras.layers.Dense(output_size, activation='softmax')
            ])
        else:
            self._model = keras.Sequential([
                keras.layers.Dense(256, activation='hard_sigmoid',
                                   input_shape=(input_size, )),
                keras.layers.Dense(128, activation='hard_sigmoid'),
                keras.layers.Dense(128, activation='hard_sigmoid'),
                keras.layers.Dense(128, activation='hard_sigmoid'),
                keras.layers.Dense(32, activation='hard_sigmoid'),
                keras.layers.Dense(16, activation='hard_sigmoid'),
                keras.layers.Dense(8, activation='hard_sigmoid'),
                keras.layers.Dense(output_size, activation='softmax')
            ])
        self._model.compile(
            optimizer=keras.optimizers.Nadam(),
            loss='categorical_crossentropy',
            metrics=['accuracy']
        )
        self._model.summary()

    def train(self, data, labels, num_classes: int = 2):
        self.__compile_model(
            data.shape[1],
            num_classes,
        )
        self._model.fit(
            data,
            labels,
            batch_size=self._batch_size,
            epochs=self._epochs,
            validation_split=0.1
        )

    def AIPredictOne(self, request, context) -> 'ai_pb2.StorePrediction':
        prediction = self.predict_one(np.array(request.inputVector))

        response = ai_pb2.StorePrediction(
            store=True if prediction == 1 else False
        )

        self.__num_predictions += 1

        if time() - self.__start_time >= 1.0:
            print(f"[AI][Predicted {self.__num_predictions}/s]", end="\r")
            self.__num_predictions = 0
            self.__start_time = time()

        return response

    def predict_one(self, data):
        tmp = np.expand_dims(data, 0)
        prediction = self._model.predict(tmp)
        return np.argmax(prediction[0])

    def predict(self, data):
        predictions = self._model.predict(data)
        return np.argmax(predictions, axis=1)

    def export_weights(self, out_name: str):
        model = {
            'name': "DonkeyModel",
            'layers': []
        }
        config = self._model.get_config()['layers']
        for layer in config:
            layer_name = layer['config']['name']
            weights, bias = model.get_layer(layer_name).get_weights()
            model['layers'].append(
                {
                    'name': layer_name,
                    'weights': {
                        "shape": weights.shape,
                        "values": weights.tolist()
                    },
                    'bias': {
                        "shape": bias.shape,
                        "values": bias.tolist()
                    },
                    'activation_function': layer['config']['activation']
                }
            )
        with gzip.GzipFile("{}.json.gz".format(out_name), "wb") as outZip:
            outZip.write(
                json.dumps(model, indent=2).encode("utf-8")
            )
        self._model.save()

    def save(self, out_name: str):
        self._model.save("{}.h5".format(out_name))

    def load(self, filename: str):
        self._model = keras.models.load_model(filename)

    def serve(self, host: str = "127.0.0.1",
              port: int = 4242,
              max_workers: int = 10,
              ) -> 'DonkeyModel':
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=max_workers)
        )
        ai_pb2_grpc.add_AIServiceServicer_to_server(self, self._server)
        self._server.add_insecure_port(f'{host}:{port}')
        print(f"[AI][Serve on {host}:{port}]")
        self._server.start()
        self._server.wait_for_termination()
        return self

    def __del__(self):
        if self._server:
            self._server.stop(True)


class CMSTest0ModelGenerator(object):

    def __init__(self, epochs=100):
        self._epochs = epochs
        self._model = None

    def __compile_model(self, input_size: int, output_size: int):
        self._model = keras.Sequential([
            keras.layers.Flatten(input_shape=(input_size, )),
            keras.layers.Dense(512, activation='hard_sigmoid'),
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
        self._model = keras.models.load_model(filename)


if __name__ == "__main__":
    if argv[1] == "serve" and argv[2] == "donkey":
        model = DonkeyModel()
        model.load(argv[3])
        logging.basicConfig()
        model.serve()
    else:
        print("Use: python -m SmartCache.ai.models.generator serve 'model_name' 'model_file_path'")
