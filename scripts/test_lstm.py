from __future__ import print_function

import argparse
import collections
import os
from datetime import datetime, timedelta

import numpy as np
import tensorflow as tf
import urllib3
from minio import Minio
from minio.error import (BucketAlreadyExists, BucketAlreadyOwnedByYou,
                         ResponseError)
from tensorflow.keras.callbacks import ModelCheckpoint
from tensorflow.keras.layers import (LSTM, Activation, Dense, Dropout,
                                     Embedding, TimeDistributed)
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.utils import to_categorical
from tqdm import tqdm

from DataManager import DataFile
import string

DATA_PATH = "./tmp_lstm_data"


def read_words(filename):
    with tf.io.gfile.GFile(filename, "r") as f:
        return f.read().replace("\n", "<eos>").split()


def build_vocab(filename):
    data = read_words(filename)

    counter = collections.Counter(data)
    count_pairs = sorted(counter.items(), key=lambda x: (-x[1], x[0]))

    words, _ = list(zip(*count_pairs))
    word_to_id = dict(zip(words, range(len(words))))

    return word_to_id


def file_to_word_ids(filename, word_to_id):
    data = read_words(filename)
    return [word_to_id[word] for word in data if word in word_to_id]


def load_data():
    # get the data paths
    train_path = os.path.join(DATA_PATH, "requests.train.txt")
    valid_path = os.path.join(DATA_PATH, "requests.valid.txt")
    test_path = os.path.join(DATA_PATH, "requests.test.txt")

    # build the complete vocabulary, then convert text data to list of integers
    word_to_id = build_vocab(train_path)
    train_data = file_to_word_ids(train_path, word_to_id)
    valid_data = file_to_word_ids(valid_path, word_to_id)
    test_data = file_to_word_ids(test_path, word_to_id)
    vocabulary = len(word_to_id)
    reversed_dictionary = dict(zip(word_to_id.values(), word_to_id.keys()))

    # print(train_data[:5])
    # print(word_to_id)
    # print(vocabulary)
    # print(" ".join([reversed_dictionary[x] for x in train_data[:10]]))
    return train_data, valid_data, test_data, vocabulary, reversed_dictionary


class KerasBatchGenerator(object):

    def __init__(self, data, num_steps, batch_size, vocabulary, skip_step=5):
        self.data = data
        self.num_steps = num_steps
        self.batch_size = batch_size
        self.vocabulary = vocabulary
        # this will track the progress of the batches sequentially through the
        # data set - once the data reaches the end of the data set it will reset
        # back to zero
        self.current_idx = 0
        # skip_step is the number of words which will be skipped before the next
        # batch is skimmed from the data set
        self.skip_step = skip_step

    def generate(self):
        x = np.zeros((self.batch_size, self.num_steps))
        y = np.zeros((self.batch_size, self.num_steps, self.vocabulary))
        while True:
            for i in range(self.batch_size):
                if self.current_idx + self.num_steps >= len(self.data):
                    # reset the index back to the start of the data set
                    self.current_idx = 0
                x[i, :] = self.data[self.current_idx:self.current_idx + self.num_steps]
                temp_y = self.data[self.current_idx +
                                   1:self.current_idx + self.num_steps + 1]
                # convert all of temp_y into a one hot representation
                y[i, :, :] = to_categorical(
                    temp_y, num_classes=self.vocabulary)
                self.current_idx += self.skip_step
            yield x, y


def gen_model(vocabulary, num_steps: int = 16, hidden_size: int = 256, use_dropout: bool = True):

    model = Sequential()
    model.add(Embedding(vocabulary, hidden_size, input_length=num_steps))
    model.add(LSTM(hidden_size, return_sequences=True))
    model.add(LSTM(hidden_size//2, return_sequences=True))
    model.add(LSTM(hidden_size//4, return_sequences=True))
    if use_dropout:
        model.add(Dropout(0.5))
    model.add(TimeDistributed(Dense(vocabulary)))
    model.add(Activation('softmax'))

    model.compile(loss='categorical_crossentropy',
                  optimizer='adam', metrics=['categorical_accuracy'])

    print(model.summary())

    return model


def period(start_date, num_days):
    delta = timedelta(days=1)

    year, month, day = [int(elm) for elm in start_date.split()]
    cur_date = datetime(year, month, day)

    for _ in range(num_days):
        yield (cur_date.year, cur_date.month, cur_date.day)
        cur_date = cur_date+delta


def convert_record(record, max_depth: int = 7):
    tmp = "/".join(record.split("/")[:max_depth])
    for elm in string.punctuation:
        tmp = tmp.replace(elm, " ")
    return tmp + "\n"


def gen_data(start_date, window_size, minio_config, validation_stride: int = 100):
    os.makedirs(DATA_PATH, exist_ok=True)

    cert_reqs = "CERT_NONE"

    if cert_reqs == "CERT_NONE":
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    httpClient = urllib3.PoolManager(
        timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
        cert_reqs=cert_reqs,
        # ca_certs='public.crt',
        retries=urllib3.Retry(
            total=5,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504]
        )
    )

    minio_url, minio_key, minio_secret, bucket = minio_config.split()
    minioClient = Minio(
        minio_url,
        access_key=minio_key,
        secret_key=minio_secret,
        secure=True,
        http_client=httpClient
    )

    start = "{} {} {}".format(*start_date.split("-"))
    size = int(window_size)

    # Train and validation set
    with open(os.path.join(DATA_PATH, "requests.train.txt"), "w") as train_set:
        with open(os.path.join(DATA_PATH, "requests.valid.txt"), "w") as validation_set:
            for year, month, day in period(start, size):
                print(f"[Original Data][{year}-{month}-{day}]")
                print("[Original Data][Download...]")
                try:
                    minioClient.fget_object(
                        f"{bucket}",
                        f'year{year}_month{month}_day{day}.json.gz',
                        './tmp.json.gz'
                    )
                except ResponseError:
                    raise
                print("[Original Data][Downloaded]")
                print("[Original Data][Open File]")
                collector = DataFile("./tmp.json.gz")
                counter = 0
                print(
                    "[Original Data][Write record in train set and validation set...]")
                for record in tqdm(collector, desc="Extracting records"):
                    cur_record = convert_record(record['FileName'])
                    # print(cur_record)
                    if counter < validation_stride:
                        train_set.write(cur_record)
                        counter += 1
                    else:
                        validation_set.write(cur_record)
                        counter = 0
                os.remove("./tmp.json.gz")

    # Test set
    last_date = datetime(year, month, day) + timedelta(days=1)
    start = f"{last_date.year} {last_date.month} {last_date.day}"
    with open(os.path.join(DATA_PATH, "requests.test.txt"), "w") as test_set:
        for year, month, day in period(start, size):
            print(f"[Original Data][{year}-{month}-{day}]")
            print("[Original Data][Download...]")
            try:
                minioClient.fget_object(
                    f"{bucket}",
                    f'year{year}_month{month}_day{day}.json.gz',
                    './tmp.json.gz'
                )
            except ResponseError:
                raise
            print("[Original Data][Downloaded]")
            print("[Original Data][Open File]")
            collector = DataFile("./tmp.json.gz")
            print("[Original Data][Write record in test set...]")
            for record in tqdm(collector, desc="Extracting records"):
                cur_record = convert_record(record['FileName'])
                if counter < validation_stride:
                    test_set.write(cur_record)
            os.remove("./tmp.json.gz")


def main():
    global DATA_PATH
    parser = argparse.ArgumentParser()
    parser.add_argument('run_opt', type=str, default="train",
                        choices=['train', 'test', 'data'])
    parser.add_argument('--data-path', type=str, default=DATA_PATH,
                        help='The full path of the training data')
    parser.add_argument('--num-epochs', type=int, default=10,
                        help='Number of epochs')
    parser.add_argument('--num-steps', type=int, default=16,
                        help='Number of steps')
    parser.add_argument('--batch-size', type=int, default=16,
                        help='Batch size')
    parser.add_argument('--hidden-size', type=int, default=512,
                        help='Hidden size')
    parser.add_argument('--gen-data-args', type=str, default="YY-MM-DD window_size",
                        help='gen_data_args')
    parser.add_argument('--minio-config', type=str, default="url key secret bucket",
                        help='minio configuration')
    args = parser.parse_args()

    if args.data_path:
        DATA_PATH = args.data_path

    # print(args)

    if args.run_opt == "data":
        gen_data(*args.gen_data_args.split(), args.minio_config)

    elif args.run_opt == "train":
        print("Loading data...")
        train_data, valid_data, test_data, vocabulary, reversed_dictionary = load_data()
        model = gen_model(vocabulary, args.num_steps, args.hidden_size)
        checkpointer = ModelCheckpoint(
            filepath=DATA_PATH + '/model-{epoch:02d}.hdf5', verbose=1
        )
        train_data_generator = KerasBatchGenerator(
            train_data, args.num_steps, args.batch_size, vocabulary,
            skip_step=args.num_steps
        )
        valid_data_generator = KerasBatchGenerator(
            valid_data, args.num_steps, args.batch_size, vocabulary,
            skip_step=args.num_steps
        )
        model.fit_generator(
            train_data_generator.generate(),
            len(train_data)//(args.batch_size*args.num_steps),
            args.num_epochs,
            validation_data=valid_data_generator.generate(),
            validation_steps=len(valid_data)//(args.batch_size*args.num_steps),
            callbacks=[checkpointer]
        )
        model.save(DATA_PATH + "/final_model.hdf5")

    elif args.run_opt == "test":
        print("Loading data...")
        train_data, valid_data, test_data, vocabulary, reversed_dictionary = load_data()
        model = load_model(DATA_PATH + "/final_model.hdf5")
        dummy_iters = 40
        example_training_generator = KerasBatchGenerator(
            train_data, args.num_steps, 1, vocabulary,
            skip_step=1
        )
        print("Training data:")
        for i in range(dummy_iters):
            dummy = next(example_training_generator.generate())
        num_predict = 10
        true_print_out = "Actual words: "
        pred_print_out = "Predicted words: "
        for i in range(num_predict):
            data = next(example_training_generator.generate())
            prediction = model.predict(data[0])
            predict_word = np.argmax(prediction[:, args.num_steps-1, :])
            true_print_out += reversed_dictionary[train_data[args.num_steps +
                                                             dummy_iters + i]] + " "
            pred_print_out += reversed_dictionary[predict_word] + " "
            print(true_print_out)
            print(pred_print_out)
        # test data set
        dummy_iters = 40
        example_test_generator = KerasBatchGenerator(
            test_data, args.num_steps, 1, vocabulary,
            skip_step=1
        )
        print("Test data:")
        for i in range(dummy_iters):
            dummy = next(example_test_generator.generate())
        num_predict = 10
        true_print_out = "Actual words: "
        pred_print_out = "Predicted words: "
        for i in range(num_predict):
            data = next(example_test_generator.generate())
            prediction = model.predict(data[0])
            predict_word = np.argmax(prediction[:, args.num_steps - 1, :])
            true_print_out += reversed_dictionary[
                test_data[
                    args.num_steps + dummy_iters + i]
            ] + " "
            pred_print_out += reversed_dictionary[predict_word] + " "
            print(true_print_out)
            print(pred_print_out)

        test_data_generator = KerasBatchGenerator(
            test_data, args.num_steps, args.batch_size, vocabulary,
            skip_step=args.num_steps
        ).generate()

        model.evaluate_generator(
            test_data_generator,
            len(test_data)//(args.batch_size*args.num_steps),
            verbose=1
        )


if __name__ == "__main__":
    main()
