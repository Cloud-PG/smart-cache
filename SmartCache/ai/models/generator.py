import numpy as np
import tensorflow as tf
from tensorflow.keras import Model
from tensorflow.keras.layers import Conv1D, Dense, Flatten


class CMSSimpleRecordModel(Model):
    def __init__(self):
        super(CMSSimpleRecordModel, self).__init__()
        self.conv1 = Conv1D(32, 2, activation='relu')
        self.flatten = Flatten()
        self.d1 = Dense(128, activation='relu')
        self.d2 = Dense(10, activation='softmax')

    def call(self, x):
        x = self.conv1(x)
        x = self.flatten(x)
        x = self.d1(x)
        return self.d2(x)


class CMSSimpleRecordModelGenerator(object):

    def __init__(self, epochs=5):
        self._model = CMSSimpleRecordModel()
        self._loss_object = tf.keras.losses.SparseCategoricalCrossentropy()
        self._optimizer = tf.keras.optimizers.Adam()
        self._train_loss = tf.keras.metrics.Mean(name='train_loss')
        self._train_accuracy = tf.keras.metrics.SparseCategoricalAccuracy(
            name='train_accuracy')
        self._test_loss = tf.keras.metrics.Mean(name='test_loss')
        self._test_accuracy = tf.keras.metrics.SparseCategoricalAccuracy(
            name='test_accuracy')
        self._epochs = epochs

    def train(self, dataset, batch_size=32):
        template = 'Epoch {}, Loss: {:0.5f}, Accuracy: {:0.5f}, Test Loss: {:0.5f}, Test Accuracy: {:0.5f}'
        features = dataset.features()
        labels = dataset.labels()
        num_train_records = int(len(features) * 0.8)
        train_set = np.array(zip(features[:num_train_records], labels[:num_train_records]))
        test_set = np.array(zip(features[num_train_records:], labels[num_train_records:]))
        train_set = np.array_split(train_set, batch_size, axis=0)
        test_set = np.array_split(test_set, batch_size, axis=0)
        for epoch in range(self._epochs):
            for record, label in train_set:
                print(record, label)
                self.train_step(record, label)

            for test_record, test_label in test_set:
                self.test_step(test_record, test_label)

            print(template.format(epoch+1,
                                  self._train_loss.result(),
                                  self._train_accuracy.result()*100,
                                  self._test_loss.result(),
                                  self._test_accuracy.result()*100))

    @tf.function
    def train_step(self, record, label):
        with tf.GradientTape() as tape:
            predictions = self._model(record)
            loss = self._loss_object(label, predictions)
        gradients = tape.gradient(loss, self._model.trainable_variables)
        self._optimizer.apply_gradients(
            zip(gradients, self._model.trainable_variables))

        train_loss(loss)
        train_accuracy(label, predictions)

    @tf.function
    def test_step(self, record, label):
        predictions = self._model(record)
        t_loss = self._loss_object(label, predictions)

        self._test_loss(t_loss)
        self._test_accuracy(label, predictions)
