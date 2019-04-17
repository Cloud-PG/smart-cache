import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


class SimpleCache(object):

    def __init__(self, init_state: dict={}):
        self._cache = set()
        self._hit = 0
        self._miss = 0
        self._size_history = []
        self._hit_rate_history = []

        if init_state:
            self._cache |= init_state['cache']
            self._hit += init_state['hit']
            self._miss += init_state['miss']
            self._size_history += init_state['size_history']
            self._hit_rate_history += init_state['hit_rate_history']

    @property
    def state(self):
        return {
            'cache': self._cache,
            'hit': self._hit,
            'miss': self._miss,
            'size_history': self._size_history,
            'hit_rate_history': self._hit_rate_history
        }

    @property
    def history(self):
        return list(zip(self._size_history, self._hit_rate_history))

    @property
    def size_history(self):
        return self._size_history

    @property
    def hit_rate_history(self):
        return self._hit_rate_history

    @property
    def hit_rate(self):
        return float(self._hit / (self._hit + self._miss)) * 100.

    def __len__(self):
        return len(self._cache)

    def check(self, file_):
        return file_ in self._cache

    def update(self, file_, insert: bool=True):
        if not self.check(file_):
            self._miss += 1
            if insert:
                self._cache |= set((file_, ))
        else:
            self._hit += 1

        self._size_history.append(len(self))
        self._hit_rate_history.append(self.hit_rate)

        return self


class Evaluator(object):

    def __init__(self, dataset, model, cache_type: str='simple', ai_cache_type: str='simple'):
        self._dataset = dataset
        self._model = model
        self.__cache_type = cache_type
        self.__ai_cache_type = ai_cache_type
        self.__cache_types = {
            'simple': SimpleCache
        }

    def _compare(
        self, initial_values: dict={}, next_window: bool=False
    ):
        cache = self.__cache_types[self.__cache_type]()
        ai_cache = self.__cache_types[self.__ai_cache_type]()

        if initial_values:
            cache = self.__cache_types[self.__cache_type](
                initial_values['cache'].state
            )
            ai_cache = self.__cache_types[self.__ai_cache_type](
                initial_values['ai_cache'].state
            )

        generator = None
        if not next_window:
            generator = self._dataset.get_raw_window()
        else:
            generator = self._dataset.get_raw_next_window()

        for idx, obj in tqdm(enumerate(generator), desc="Simulation"):
            FileName = obj['data']['FileName']
            tensor = obj['tensor']

            cache.update(FileName)

            prediction = self._model.predict_single(tensor)
            ai_cache.update(FileName, bool(prediction))

        return {
            'cache': cache,
            'ai_cache': ai_cache
        }

    def compare_window(self, show: bool=False):
        result = self._compare()

        self._plot_stats(
            {
                'cache': result['cache'].size_history,
                'ai_cache': result['ai_cache'].size_history
            },
            {
                'cache': result['cache'].hit_rate_history,
                'ai_cache': result['ai_cache'].hit_rate_history
            }
        )
        if show:
            plt.show()
        else:
            plt.savefig("compare_window.png")

    def compare_next_window(self, show: bool=False):
        result = self._compare(next_window=True)

        self._plot_stats(
            {
                'cache': result['cache'].size_history,
                'ai_cache': result['ai_cache'].size_history
            },
            {
                'cache': result['cache'].hit_rate_history,
                'ai_cache': result['ai_cache'].hit_rate_history
            }
        )
        if show:
            plt.show()
        else:
            plt.savefig("compare_next_window.png")

    def compare_all(self, show: bool=False):
        result = self._compare()
        separator = len(result['cache'].size_history)
        result = self._compare(
            initial_values=result,
            next_window=True
        )

        self._plot_stats(
            {
                'cache': result['cache'].size_history,
                'ai_cache': result['ai_cache'].size_history
            },
            {
                'cache': result['cache'].hit_rate_history,
                'ai_cache': result['ai_cache'].hit_rate_history
            }
        )
        plt.axvline(x=separator)
        if show:
            plt.show()
        else:
            plt.savefig("compare_all.png")

    def _plot_stats(self, size, hit_rate):
        plt.clf()
        # Size
        axes = plt.subplot(2, 1, 1)
        plt.plot(
            range(len(size['cache'])),
            size['cache'],
            label="cache [{}] size".format(self.__cache_type),
            alpha=0.9
        )
        plt.plot(
            range(len(size['ai_cache'])),
            size['ai_cache'],
            label="ai_cache size",
            alpha=0.9
        )
        axes.set_ylim(0)
        axes.set_xlim(0)
        plt.legend()
        # Hit rate
        axes = plt.subplot(2, 1, 2)
        plt.plot(
            range(len(hit_rate['cache'])),
            hit_rate['cache'],
            label="cache [{}] hit rate".format(self.__cache_type),
            alpha=0.9
        )
        plt.plot(
            range(len(hit_rate['ai_cache'])),
            hit_rate['ai_cache'],
            label="ai_cache hit rate",
            alpha=0.9
        )
        axes.set_ylim(0, 100)
        axes.set_xlim(0)
        plt.xlabel("Num. request accepted")
        plt.legend()
