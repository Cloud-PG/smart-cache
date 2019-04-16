import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


class SimpleCache(object):

    def __init__(self, init_state: dict={}):
        self._cache = set()
        self._hit = 0
        self._miss = 0
        self._history = []

        if init_state:
            self._cache |= init_state['cache']
            self._hit += init_state['hit']
            self._miss += init_state['miss']
            self._history += init_state['history']

    @property
    def state(self):
        return {
            'cache': self._cache,
            'hit': self._hit,
            'miss': self._miss,
            'history': self._history
        }

    @property
    def history(self):
        return list(zip(*self._history))

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
        self._history.append(
            len(self),
            self.hit_rate()
        )
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
                initial_values['cache']
            )
            ai_cache = self.__cache_types[self.__ai_cache_type](
                initial_values['ai_cache']
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
                'cache': result['cache']['size'],
                'ai_cache': result['ai_cache']['size']
            },
            {
                'cache': result['cache']['hit_ratio'],
                'ai_cache': result['ai_cache']['hit_ratio']
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
                'cache': result['cache']['size'],
                'ai_cache': result['ai_cache']['size']
            },
            {
                'cache': result['cache']['hit_ratio'],
                'ai_cache': result['ai_cache']['hit_ratio']
            }
        )
        if show:
            plt.show()
        else:
            plt.savefig("compare_next_window.png")

    def compare_all(self, show: bool=False):
        result = self._compare()
        separator = len(result['cache']['size'])
        result = self._compare(
            initial_values=result,
            next_window=True
        )

        self._plot_stats(
            {
                'cache': result['cache']['size'],
                'ai_cache': result['ai_cache']['size']
            },
            {
                'cache': result['cache']['hit_ratio'],
                'ai_cache': result['ai_cache']['hit_ratio']
            }
        )
        plt.axvline(x=separator)
        if show:
            plt.show()
        else:
            plt.savefig("compare_all.png")

    @staticmethod
    def _plot_stats(size, hit_rate):
        plt.clf()
        # Size
        plt.subplot(2, 1, 1)
        plt.plot(range(len(size['cache'])),
                 size['cache'], label="cache size", alpha=0.9)
        plt.plot(range(len(size['ai_cache'])), size['ai_cache'],
                 label="ai_cache size", alpha=0.9)
        plt.legend()
        # Hit rate
        plt.subplot(2, 1, 2)
        plt.plot(range(len(hit_rate['cache'])),
                 hit_rate['cache'], label="cache hit rate", alpha=0.9)
        plt.plot(range(len(hit_rate['ai_cache'])), hit_rate['ai_cache'],
                 label="ai_cache hit rate", alpha=0.9)
        plt.xlabel("Num. request accepted")
        plt.legend()
