import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


class Evaluator(object):

    def __init__(self, dataset, model):
        self._dataset = dataset
        self._model = model

    def _compare(self, initial_values: set=(), next_window: bool=False):
        raise NotImplementedError

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
        plt.xlabel("Num. request accepted x{}".format(stride))
        plt.legend()


class SimpleCacheFiniteSpaceLRU(Evaluator):

    def __init__(self, dataset, model, max_num_files=1000):
        super(SimpleCacheInfiniteSpace, self).__init__(dataset, model)
        self.__cache_size = 1000

    def _compare(
        self, initial_values: set=(), next_window: bool=False
    ):
        pass


class SimpleCacheInfiniteSpace(Evaluator):

    def __init__(self, dataset, model):
        super(SimpleCacheInfiniteSpace, self).__init__(dataset, model)

    def _compare(
        self, initial_values: dict={}, next_window: bool=False
    ):
        cache = set()
        cache_hit_ratio = []
        ai_cache = set()
        ai_cache_hit_ratio = []

        cache_hit = 0
        cache_miss = 0
        ai_cache_hit = 0
        ai_cache_miss = 0

        size_cache = []
        size_ai_cache = []

        if initial_values:
            cache |= set(initial_values['cache']['files'])
            cache_hit_ratio += initial_values['cache']['hit_ratio']
            size_cache += initial_values['cache']['size']
            ai_cache |= set(initial_values['ai_cache']['files'])
            ai_cache_hit_ratio += initial_values['ai_cache']['hit_ratio']
            size_ai_cache += initial_values['ai_cache']['size']
            cache_hit = initial_values['cache']['hit']
            cache_miss = initial_values['cache']['miss']
            ai_cache_hit = initial_values['ai_cache']['hit']
            ai_cache_miss = initial_values['ai_cache']['miss']

        generator = None
        if not next_window:
            generator = self._dataset.get_raw_window()
        else:
            generator = self._dataset.get_raw_next_window()

        for idx, obj in tqdm(enumerate(generator), desc="Simulation"):
            FileName = obj['data']['FileName']
            tensor = obj['tensor']

            if FileName in cache:
                cache_hit += 1
            else:
                cache_miss += 1
                cache |= set((FileName,))

            if FileName in ai_cache:
                ai_cache_hit += 1
            else:
                ai_cache_miss += 1
                prediction = self._model.predict_single(tensor)
                if prediction != 0:
                    ai_cache |= set((FileName,))

            size_cache.append(len(cache))
            size_ai_cache.append(len(ai_cache))

            cache_hit_ratio.append(
                float(cache_hit / (cache_hit + cache_miss)) * 100.
                if cache_hit > 0 else 0.
            )
            ai_cache_hit_ratio.append(
                float(ai_cache_hit / (ai_cache_hit + ai_cache_miss)) * 100.
                if ai_cache_hit > 0 else 0.
            )

        return {
            'cache': {
                'files': cache,
                'size': size_cache,
                'hit_ratio': cache_hit_ratio,
                'hit': cache_hit,
                'miss': cache_miss
            },
            'ai_cache': {
                'files': ai_cache,
                'size': size_ai_cache,
                'hit_ratio': ai_cache_hit_ratio,
                'hit': ai_cache_hit,
                'miss': ai_cache_miss
            }
        }
