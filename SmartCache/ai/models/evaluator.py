import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


class SimpleCache(object):

    def __init__(self, init_state: dict={}):
        self._cache = []
        self._hit = 0
        self._miss = 0
        self._size_history = []
        self._hit_rate_history = []

        if init_state:
            self._cache += init_state['cache']
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

    def update(self, file_, insert: bool=True, insert_index: int=-1):
        if not self.check(file_):
            self._miss += 1
            if insert:
                if insert_index == -1:
                    self._cache.append(file_)
                else:
                    self._cache.insert(insert_index, file_)
        else:
            self._hit += 1

        self._size_history.append(len(self))
        self._hit_rate_history.append(self.hit_rate)

        return self


class FIFOCache(SimpleCache):

    def __init__(self, *args, max_size: int=1000, **kwargs):
        super(FIFOCache, self).__init__(*args, **kwargs)
        self._max_size = max_size

    def update(self, file_, insert: bool=True):
        if len(self._cache) == self._max_size:
            self._cache.pop(0)
        super(FIFOCache, self).update(file_, insert)


class LRUCache(SimpleCache):

    def __init__(self, *args, max_size: int=1000, **kwargs):
        super(LRUCache, self).__init__(*args, **kwargs)
        self._max_size = max_size
        self._counters = np.zeros(self._max_size)

        if 'init_state' in kwargs:
            self._max_size = kwargs['init_state']['max_size']
            self._counters = np.array(kwargs['init_state']['counters'])

    def update(self, file_, insert: bool=True):
        self._counters += 1

        if self.check(file_):
            self._hit += 1
            idx = self._cache.index(file_)
            self._counters[idx] = 0
        elif len(self._cache) == self._max_size:
            self._miss += 1
            if insert:
                idx = np.argmin(self._counters)
                self._cache.pop(idx)
                self._cache.insert(idx, file_)
                self._counters[idx] = 0
        else:
            self._miss += 1
            if insert:
                self._cache.append(file_)
                idx = self._cache.index(file_)
                self._counters[idx] = 0

        self._size_history.append(len(self))
        self._hit_rate_history.append(self.hit_rate)

    @property
    def state(self):
        return {
            'cache': self._cache,
            'hit': self._hit,
            'miss': self._miss,
            'size_history': self._size_history,
            'hit_rate_history': self._hit_rate_history,
            'max_size': self._max_size,
            'counters': list(self._counters)
        }


class Evaluator(object):

    def __init__(self, dataset, model, support_table, cache_type: str='simple', ai_cache_type: str='simple', cache_settings: dict={}):
        self._dataset = dataset
        self._support_table = support_table
        self._model = model
        self.__cache_type = cache_type.lower()
        self.__ai_cache_type = ai_cache_type.lower()
        self.__cache_types = {
            'simple': SimpleCache,
            'lru': LRUCache,
            'fifo': FIFOCache
        }
        self.__cache_settings = cache_settings

    def _compare(
        self, initial_values: dict={}
    ):
        cache = self.__cache_types[self.__cache_type](**self.__cache_settings)
        ai_cache = self.__cache_types[self.__ai_cache_type](
            **self.__cache_settings)

        if initial_values:
            cache = self.__cache_types[self.__cache_type](
                initial_values['cache'].state,
                **self.__cache_settings
            )
            ai_cache = self.__cache_types[self.__ai_cache_type](
                initial_values['ai_cache'].state,
                **self.__cache_settings
            )

        for _, obj in tqdm(enumerate(self._dataset), desc="Simulation"):
            FileName = obj['data']['FileName']
            ##
            # TO DO
            # Add support table configuration and dataset export
            # configuration to be loaded to pass also normalized and
            # one hot arguments
            tensor = np.array(
                self._support_table.close_conversion(
                    'features',
                    obj['features'],
                    normalized=False,
                    one_hot=True
                )
            )

            cache.update(FileName)

            prediction = self._model.predict_single(tensor)
            ai_cache.update(FileName, bool(prediction))

            if _ == 10000:
                break

        return {
            'cache': cache,
            'ai_cache': ai_cache
        }
    
    def compare(self, show: bool=False):
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
            },
            x_separator=separator
        )
        if show:
            plt.show()
        else:
            plt.savefig("compare_all.png")

    def _plot_stats(self, size, hit_rate, x_separator: int=-1):
        plt.clf()
        # Size
        axes = plt.subplot(2, 1, 1)
        if x_separator != -1:
            axes.axvline(x=x_separator)
        plt.plot(
            range(len(size['cache'])),
            size['cache'],
            label="cache [{}] size".format(self.__cache_type),
            alpha=0.9
        )
        plt.plot(
            range(len(size['ai_cache'])),
            size['ai_cache'],
            label="ai_cache [{}] hit rate".format(self.__ai_cache_type),
            alpha=0.9
        )
        axes.set_ylabel("Num. stored files")
        axes.set_ylim(0)
        axes.set_xlim(0)
        plt.legend()
        # Hit rate
        axes = plt.subplot(2, 1, 2)
        if x_separator != -1:
            axes.axvline(x=x_separator)
        plt.plot(
            range(len(hit_rate['cache'])),
            hit_rate['cache'],
            label="cache [{}] hit rate".format(self.__cache_type),
            alpha=0.9
        )
        plt.plot(
            range(len(hit_rate['ai_cache'])),
            hit_rate['ai_cache'],
            label="ai_cache [{}] hit rate".format(self.__ai_cache_type),
            alpha=0.9
        )
        axes.set_ylabel("Hit rate %")
        axes.set_ylim(0, 100)
        axes.set_xlim(0)
        plt.xlabel("Num. request accepted")
        plt.legend()
