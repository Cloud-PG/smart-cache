from datetime import timedelta

import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

from .utils import date_from_timestamp_ms


class SimpleCache(object):

    def __init__(self, init_state: dict = {}):
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

    def update(self, file_, insert: bool = True, insert_index: int = -1):
        hit = self.check(file_)
        if not hit:
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

        return hit


class FIFOCache(SimpleCache):

    def __init__(self, *args, max_size: int = 1000, **kwargs):
        super(FIFOCache, self).__init__(*args, **kwargs)
        self._max_size = max_size

    def update(self, file_, insert: bool = True):
        if len(self._cache) == self._max_size:
            self._cache.pop(0)
        return super(FIFOCache, self).update(file_, insert)


class LRUCache(SimpleCache):

    def __init__(self, *args, max_size: int = 1000, **kwargs):
        super(LRUCache, self).__init__(*args, **kwargs)
        self._max_size = max_size
        self._counters = np.zeros(self._max_size)

        if 'init_state' in kwargs:
            self._max_size = kwargs['init_state']['max_size']
            self._counters = np.array(kwargs['init_state']['counters'])

    def update(self, file_, insert: bool = True):
        self._counters += 1

        hit = self.check(file_)
        if hit:
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

        return hit

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

    def __init__(self, dataset, model, support_table, cache_type: str = 'simple', ai_cache_type: str = 'simple', cache_settings: dict = {}):
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
        self._wrap_cpu = {
            'cache': [],
            'ai_cache': []
        }

    def add_wrap_cpu(self, cache: str, amount: float, hit: bool):
        cur_cache = self._wrap_cpu[cache]
        if len(cur_cache) == 0:
            cur_cache.append(amount)
        else:
            if hit:
                cur_cache.append(amount + cur_cache[-1])
            else:
                cur_cache.append(cur_cache[-1])

    def _compare(
        self, initial_values: dict = {}
    ):
        cache = self.__cache_types[self.__cache_type](**self.__cache_settings)
        ai_cache = self.__cache_types[self.__ai_cache_type](
            **self.__cache_settings)

        separators = []

        if initial_values:
            cache = self.__cache_types[self.__cache_type](
                initial_values['cache'].state,
                **self.__cache_settings
            )
            ai_cache = self.__cache_types[self.__ai_cache_type](
                initial_values['ai_cache'].state,
                **self.__cache_settings
            )

        last_day = None
        delta_time = timedelta(days=1)

        for idx, obj in tqdm(enumerate(self._dataset), desc="Simulation"):
            FileName = obj['data']['FileName']
            WrapCPU = float(obj['data']['WrapCPU'])
            timestamp = obj['data'].get('StartedRunningTimeStamp', None)
            obj_deltatime = None

            if timestamp:
                obj_deltatime = date_from_timestamp_ms(timestamp)

            if obj_deltatime:
                if not last_day:
                    separators.append(idx)
                    last_day = obj_deltatime
                else:
                    if obj_deltatime - last_day > delta_time:
                        separators.append(idx)
                        last_day = obj_deltatime
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

            hit = cache.update(FileName)
            self.add_wrap_cpu('cache', WrapCPU, hit)

            prediction = self._model.predict_single(tensor)
            hit = ai_cache.update(FileName, bool(prediction))
            self.add_wrap_cpu('ai_cache', WrapCPU, hit)

            # Block for debug
            # if idx == 10000:
            #     break

        return {
            'cache': cache,
            'ai_cache': ai_cache,
            'separators': separators
        }

    def compare(self, show: bool = False, filename: str = "cache_compare.png", dpi: int = 300):
        result = self._compare()

        self._plot_stats(
            {
                'cache': result['cache'].size_history,
                'ai_cache': result['ai_cache'].size_history
            },
            {
                'cache': result['cache'].hit_rate_history,
                'ai_cache': result['ai_cache'].hit_rate_history
            },
            {
                'cache': self._wrap_cpu['cache'],
                'ai_cache': self._wrap_cpu['ai_cache']
            },
            result['separators']
        )
        if show:
            plt.show()
        else:
            plt.savefig(filename, dpi=dpi)

    def _plot_stats(self, size, hit_rate, wrap_cpu, x_separator: list = []):
        plt.clf()
        # Size
        axes = plt.subplot(3, 1, 1)
        for _x_ in x_separator:
            axes.axvline(x=_x_)
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
        axes = plt.subplot(3, 1, 2)
        for _x_ in x_separator:
            axes.axvline(x=_x_)
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
        plt.legend()
        # WrapCPU
        axes = plt.subplot(3, 1, 3)
        for _x_ in x_separator:
            axes.axvline(x=_x_)
        plt.plot(
            range(len(wrap_cpu['cache'])),
            wrap_cpu['cache'],
            label="cache [{}] WrapCPU".format(self.__cache_type),
            alpha=0.9
        )
        plt.plot(
            range(len(wrap_cpu['ai_cache'])),
            wrap_cpu['ai_cache'],
            label="ai_cache [{}] WrapCPU".format(self.__ai_cache_type),
            alpha=0.9
        )
        axes.set_ylabel("WrapCPU on hit")
        axes.set_ylim(0)
        axes.set_xlim(0)
        plt.xlabel("Num. request accepted")
        plt.legend()
        plt.tight_layout()
