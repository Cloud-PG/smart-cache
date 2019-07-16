import argparse
import gzip
import os
from functools import wraps
from multiprocessing import Pool
from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm


def simple_cost_function(size: float, frequency: float, num_files:
                         float, exp: float = 2.0):
    return size / (frequency / num_files) ** exp


class Cache(object):

    def __init__(self, init_state: dict = {}):
        self._cache: List[str] = []
        self._sizes: List[float] = []
        self._hit = 0
        self._miss = 0
        self.size_history: List[float] = []
        self.hit_rate_history: List[float] = []

        if init_state:
            self._cache += init_state['cache']
            self._hit += init_state['hit']
            self._miss += init_state['miss']
            self.size_history += init_state['size_history']
            self.hit_rate_history += init_state['hit_rate_history']

    @property
    def state(self):
        return {
            'cache': self._cache,
            'hit': self._hit,
            'miss': self._miss,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history
        }

    @property
    def history(self):
        return list(zip(self.size_history, self.hit_rate_history))

    @property
    def hit_rate(self):
        return float(self._hit / (self._hit + self._miss)) * 100.

    @property
    def size(self):
        return sum(self._sizes)

    def check(self, filename):
        return filename in self._cache

    def get(self, filename, size, **kwargs):
        hit = self.check(filename)
        if hit:
            self._hit += 1
        else:
            self._miss += 1

        self.update_policy(filename, size, hit, **kwargs)

        self.size_history.append(self.size)
        self.hit_rate_history.append(self.hit_rate)

    def update_policy(self, filename: str, size: float, hit: bool, **kwargs):
        raise NotImplementedError


class CacheLRU(Cache):

    def __init__(self, max_size: float, init_state: dict = {}):
        super().__init__(init_state=init_state)
        self._max_size = max_size
        self._counters: List[int] = []
        self.__counter = 0

        if init_state:
            self._max_size = init_state['max_size']
            self._counters = init_state['counters']
            self.__counter = init_state['counter']

    @property
    def state(self):
        return {
            'cache': self._cache,
            'hit': self._hit,
            'miss': self._miss,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history,
            'max_size': self._max_size,
            'counters': self._counters,
            'counter': self.__counter
        }

    def update_policy(self, filename: str, size: float, hit: bool, **kwargs):
        self.__counter += 1

        if hit:
            self._counters[self._cache.index(filename)] = self.__counter
        elif self.size + size > self._max_size:
            idx = self._counters.index(min(self._counters))
            self._cache.pop(idx)
            self._sizes.pop(idx)
            self._counters.pop(idx)

            self._cache.append(filename)
            self._sizes.append(size)
            self._counters.append(self.__counter)
        else:
            self._cache.append(filename)
            self._sizes.append(size)
            self._counters.append(self.__counter)


class CacheLRUMod(Cache):

    def __init__(self, max_size: float, init_state: dict = {}):
        super().__init__(init_state=init_state)
        self._max_size = max_size
        self._counters: List[int] = []
        self.__counter = 0

        if init_state:
            self._max_size = init_state['max_size']
            self._counters = init_state['counters']
            self.__counter = init_state['counter']

    @property
    def state(self):
        return {
            'cache': self._cache,
            'hit': self._hit,
            'miss': self._miss,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history,
            'max_size': self._max_size,
            'counters': self._counters,
            'counter': self.__counter
        }

    def update_policy(self, filename: str, size: float, hit: bool, **kwargs):
        self.__counter += 1

        if hit:
            self._counters[self._cache.index(filename)] = self.__counter
        elif kwargs['weight'] > kwargs['threshold']:
            if self.size + size > self._max_size:
                idx = self._counters.index(min(self._counters))
                self._cache.pop(idx)
                self._sizes.pop(idx)
                self._counters.pop(idx)

                self._cache.append(filename)
                self._sizes.append(size)
                self._counters.append(self.__counter)
            else:
                self._cache.append(filename)
                self._sizes.append(size)
                self._counters.append(self.__counter)


def star_decorator(func):
    @wraps(func)
    def star_wrapper(inputs):
        return func(*inputs)
    return star_wrapper


@star_decorator
def parse_data_frames(data: list, cost_function: callable,
                      exp_value: float = 2.0,
                      num_window: int = 0,
                      process_num: int = 0
                      ) -> 'pd.DataFrame':

    frequency_table: Dict[str, float] = {}
    num_files_table: Dict[str, set] = {}

    df = []
    for filename in tqdm(data, desc="Open files", position=process_num):
        with gzip.GzipFile(
            filename, mode="rb"
        ) as stats_file:
            df.append(pd.read_feather(stats_file))
    df = pd.concat(df).dropna()

    new_data: Dict[str, List] = {
        'filename': [],
        'weight': [],
        'frequency': [],
        'num_files': [],
        'size': [],
        'group': []
    }

    for _, record in tqdm(
        df.iterrows(), total=df.shape[0], position=process_num,
        desc=f"Parse window {num_window}"
    ):
        filename = record['filename']
        size = record['size']
        store_type, campain, process, file_type = [
            part for part in filename.split("/") if part
        ][:4]
        group = f"/{store_type}/{campain}/{process}/{file_type}/"

        if group not in frequency_table:
            frequency_table[group] = 0.
        if group not in num_files_table:
            num_files_table[group] = set()

        frequency_table[group] += 1.
        num_files_table[group] |= set((filename, ))

        new_data['filename'].append(filename)
        new_data['frequency'].append(frequency_table[group])
        new_data['num_files'].append(len(num_files_table[group]))
        new_data['size'].append(size)
        new_data['weight'].append(
            simple_cost_function(
                size,
                frequency_table[group],
                len(num_files_table[group]),
                exp_value
            )
        )
        new_data['group'].append(group)

        # TO TEST
        # if _ == 10000:
        #     break

    new_df = pd.DataFrame(data=new_data)
    # print(new_df.describe())
    # for name, group in new_df.groupby('group'):
    #     print(name)
    #     print(group)
    return new_df


@star_decorator
def test_function(data_filename: str, cache_size: float,
                  num_window: int = 0, process_num: int = 0
                  ) -> Dict[str, 'Cache']:

    with gzip.GzipFile(data_filename, mode="rb") as stats_file:
        df = pd.read_feather(stats_file)

    lru_cache = CacheLRU(max_size=cache_size)
    lru_mod_cache = CacheLRUMod(max_size=cache_size)

    wieght_mean = df['weight'].quantile(0.25)

    for _, record in tqdm(
        df.iterrows(), total=df.shape[0], position=process_num,
        desc=f"Cache simulation {num_window}"
    ):
        filename = record['filename']
        size = record['size'] / 1024**2
        weight = record['weight']
        lru_cache.get(filename, size)
        lru_mod_cache.get(filename, size,
                          weight=weight,
                          threshold=wieght_mean
                          )

    return {'lru': lru_cache, 'lru_mod': lru_mod_cache}


def plot_cache_results(caches: Dict[str, 'Cache'], out_file: str,
                       dpi: int = 300):
    grid = plt.GridSpec(8*len(caches), 6)

    axes = plt.subplot(grid[0:7, 0:])
    for cache_name, cache in caches.items():
        lenght = len(cache.size_history)
        axes.plot(
            range(lenght),
            cache.size_history,
            label=f"{cache_name}",
            alpha=0.9
        )
        axes.set_ylabel("Size (MB)")
    axes.legend()
    axes.set_xlim(0)
    axes.set_yscale('log')

    axes = plt.subplot(grid[8:15, 0:])
    for cache_name, cache in caches.items():
        lenght = len(cache.hit_rate_history)
        axes.plot(
            range(lenght),
            cache.hit_rate_history,
            label=f"{cache_name}",
            alpha=0.9
        )
        axes.set_ylabel("Hit rate %")
    axes.legend()
    axes.set_ylim(0, 100)
    axes.set_xlim(0)

    axes.set_xlabel("Requests")
    plt.savefig(
        out_file,
        dpi=dpi
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-folder', type=str, default="./results",
                        help='The folder where the json results are stored.')
    parser.add_argument('--window-size', '-pws', type=int, default=7,
                        help="Num. of days to plot")
    parser.add_argument('--exp-value', type=float, default=2,
                        help="Exponential of cost function")
    parser.add_argument('--jobs', '-j', type=int, default=4,
                        help="Num. of concurrent jobs")
    parser.add_argument('--cache-size', type=float, default=10.*1024.**2,
                        help="Cache size in MBytes (10TB default)")

    args, _ = parser.parse_known_args()

    files = list(sorted(os.listdir(args.result_folder)))

    data_frames = []
    windows = []
    pool = Pool(processes=args.jobs)

    counter = 0
    for file_idx, file_ in enumerate(tqdm(
        files, desc="Search stat results", position=0, ascii=True
    )):
        head, tail0 = os.path.splitext(file_)
        head, tail1 = os.path.splitext(head)

        if tail0 == ".gz" and tail1 == ".feather"\
                and head.find("results_") == 0:
            cur_file = os.path.join(args.result_folder, file_)
            data_frames.append(cur_file)

        if len(data_frames) == args.window_size:
            windows.append((
                data_frames, simple_cost_function, args.exp_value,
                counter,
                counter % args.jobs
            ))
            data_frames = []
            counter += 1

        # TO TEST
        # if file_idx == 7:
        #     break

    if len(data_frames) > 0:
        windows.append((
            data_frames, simple_cost_function,
            counter,
            counter % args.jobs
        ))
        data_frames = []

    for idx, new_df in enumerate(pool.imap(
        parse_data_frames, windows)
    ):
        cur_file = os.path.join(
            args.result_folder, f"window_{idx:02d}.feather.gz"
        )
        with gzip.GzipFile(cur_file, mode="wb") as output_file:
            new_df.to_feather(output_file)
        windows[idx] = (cur_file, args.cache_size, idx, idx % args.jobs)

    for idx, result in enumerate(pool.imap(
        test_function, windows)
    ):
        plot_cache_results(result, os.path.join(
            args.result_folder, f"window_{idx:02d}.png"
        ))


if __name__ == "__main__":
    main()
