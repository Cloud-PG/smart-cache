import argparse
import gzip
import itertools
import os
from collections import OrderedDict
from functools import wraps
from multiprocessing import Pool, current_process
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

    def reset_history(self):
        self.size_history = []
        self.hit_rate_history = []
        self._hit = 0
        self._miss = 0

    @property
    def state(self):
        return {
            'cache': self._cache,
            'sizes': self._sizes,
            'hit': self._hit,
            'miss': self._miss,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history
        }

    def __getstate__(self):
        return self.state

    def __setstate__(self, state):
        self._cache = state['cache']
        self._sizes = state['sizes']
        self._hit = state['hit']
        self._miss = state['miss']
        self.size_history = state['size_history']
        self.hit_rate_history = state['hit_rate_history']

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
            'sizes': self._sizes,
            'hit': self._hit,
            'miss': self._miss,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history,
            'max_size': self._max_size,
            'counters': self._counters,
            'counter': self.__counter
        }

    def __setstate__(self, state):
        self._cache = state['cache']
        self._sizes = state['sizes']
        self._hit = state['hit']
        self._miss = state['miss']
        self.size_history = state['size_history']
        self.hit_rate_history = state['hit_rate_history']
        self._max_size = state['max_size']
        self._counters = state['counters']
        self.__counter = state['counter']

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
            'sizes': self._sizes,
            'hit': self._hit,
            'miss': self._miss,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history,
            'max_size': self._max_size,
            'counters': self._counters,
            'counter': self.__counter
        }

    def __setstate__(self, state):
        self._cache = state['cache']
        self._sizes = state['sizes']
        self._hit = state['hit']
        self._miss = state['miss']
        self.size_history = state['size_history']
        self.hit_rate_history = state['hit_rate_history']
        self._max_size = state['max_size']
        self._counters = state['counters']
        self.__counter = state['counter']

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
def parse_data_frames(source, target: str,
                      cost_function: callable,
                      exp_values: List[float],
                      num_window: int = 0,
                      ) -> 'pd.DataFrame':
    process_num = int(str(current_process()).split("Worker-")[1].split(",")[0])
    frequency_table: Dict[str, float] = {}
    num_files_table: Dict[str, set] = {}

    new_data: Dict[str, List] = {
        'filename': [],
        'frequency': [],
        'num_files': [],
        'size': [],
        'group': []
    }

    for exp in exp_values:
        new_data[f'weight_sE{int(exp)}'] = []

    if target:
        with gzip.GzipFile(source, mode="rb") as stats_file:
            source_df = pd.read_feather(stats_file)

        with gzip.GzipFile(target, mode="rb") as stats_file:
            df = pd.read_feather(stats_file).dropna()

        groups = source_df.groupby('group').tail(1)[[
            'group', 'frequency', 'num_files']
        ]
        files = source_df.groupby('group')[[
            'group', 'filename']
        ]

        for _, record in tqdm(
            groups.iterrows(), total=groups.shape[0], position=process_num,
            desc=f"Add previous data to {num_window}", ascii=True
        ):
            group = record['group']
            frequency_table[group] = record['frequency']
            num_files_table[group] = set(
                files.get_group(group)['filename'].to_list()
            )
    else:
        df = []
        for filename in tqdm(source, desc="Open files", position=process_num,
                             ascii=True):
            with gzip.GzipFile(
                filename, mode="rb"
            ) as stats_file:
                df.append(pd.read_feather(stats_file))
        df = pd.concat(df).dropna()

    for _, record in tqdm(
        df.iterrows(), total=df.shape[0], position=process_num,
        desc=f"Parse window {num_window}", ascii=True
    ):
        filename = record['filename']
        size = record['size'] / 1024**2  # Convert from Bytes to MegaBytes
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

        cur_frequency = frequency_table[group]
        cur_num_files = len(num_files_table[group])

        new_data['filename'].append(filename)
        new_data['frequency'].append(cur_frequency)
        new_data['num_files'].append(cur_num_files)
        new_data['size'].append(size)
        for exp in exp_values:
            new_data[f'weight_sE{exp}'].append(
                simple_cost_function(
                    size,
                    cur_frequency,
                    cur_num_files,
                    exp
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
def test_function(data_filename: str, cache_sizes: List[float],
                  num_window: int = 0,
                  prev_cache_state: tuple = None,
                  quantile_list: List[float] = [0.25, 0.5, 0.75]
                  ) -> tuple:
    process_num = int(str(current_process()).split("Worker-")[1].split(",")[0])
    with gzip.GzipFile(data_filename, mode="rb") as stats_file:
        df = pd.read_feather(stats_file)

    if prev_cache_state:
        result, lru_mod, weight_table = prev_cache_state
        for key in [
            value for value in result.keys() if value.find("_mod") != -1
        ]:
            del result[key]
        for cache in result.values():
            cache.reset_history()
        for _, cache in lru_mod.values():
            cache.reset_history()
    else:
        result = OrderedDict()

        lru_mod: Dict[str, tuple(str, 'CacheLRUMod')] = {}
        weight_table: Dict[str, float] = {}
        available_weights = [
            elm for elm in df.columns if elm.find("weight_") != -1
        ]

        for cache_size in cache_sizes:
            result[f'lru_{int(cache_size / 1024**2)}T'] = CacheLRU(
                max_size=cache_size
            )

        for cache_size in cache_sizes:
            for weight in available_weights:
                weight_type = weight.split("_")[1]
                current_cache_name = f'lru_mod_{int(cache_size / 1024**2)}T_{weight_type}'
                for quantile in quantile_list:
                    full_name = f'{current_cache_name}_q{int(quantile*100.)}'
                    lru_mod[full_name] = (
                        weight, CacheLRUMod(max_size=cache_size))
                    weight_table[full_name] = df[weight].quantile(quantile)

    for _, record in tqdm(
        df.iterrows(), total=df.shape[0], position=process_num,
        desc=f"Cache simulation {num_window}", ascii=True
    ):
        filename = record['filename']
        size = record['size']

        for cache in result.values():
            cache.get(filename, size)

        for name, (type_, cache) in lru_mod.items():
            weight = record[type_]
            cache.get(
                filename, size,
                weight=weight,
                threshold=weight_table[name]
            )

    for name, (_, cache) in lru_mod.items():
        result[name] = cache

    return result, lru_mod, weight_table


def plot_cache_results(caches: Dict[str, 'Cache'], out_file: str,
                       dpi: int = 300):
    grid = plt.GridSpec(64, 32, wspace=1.42, hspace=1.42)
    styles = itertools.cycle(
        itertools.product(
            (',', '+', '.', 'o', '*'), ('-', '--', '-.', ':')
        )
    )
    markevery = itertools.cycle([500, 1000, 1500, 2000])
    marker_list = []
    linestyle_list = []

    axes = plt.subplot(grid[0:31, 0:])
    for cache_name, cache in caches.items():
        cur_marker, cur_linestyle = next(styles)
        marker_list.append(cur_marker)
        linestyle_list.append(cur_linestyle)
        lenght = len(cache.size_history)
        axes.plot(
            range(lenght),
            cache.size_history,
            label=f"{cache_name}",
            marker=cur_marker,
            markevery=next(markevery),
            linestyle=cur_linestyle,
            # alpha=0.9
        )
        axes.set_ylabel("Size (MB)")
    legend = axes.legend(bbox_to_anchor=(1.0, 1.0))
    axes.grid()
    axes.set_xlim(0)
    axes.set_yscale('log')

    axes = plt.subplot(grid[32:, 0:])
    for idx, (cache_name, cache) in enumerate(caches.items()):
        lenght = len(cache.hit_rate_history)
        axes.plot(
            range(lenght),
            cache.hit_rate_history,
            label=f"{cache_name}",
            marker=marker_list[idx],
            markevery=next(markevery),
            linestyle=linestyle_list[idx],
            # alpha=0.9
        )
        axes.set_ylabel("Hit rate %")
    axes.grid()
    axes.set_ylim(0, 100)
    axes.set_xlim(0)

    axes.set_xlabel("Requests")
    plt.savefig(
        out_file,
        dpi=dpi,
        bbox_extra_artists=(legend, ),
        bbox_inches='tight'
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-folder', type=str, default="./results",
                        help='The folder where the json results are stored.')
    parser.add_argument('--window-size', '-pws', type=int, default=7,
                        help="Num. of days to plot")
    parser.add_argument('--exp-values', type=list, default=[2, 3, 4],
                        help="Exponential of cost function")
    parser.add_argument('--jobs', '-j', type=int, default=4,
                        help="Num. of concurrent jobs")
    parser.add_argument('--cache-sizes', type=list,
                        default=[1024.**2, 10.*1024.**2],
                        help="List of cache sizes in MBytes (10TB default)")

    args, _ = parser.parse_known_args()

    files = list(sorted(os.listdir(args.result_folder)))

    data_frames = []
    windows = []
    updated_windows = []

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
                data_frames,
                None,
                simple_cost_function,
                args.exp_values,
                counter
            ))
            data_frames = []
            counter += 1

        # TO TEST
        # if file_idx == 7:
        #     break

    if len(data_frames) > 0:
        windows.append((
            data_frames,
            None,
            simple_cost_function,
            counter
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
        windows[idx] = (cur_file, args.cache_sizes, idx)

    for idx in range(len(windows) - 1):
        source = windows[idx][0]
        target = windows[idx+1][0]
        updated_windows.append(
            (
                source,
                target,
                simple_cost_function,
                args.exp_values,
                idx + 1
            )
        )

    for idx, new_df in enumerate(pool.imap(
        parse_data_frames, updated_windows)
    ):
        cur_file = os.path.join(
            args.result_folder, f"window_{idx+1:02d}_updated.feather.gz"
        )
        with gzip.GzipFile(cur_file, mode="wb") as output_file:
            new_df.to_feather(output_file)
        updated_windows[idx] = (
            cur_file, args.cache_sizes, idx + 1
        )

    cache_results = []
    for idx, results in enumerate(pool.imap(
        test_function, windows)
    ):
        cache_results.append(results)
        plot_cache_results(results[0], os.path.join(
            args.result_folder, f"window_{idx:02d}.png"
        ))

    updated_cache_results = []
    for idx, results in enumerate(pool.imap(
        test_function, updated_windows)
    ):
        updated_cache_results.append(results)
        plot_cache_results(results[0], os.path.join(
            args.result_folder, f"window_{idx:02d}_updated.png"
        ))

    for idx in range(1, len(windows)):
        windows[idx] = windows[idx] + (cache_results[idx],)

    for idx in range(1, len(updated_windows)):
        updated_windows[idx] = updated_windows[idx] + (cache_results[idx],)

    cache_results = []
    for idx, results in enumerate(pool.imap(
        test_function, windows[1:]), 1
    ):
        cache_results.append(results)
        plot_cache_results(results[0], os.path.join(
            args.result_folder, f"window_{idx:02d}_hot_cache.png"
        ))

    updated_cache_results = []
    for idx, results in enumerate(pool.imap(
        test_function, updated_windows[1:]), 1
    ):
        updated_cache_results.append(results)
        plot_cache_results(results[0], os.path.join(
            args.result_folder, f"window_{idx:02d}_hot_cache_updated.png"
        ))


if __name__ == "__main__":
    main()
