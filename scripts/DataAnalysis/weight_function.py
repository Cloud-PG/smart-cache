import argparse
import gzip
import itertools
import os
import pickle
from functools import wraps
from multiprocessing import Pool, current_process
from tempfile import NamedTemporaryFile
from typing import Dict, List, Tuple, Set

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm
from bisect import bisect_left


class WeightedCache(object):

    def __init__(self, max_size: float, exp: float = 2.0,
                 init_state: dict = {}):
        # filename -> size
        self._cache: Dict[str, float] = {}
        # group -> (frequency, file_set)
        self._groups: Dict[str, Tuple(float, Set[str])] = {}
        # filename -> weight
        self._weights: Dict[str, float] = {}
        self.__exp = exp
        self._hit = 0
        self._miss = 0
        self._max_size = max_size
        self.size_history: List[float] = []
        self.hit_rate_history: List[float] = []

        if init_state:
            self.__setstate__(init_state)

    def __repr__(self):
        return f"WeightedCache_{int(self._max_size / 1024**2)}T_{int(self.__exp)}e"

    def __len__(self):
        return len(self._cache)

    def reset_history(self):
        self.size_history = []
        self.hit_rate_history = []
        self._hit = 0
        self._miss = 0

    def reset_weights(self):
        self._groups = {}
        self._weights = {}

        for filename, size in self._cache.items():
            self.update_policy(filename, size, hit=True)

    def clear(self):
        self._cache = {}

    @property
    def state(self):
        return {
            'cache': self._cache,
            'weights': self._weights,
            'groups': self._groups,
            'exp': self.__exp,
            'hit': self._hit,
            'miss': self._miss,
            'max_size': self._max_size,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history
        }

    def __getstate__(self):
        return self.state

    def __setstate__(self, state):
        self._cache = state['cache']
        self._weights = state['weights']
        self._groups = state['groups']
        self.__exp = state['exp']
        self._hit = state['hit']
        self._miss = state['miss']
        self._max_size = state['max_size']
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
        return sum(self._cache.values())

    def check(self, filename):
        return filename in self._cache

    @staticmethod
    def get_group(filename: str):
        _, store_type, campain, process, file_type, _ = [
            part for part in filename.split("/", 6) if part
        ]
        return f"/{store_type}/{campain}/{process}/{file_type}/"

    @staticmethod
    def simple_cost_function(size: float, frequency: float, num_files:
                             float, exp: float = 2.0):
        return size / (frequency / num_files) ** exp

    def get(self, filename, size):
        hit = self.check(filename)
        if hit:
            self._hit += 1
        else:
            self._miss += 1

        self.update_policy(filename, size, hit)

        self.size_history.append(self.size)
        self.hit_rate_history.append(self.hit_rate)

    def update_weights(self, group: str):
        new_group_freq, files_ = self._groups[group]
        num_files = len(files_)
        for filename in files_:
            if filename in self._cache:
                size = self._cache[filename]
                new_weight = self.simple_cost_function(
                    size,
                    new_group_freq,
                    num_files,
                    self.__exp
                )
                self._cache[filename] = size
                self._weights[filename] = new_weight

    def update_policy(self, filename: str, size: float, hit: bool):
        group = self.get_group(filename)

        if group not in self._groups:
            self._groups[group] = (0.0, set())

        frequency, group_files = self._groups[group]
        frequency += 1
        group_files |= set((filename, ))
        self._groups[group] = (frequency, group_files)

        self.update_weights(group)

        if not hit:
            file_weight = self.simple_cost_function(
                size,
                frequency,
                len(group_files),
                self.__exp
            )
            if self.size + size >= self._max_size:
                for filename, weight in sorted(
                    self._weights.items(),
                    key=lambda elm: elm[1],
                    reverse=True
                ):
                    if file_weight < weight:
                        del self._cache[filename]
                        del self._weights[filename]
                    else:
                        break
                    if self.size + size < self._max_size:
                        self._cache[filename] = size
                        self._weights[filename] = file_weight
                        break
            else:
                self._cache[filename] = size
                self._weights[filename] = file_weight


class LRUCache(object):

    def __init__(self, max_size: float, init_state: dict = {}):
        self._cache: List[str] = []
        self._sizes: List[float] = []
        self._hit = 0
        self._miss = 0
        self._max_size = max_size
        self._counters: List[int] = []
        self.__counter = 0
        self.size_history: List[float] = []
        self.hit_rate_history: List[float] = []

        if init_state:
            self.__setstate__(init_state)

    def __repr__(self):
        return f"LRUCache_{int(self._max_size / 1024**2)}T"

    def reset_history(self):
        self.size_history = []
        self.hit_rate_history = []
        self._hit = 0
        self._miss = 0

    def clear(self):
        self.__counter = 0
        self._counters = []
        self._cache = []
        self._sizes = []

    @property
    def state(self):
        return {
            'cache': self._cache,
            'sizes': self._sizes,
            'hit': self._hit,
            'miss': self._miss,
            'max_size': self._max_size,
            'counters': self._counters,
            'counter': self.__counter,
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
        self._max_size = state['max_size']
        self._counters = state['counters']
        self.__counter = state['counter']
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
        self.__counter += 1

        if hit:
            self._counters[self._cache.index(filename)] = self.__counter
        elif self.size + size > self._max_size:
            while self.size + size > self._max_size:
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
def simulate(cache, windows: list, cache_params: Dict[str, bool]):
    process_num = int(
        str(current_process()).split("Worker-")[1].split(",")[0]
    )

    clear_cache = cache_params.get('clear_cache', False)
    clear_weights = cache_params.get('clear_weights', False)

    tmp_size_history = NamedTemporaryFile()
    tmp_hit_rate_history = NamedTemporaryFile()

    for num_window, window in enumerate(windows, 1):
        num_file = 1
        win_pbar = tqdm(
            desc=f"[Open Data Frames][{str(cache)}][Window {num_window}/{len(windows)}][File {num_file}/{len(window)}]",
            position=process_num, ascii=True,
            total=len(window)
        )
        for filename in window:
            with gzip.GzipFile(
                filename, mode="rb"
            ) as stats_file:
                # df = pd.read_feather(stats_file)[['filename', 'size']].dropna()
                df = pd.read_feather(stats_file)
                df = df[
                    df.site_name.str.contains('_it_', case=False)
                ][['filename', 'size']].dropna()

            record_pbar = tqdm(
                total=df.shape[0], position=process_num,
                desc=f"[Simulation][Hit Rate 0.00][{str(cache)}][Window {num_window}/{len(windows)}][File {num_file}/{len(window)}]",
                ascii=True
            )
            for _, record in df.iterrows():
                cache.get(
                    record['filename'],
                    record['size'] / 1024**2  # Convert from Bytes to MegaBytes
                )
                record_pbar.update(1)
                record_pbar.desc = f"[Simulation][{str(cache)}][Hit Rate {cache.hit_rate:0.2f}][Window {num_window}/{len(windows)}][File {num_file}/{len(window)}]"

                # TEST
                # if _ == 10000:
                #     break

            record_pbar.close()

            num_file += 1
            win_pbar.update(1)
            win_pbar.desc = f"[Open Data Frames][{str(cache)}][Window {num_window}/{len(windows)}][File {num_file}/{len(window)}]"
            # TEST
            # if num_file == 2:
            #     break

        cur_size_history, cur_hit_rate_history = zip(*cache.history)
        store_results(tmp_size_history.name, [cur_size_history])
        store_results(tmp_hit_rate_history.name, [cur_hit_rate_history])

        cache.reset_history()

        if clear_cache:
            cache.clear()

        if clear_weights:
            cache.reset_weights()

        win_pbar.close()

    size_history = load_results(tmp_size_history.name)
    hit_rate_history = load_results(tmp_hit_rate_history.name)

    tmp_size_history.close()
    tmp_hit_rate_history.close()

    return (size_history, hit_rate_history)


def store_results(filename: str, data):
    cur_file_name = f"{filename}.gz"
    if not os.path.exists(cur_file_name):
        with gzip.GzipFile(cur_file_name, "wb") as out_file:
            pickle.dump(data, out_file, pickle.HIGHEST_PROTOCOL)
    else:
        with gzip.GzipFile(cur_file_name, "rb") as input_file:
            cur_file = pickle.load(input_file)
        if isinstance(cur_file, (list, tuple)):
            new_data = cur_file + data
        elif isinstance(cur_file, dict):
            cur_file.update(data)
            new_data = cur_file
        with gzip.GzipFile(cur_file_name, "wb") as out_file:
            pickle.dump(new_data, out_file, pickle.HIGHEST_PROTOCOL)


def load_results(filename: str):
    with gzip.GzipFile(f"{filename}.gz", "rb") as input_file:
        data = pickle.load(input_file)
    return data


def plot_cache_results(caches: dict, out_file: str = "simulation_result.png",
                       dpi: int = 300):
    grid = plt.GridSpec(64, 32, wspace=1.42, hspace=1.42)
    styles = itertools.cycle(
        itertools.product(
            (',', '+', '.', 'o', '*'), ('-', '--', '-.', ':')
        )
    )
    markevery = itertools.cycle([50000, 100000, 150000, 200000])
    marker_list = []
    linestyle_list = []
    vertical_lines = []

    pbar = tqdm(desc="Plot results", total=len(caches)*2, ascii=True)

    axes = plt.subplot(grid[0:31, 0:])
    for cache_name, (size_history, hit_rate_history) in caches.items():
        cur_marker, cur_linestyle = next(styles)
        marker_list.append(cur_marker)
        linestyle_list.append(cur_linestyle)
        points = [elm for sublist in size_history for elm in sublist]
        lenght = len(points)
        if not vertical_lines:
            vertical_lines = [
                len(sublist) for sublist in size_history
            ]
        axes.plot(
            range(lenght),
            points,
            label=f"{cache_name}",
            marker=cur_marker,
            markevery=next(markevery),
            linestyle=cur_linestyle,
            alpha=0.9
        )
        axes.set_ylabel("Size (MB)")
        pbar.update(1)
    legend = axes.legend(bbox_to_anchor=(1.0, 1.0))
    axes.grid()
    for vline in vertical_lines:
        axes.axvline(vline, linewidth=0.2, color='k')
    axes.set_xlim(0)
    axes.set_yscale('log')

    axes = plt.subplot(grid[32:, 0:])
    for idx, (cache_name, (size_history, hit_rate_history)
              ) in enumerate(caches.items()):
        points = [elm for sublist in hit_rate_history for elm in sublist]
        lenght = len(points)
        axes.plot(
            range(lenght),
            points,
            label=f"{cache_name}",
            marker=marker_list[idx],
            markevery=next(markevery),
            linestyle=linestyle_list[idx],
            alpha=0.9
        )
        axes.set_ylabel("Hit rate %")
        pbar.update(1)
    axes.grid()
    for vline in vertical_lines:
        axes.axvline(vline, linewidth=0.2, color='k')
    axes.set_ylim(0, 100)
    axes.set_xlim(0)

    axes.set_xlabel("Requests")
    plt.savefig(
        out_file,
        dpi=dpi,
        bbox_extra_artists=(legend, ),
        bbox_inches='tight'
    )
    pbar.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-folder', type=str, default="./results",
                        help='The folder where the json results are stored')
    parser.add_argument('--out-file', type=str,
                        default="simulation_result.png",
                        help='The output plot name.')
    parser.add_argument('--window-size', '-ws', type=int, default=7,
                        help="Num. of days of a window")
    parser.add_argument('--max-windows', '-mw', type=int, default=-1,
                        help="Num. of windows to simulate")
    parser.add_argument('--exp-values', type=List[int],
                        default=[
                            2.,
                            # 3.,
                            # 4.
    ],
        help="Exponential of cost function"
    )
    parser.add_argument('--jobs', '-j', type=int, default=4,
                        help="Num. of concurrent jobs")
    parser.add_argument('--cache-sizes', type=list,
                        default=[
                            # 1024.**2,  # 1T
                            # 10.*1024.**2,  # 10T
                            100.*1024.**2,  # 10T
                        ],
                        help="List of cache sizes in MBytes (10TB default)")

    args, _ = parser.parse_known_args()

    result_files = list(sorted(os.listdir(args.result_folder)))

    files = []
    windows = []
    cache_list = []
    cache_params = []

    for size in args.cache_sizes:
        for clear_cache in [True, False]:
            cache_list.append(LRUCache(size))
            cache_params.append({'clear_cache': clear_cache})
        # TEST
        #     break
        # break

    for size in args.cache_sizes:
        for exp in args.exp_values:
            for clear_cache in [True, False]:
                for clear_weights in [True, False]:
                    cache_list.append(WeightedCache(size, exp))
                    cache_params.append({
                        'clear_cache': clear_cache,
                        'clear_weights': clear_weights
                    })
        # TEST
        #             break
        #         break
        #     break
        # break

    pool = Pool(processes=args.jobs)

    for file_idx, file_ in enumerate(tqdm(
        result_files, desc="Search stat results", ascii=True
    )):
        head, tail0 = os.path.splitext(file_)
        head, tail1 = os.path.splitext(head)

        if tail0 == ".gz" and tail1 == ".feather"\
                and head.find("results_") == 0:
            cur_file = os.path.join(args.result_folder, file_)
            files.append(cur_file)

        if len(files) == args.window_size:
            windows.append(files)
            files = []

        if len(windows) == args.max_windows:
            break

    if len(files) > 0:
        windows.append(files)
        files = []

    for idx, cache_results in enumerate(tqdm(pool.imap(simulate, zip(
        cache_list,
        [windows for _ in range(len(cache_list))],
        cache_params
    )), position=0, total=len(cache_list), desc="Cache simulated", ascii=True)):
        cache_name = f"{str(cache_list[idx])}"
        if cache_params[idx].get('clear_cache', False):
            cache_name += "_cC"
        if cache_params[idx].get('clear_weights', False):
            cache_name += "_cW"
        store_results('cache_results.pickle', {
            cache_name: cache_results
        })

    plot_cache_results(
        load_results('cache_results.pickle'),
        out_file=args.out_file
    )


if __name__ == "__main__":
    main()
