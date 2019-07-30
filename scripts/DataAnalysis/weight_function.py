import argparse
import gzip
import itertools
import os
import pickle
from functools import partial, wraps
from multiprocessing import Pool, current_process
from random import seed, shuffle
from tempfile import NamedTemporaryFile
from time import time
from typing import Dict, List, Set, Tuple

import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm


def simple_cost_function(**kwargs) -> float:
    return kwargs['size'] / (
        kwargs['frequency'] / kwargs['num_files']
    ) ** kwargs['exp']


def cost_function_with_time(**kwargs) -> float:
    return (kwargs['size'] / (
        kwargs['frequency'] / kwargs['num_files']
    ) ** kwargs['exp']) * (
        time() - kwargs['last_time']
    ) ** kwargs['exp']


class WeightedCache(object):

    def __init__(self, max_size: float, cost_function: callable,
                 init_state: dict = {}, cache_options: dict = {}):
        # filename -> size
        self._cache: Dict[str, float] = {}
        # group -> (frequency, last_time)
        self._groups: Dict[str, Tuple(float, float)] = {}
        self._groups_files: Dict[str, Set[str]] = {}
        # filename -> weight
        self._weights: Dict[str, float] = {}
        # deferred groups
        self._group2update: List[str] = []
        self._hit = 0
        self._miss = 0
        self._max_size = max_size
        self.size_history: List[float] = []
        self.hit_rate_history: List[float] = []
        self.write_history: List[float] = []
        self.__info = []
        self.cost_function = cost_function
        self.__cache_options = cache_options

        if init_state:
            self.__setstate__(init_state)

    def __repr__(self):
        options = "_".join(
            [
                elm for elm in
                [
                    'cC' if self.__cache_options.get(
                        'clear_cache', False) else '',
                    'cW' if self.__cache_options.get(
                        'clear_weights', False) else ''
                ]
                if elm
            ]
        )
        return "WeightedCache_{}T_{}_f_{}e{}".format(
            int(self._max_size / 1024**2),
            self.__cache_options.get('fun_name', str(
                self.cost_function).split()[1]),
            int(self.__cache_options.get('exp', 2)),
            "_{}".format(
                options
            )
            if options else ''
        )

    def __len__(self):
        return len(self._cache)

    @property
    def clear_me(self):
        return self.__cache_options.get('clear_cache', False)

    @property
    def clear_my_weights(self):
        return self.__cache_options.get('clear_weights', False)

    def reset_weights(self):
        self._groups = {}
        self._weights = {}
        self._group2update = []

        for filename, size in self._cache.items():
            self.update_policy(filename, size, hit=True)

    def clear(self):
        self._cache = {}

    def clear_history(self):
        self.size_history = [self.size_history[-1]]
        self.hit_rate_history = [self.hit_rate_history[-1]]
        self.write_history = [self.write_history[-1]]

    def reset_history(self):
        self.size_history = []
        self.hit_rate_history = []
        self.write_history = []
        self._hit = 0
        self._miss = 0

    @property
    def info(self):
        return self._weights

    @property
    def state(self):
        return {
            'cache': self._cache,
            'groups': self._groups,
            'groups_files': self._groups_files,
            'weights': self._weights,
            'group2update': self._group2update,
            'hit': self._hit,
            'miss': self._miss,
            'max_size': self._max_size,
            'size_history': self.size_history,
            'hit_rate_history': self.hit_rate_history,
            'write_history': self.write_history,
            'cost_function': self.cost_function,
            'cache_options': self.__cache_options,
            'info': self.__info
        }

    def __getstate__(self) -> dict:
        return self.state

    def __setstate__(self, state):
        self._cache = state['cache']
        self._groups = state['groups']
        self._groups_files = state['groups_files']
        self._weights = state['weights']
        self._group2update = state['group2update']
        self._hit = state['hit']
        self._miss = state['miss']
        self._max_size = state['max_size']
        self.size_history = state['size_history']
        self.hit_rate_history = state['hit_rate_history']
        self.write_history = state['write_history']
        self.cost_function = state['cost_function']
        self.__cache_options = state['cache_options']
        self.__info = state['info']

    @property
    def history(self) -> Tuple[List[float]]:
        return (self.size_history, self.hit_rate_history, self.write_history)

    @property
    def hit_rate(self) -> float:
        return float(self._hit / (self._hit + self._miss)) * 100.

    @property
    def size(self) -> float:
        return sum(self._cache.values())

    def update_write_history(self, size: float, added: bool):
        try:
            last = self.write_history[-1]
        except IndexError:
            last = 0.0
        if not added:
            self.write_history.append(last)
        else:
            self.write_history.append(last + size)

    def check(self, filename: str) -> bool:
        return filename in self._cache

    @staticmethod
    def get_group(filename: str) -> str:
        _, store_type, campain, process, file_type, _ = [
            part for part in filename.split("/", 6) if part
        ]
        return f"/{store_type}/{campain}/{process}/{file_type}/"

    def get(self, filename: str, size: float) -> bool:
        hit = self.check(filename)
        if hit:
            self._hit += 1
        else:
            self._miss += 1

        added = self.update_policy(filename, size, hit)

        self.size_history.append(self.size)
        self.hit_rate_history.append(self.hit_rate)
        self.update_write_history(size, added)

        return hit

    def update_weights(self, group: str):
        new_group_freq, last_time = self._groups[group]
        files = self._groups_files[group]
        num_files = len(files)
        for filename in files:
            if filename in self._cache:
                size = self._cache[filename]
                new_weight = self.cost_function(
                    size=size,
                    frequency=new_group_freq,
                    num_files=num_files,
                    last_time=last_time
                )
                self._cache[filename] = size
                self._weights[filename] = new_weight
            elif filename in self._weights:
                del self._weights[filename]

    def update_policy(self, filename: str, size: float, hit: bool) -> bool:
        group = self.get_group(filename)

        if group not in self._groups:
            self._groups[group] = (0.0, 0.0)
            self._groups_files[group] = set()

        frequency, _ = self._groups[group]
        frequency += 1
        last_time = time()
        self._groups[group] = (frequency, last_time)
        self._groups_files[group] |= set((filename, ))

        self._group2update.append(group)

        if not hit:
            file_weight = self.cost_function(
                size=size,
                frequency=frequency,
                num_files=len(self._groups_files[group]),
                last_time=last_time
            )
            if self.size + size >= self._max_size:
                for _ in range(len(self._group2update)):
                    self.update_weights(self._group2update.pop())

                for filename, weight in sorted(
                    self._weights.items(),
                    key=lambda elm: elm[1],
                    reverse=True
                ):
                    if file_weight < weight:
                        if filename in self._cache:
                            del self._cache[filename]
                        del self._weights[filename]
                    else:
                        break
                    if self.size + size < self._max_size:
                        self._cache[filename] = size
                        self._weights[filename] = file_weight
                        return True
            else:
                self._cache[filename] = size
                self._weights[filename] = file_weight
                return True

        return False


class LRUCache(object):

    def __init__(self, max_size: float, cache_options: dict = {},
                 init_state: dict = {}):
        self._cache: List[str] = []
        self._sizes: List[float] = []
        self._hit = 0
        self._miss = 0
        self._max_size = max_size
        self._counters: List[int] = []
        self.__counter = 0
        self.size_history: List[float] = []
        self.hit_rate_history: List[float] = []
        self.write_history: List[float] = []
        self.__cache_options = cache_options
        self.__info = []

        if init_state:
            self.__setstate__(init_state)

    def __repr__(self):
        return "LRUCache_{}T{}".format(
            int(self._max_size / 1024**2),
            "_cC" if self.__cache_options.get('clear_cache', False) else ''
        )

    def __len__(self) -> int:
        return len(self._cache)

    @property
    def clear_me(self):
        return self.__cache_options.get('clear_cache', False)

    @property
    def clear_my_weights(self):
        return False

    @property
    def info(self):
        return dict(zip(self._cache, self._counters))

    def clear(self):
        self.__counter = 0
        self._counters = []
        self._cache = []
        self._sizes = []

    def clear_history(self):
        self.size_history = [self.size_history[-1]]
        self.hit_rate_history = [self.hit_rate_history[-1]]
        self.write_history = [self.write_history[-1]]

    def reset_history(self):
        self.size_history = []
        self.hit_rate_history = []
        self.write_history = []
        self._hit = 0
        self._miss = 0

    def update_write_history(self, size: float, added: bool):
        try:
            last = self.write_history[-1]
        except IndexError:
            last = 0.0
        if not added:
            self.write_history.append(last)
        else:
            self.write_history.append(last + size)

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
            'hit_rate_history': self.hit_rate_history,
            'write_history': self.write_history,
            'cache_options': self.__cache_options,
            'info': self.__info
        }

    def __getstate__(self) -> dict:
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
        self.write_history = state['write_history']
        self.__cache_options = state['cache_options']
        self.__info = state['info']

    @property
    def history(self) -> Tuple[List[float]]:
        return (self.size_history, self.hit_rate_history, self.write_history)

    @property
    def hit_rate(self) -> float:
        return float(self._hit / (self._hit + self._miss)) * 100.

    @property
    def size(self) -> float:
        return sum(self._sizes)

    def check(self, filename: str) -> bool:
        return filename in self._cache

    def get(self, filename: str, size: float) -> bool:
        hit = self.check(filename)
        if hit:
            self._hit += 1
        else:
            self._miss += 1

        added = self.update_policy(filename, size, hit)

        self.size_history.append(self.size)
        self.hit_rate_history.append(self.hit_rate)
        self.update_write_history(size, added)

        return hit

    def update_policy(self, filename: str, size: float, hit: bool) -> bool:
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
            return True
        else:
            self._cache.append(filename)
            self._sizes.append(size)
            self._counters.append(self.__counter)
            return True

        return False


def star_decorator(func):
    @wraps(func)
    def star_wrapper(inputs):
        return func(*inputs)
    return star_wrapper


@star_decorator
def simulate(cache, windows: list, region: str = "_all_"):
    process_num = int(
        str(current_process()).split("Worker-")[1].split(",")[0]
    )

    tmp_size_history = NamedTemporaryFile()
    tmp_hit_rate_history = NamedTemporaryFile()
    tmp_write_history = NamedTemporaryFile()
    tmp_info = NamedTemporaryFile()

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
                df = pd.read_feather(stats_file)
                if region != "_all_":
                    df = df[
                        df.site_name.str.contains(region, case=False)
                    ][['filename', 'size']].dropna().reset_index()
                else:
                    df = df[['filename', 'size']].dropna().reset_index()

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

        cur_size_history, cur_hit_rate_history, cur_write_history = cache.history
        store_results(tmp_size_history.name, [cur_size_history])
        store_results(tmp_hit_rate_history.name, [cur_hit_rate_history])
        store_results(tmp_write_history.name, [cur_write_history])
        store_results(tmp_info.name, [cache.info])

        cache.clear_history()

        if cache.clear_me:
            cache.clear()

        if cache.clear_my_weights:
            cache.reset_weights()

        win_pbar.close()

    size_history = load_results(tmp_size_history.name)
    hit_rate_history = load_results(tmp_hit_rate_history.name)
    write_history = load_results(tmp_write_history.name)
    cache_info = load_results(tmp_info.name)

    tmp_size_history.close()
    tmp_hit_rate_history.close()
    tmp_write_history.close()
    tmp_info.close()

    return (size_history, hit_rate_history, write_history, cache_info)


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
    _, ext = os.path.splitext(filename)
    if ext == ".gz":
        target = filename
    else:
        target = f"{filename}.gz"
    with gzip.GzipFile(target, "rb") as input_file:
        data = pickle.load(input_file)
    return data


def plot_cache_results(caches: dict, out_folder: str, dpi: int = 300):
    grid = plt.GridSpec(96, 32, wspace=2.42, hspace=1.42)
    styles_list = list(itertools.product(
        ('+', '*', '.', 'o', ','), ('-', '--', '-.', ':')
    ))
    seed(42)
    shuffle(styles_list)
    styles = itertools.cycle(styles_list)
    markevery = itertools.cycle([50000, 100000, 150000, 200000])
    cache_styles = {}
    vertical_lines = []

    pbar = tqdm(desc="Plot results", total=len(caches)*3, ascii=True)

    axes = plt.subplot(grid[0:31, 0:])
    for cache_name, (size_history, hit_rate_history, write_history, _) in caches.items():
        cur_marker, cur_linestyle = next(styles)
        cache_styles[cache_name] = (cur_marker, cur_linestyle)
        points = [elm for sublist in size_history for elm in sublist]
        lenght = len(points)
        if not vertical_lines:
            vertical_lines = [
                len(sublist) for sublist in size_history
            ]
            for v_idx in range(1, len(vertical_lines)):
                vertical_lines[v_idx] = vertical_lines[v_idx] + \
                    vertical_lines[v_idx-1]
        axes.plot(
            range(lenght),
            points,
            label=f"{cache_name}",
            marker=cur_marker,
            markevery=next(markevery),
            linestyle=cur_linestyle,
            # alpha=0.9
        )
        axes.set_ylabel("Size (MB)")
        pbar.update(1)
    legend = axes.legend(bbox_to_anchor=(1.0, 1.0))
    axes.grid()
    for vline in vertical_lines:
        axes.axvline(vline, linewidth=0.9, color='k')
    axes.set_xlim(0)
    axes.set_yscale('log')
    axes.set_xticklabels([])

    axes = plt.subplot(grid[32:63, 0:])
    for cache_name, (size_history, hit_rate_history, write_history, _) in caches.items():
        cur_marker, cur_linestyle = cache_styles[cache_name]
        points = [elm for sublist in write_history for elm in sublist]
        lenght = len(points)
        axes.plot(
            range(lenght),
            points,
            label=f"{cache_name}",
            marker=cur_marker,
            markevery=next(markevery),
            linestyle=cur_linestyle,
            # alpha=0.9
        )
        axes.set_ylabel("MB Written")
        pbar.update(1)
    axes.grid()
    for vline in vertical_lines:
        axes.axvline(vline, linewidth=0.9, color='k')
    axes.set_xlim(0)
    axes.set_yscale('log')
    axes.set_xticklabels([])

    axes = plt.subplot(grid[64:, 0:])
    for cache_name, (size_history, hit_rate_history, write_history, _) in caches.items():
        cur_marker, cur_linestyle = cache_styles[cache_name]
        points = [elm for sublist in hit_rate_history for elm in sublist]
        lenght = len(points)
        axes.plot(
            range(lenght),
            points,
            label=f"{cache_name}",
            marker=cur_marker,
            markevery=next(markevery),
            linestyle=cur_linestyle,
            # alpha=0.9
        )
        axes.set_ylabel("Hit rate %")
        pbar.update(1)
    axes.grid()
    for vline in vertical_lines:
        axes.axvline(vline, linewidth=0.9, color='k')
    axes.set_ylim(0, 100)
    axes.set_xlim(0)
    axes.set_xlabel("Requests")

    plt.savefig(
        os.path.join(out_folder, "simulation_result.png"),
        dpi=dpi,
        bbox_extra_artists=(legend, ),
        bbox_inches='tight'
    )

    plt.cla()

    no_LRU_info = [(name, info) for name, info in caches.items()
                   if name.find("LRU") == -1]
    grid = plt.GridSpec(16*len(no_LRU_info), 16 *
                        len(vertical_lines), wspace=1.42, hspace=1.42)

    all_texts = []
    for lru_name, (_, _, _, lru_info) in [
        (name, data) for name, data in caches.items()
            if name.find("LRU") != -1]:
        for cache_idx, (cur_name, (_, _, _, cur_info)) in enumerate(no_LRU_info):
            for window_idx, cur_results in enumerate(cur_info):
                axes = plt.subplot(
                    grid[
                        16*cache_idx:16*cache_idx+15,
                        16*window_idx:16*window_idx+15
                    ]
                )
                # if window_idx == 0:
                #     all_texts.append(axes.text(0.0, 0.1, f"{cur_name}"))
                files = [(filename, weight) for filename, weight in sorted(
                    cur_results.items(),
                    key=lambda elm: elm[1],
                    reverse=True
                )]
                for file_idx, (filename, weight) in enumerate(files):
                    axes.bar(
                        file_idx,
                        weight,
                        color='b' if filename in lru_info[window_idx] else 'r',
                        width=1.0,
                        label="Weight"
                    )
                axes.set_ylim(0)
                axes.set_xlim(0)
                axes.set_xticklabels([])
                axes.grid()

        plt.savefig(
            os.path.join(out_folder, f"simulation_result_info_{lru_name}.png"),
            dpi=dpi,
            bbox_extra_artists=all_texts,
            bbox_inches='tight'
        )

    pbar.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-folder', type=str, default="./results",
                        help='The folder where the json results are stored')
    parser.add_argument('--out-folder', type=str,
                        default="./sim_res",
                        help='The output plot name.')
    parser.add_argument('--region', type=str, default="it",
                        help='Region to filter.')
    parser.add_argument('--plot-results', type=str, default="",
                        help='File with results to plot.')
    parser.add_argument('--window-size', '-ws', type=int, default=7,
                        help="Num. of days of a window")
    parser.add_argument('--max-windows', '-mw', type=int, default=-1,
                        help="Num. of windows to simulate")
    parser.add_argument('--exp-values', type=str,
                        default="2,4",
                        help="Exponential of cost function. List divided by ','."
                        )
    parser.add_argument('--jobs', '-j', type=int, default=4,
                        help="Num. of concurrent jobs")
    parser.add_argument('--functions', type=str,
                        default="simple,with_time",
                        help="List of functions to test. List divided by ','.")
    parser.add_argument('--cache-sizes', type=str,
                        default="10485760,104857600",  # 10T and 100T
                        help="List of cache sizes in MBytes (10TB default). List divided by ','.")
    parser.add_argument('--clear-cache', '-cC', action='store_true',
                        help="Clear the cache on next window")
    parser.add_argument('--clear-weights', '-cW', action='store_true',
                        help="Clear the weights on next window")

    args, _ = parser.parse_known_args()
    args.exp_values = [float(elm) for elm in args.exp_values.split(",")]
    args.functions = [elm for elm in args.functions.split(",")]
    args.cache_sizes = [float(elm) for elm in args.cache_sizes.split(",")]

    if args.plot_results == "":
        result_files = list(sorted(os.listdir(args.result_folder)))

        files = []
        windows = []
        cache_list = []
        clear_cache_list = [False] if not args.clear_cache else [False, True]
        clear_weights_list = [
            False] if not args.clear_weights else [False, True]
        cost_functions = {
            'simple': simple_cost_function,
            'with_time': cost_function_with_time
        }

        for size in args.cache_sizes:
            for clear_cache in clear_cache_list:
                cache_list.append(
                    LRUCache(
                        size,
                        cache_options={
                            'clear_cache': clear_cache,
                        }
                    )
                )
        # TEST
        #     break
        # break

        for fun_name, function in [(fun_name, function)
                                   for fun_name, function in cost_functions.items()
                                   if fun_name in args.functions]:
            for size in args.cache_sizes:
                for exp in args.exp_values:
                    for clear_cache in clear_cache_list:
                        for clear_weights in clear_weights_list:
                            cache_list.append(
                                WeightedCache(
                                    size,
                                    partial(function,
                                            exp=exp),
                                    cache_options={
                                        'fun_name': fun_name,
                                        'exp': exp,
                                        'clear_cache': clear_cache,
                                        'clear_weights': clear_weights
                                    }
                                )
                            )
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
                files = []
                break

        if len(files) > 0:
            windows.append(files)
            files = []

        for idx, cache_results in enumerate(tqdm(pool.imap(simulate, zip(
            cache_list,
            [windows for _ in range(len(cache_list))],
            [f"_{args.region}_" for _ in range(len(cache_list))]
        )), position=0, total=len(cache_list), desc="Cache simulated", ascii=True)):
            cache_name = f"{str(cache_list[idx])}"
            store_results(f'cache_results_{id(pool)}.pickle', {
                cache_name: cache_results
            })

        os.makedirs(args.out_folder, exist_ok=True)
        plot_cache_results(
            load_results(f'cache_results_{id(pool)}.pickle'),
            out_folder=args.out_folder
        )

        pool.close()
        pool.join()

    else:
        os.makedirs(args.out_folder, exist_ok=True)
        plot_cache_results(
            load_results(args.plot_results),
            out_folder=args.out_folder
        )


if __name__ == "__main__":
    main()
