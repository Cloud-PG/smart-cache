import argparse
import gzip
import json
import os
import sqlite3
import warnings
from collections import OrderedDict
from datetime import datetime, timedelta
from functools import lru_cache
from multiprocessing import Pool

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import urllib3
from minio import Minio
from minio.error import (BucketAlreadyExists, BucketAlreadyOwnedByYou,
                         ResponseError)
from tqdm import tqdm

import redis
from DataManager import DataFile, date_from_timestamp_ms


def create_minio_client(minio_config: str):
    cert_reqs = "CERT_NONE"

    if cert_reqs == "CERT_NONE":
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    httpClient = urllib3.PoolManager(
        timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
        cert_reqs=cert_reqs,
        # ca_certs='public.crt',
        retries=urllib3.Retry(
            total=5,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504]
        )
    )

    minio_url, minio_key, minio_secret, bucket = minio_config.split()
    minioClient = Minio(
        minio_url,
        access_key=minio_key,
        secret_key=minio_secret,
        secure=True,
        http_client=httpClient
    )

    return minioClient, bucket


def period(start_date, num_days):
    delta = timedelta(days=1)

    year, month, day = [int(elm) for elm in start_date.split()]
    cur_date = datetime(year, month, day)

    for _ in range(num_days):
        yield (cur_date.year, cur_date.month, cur_date.day)
        cur_date = cur_date+delta


class Statistics(object):

    def __init__(self, file_size_db_path: str = None, redis_url: str = None, bar_position: int = 0):
        self.__columns = [
            'day',
            'filename',
            'protocol',
            'task_monitor_id',
            'task_id',
            'job_id',
            'site_name',
            'job_success',
            'job_length_h',
            'job_length_m',
            'user',
            'cpu_time',
            'io_time',
            'size'
        ]
        self._data = pd.DataFrame(
            columns=self.__columns
        )
        self.__buffer = []
        self.__last_date = None
        self.__cur_date = None
        self.__cursors = None
        self.__conn_mc = None
        self.__conn_data = None
        self.__redis = None
        self.__tmp_cache = {}
        self.__tmp_cache_sets = set()
        self.__tmp_cache_set_added = set()
        self.__bar_position = bar_position

        if file_size_db_path:
            self.__conn_mc = sqlite3.connect(os.path.join(
                file_size_db_path, "mc_file_sizes.db"))
            self.__conn_data = sqlite3.connect(os.path.join(
                file_size_db_path, "data_file_sizes.db"))

            self.__cursors = {
                'mc': self.__conn_mc.cursor(),
                'data': self.__conn_data.cursor()
            }
        if redis_url:
            self.__redis = redis.Redis(
                connection_pool=redis.BlockingConnectionPool(
                    host=redis_url,
                    port=6379, db=0
                )
            )

    def __del__(self):
        if self.__conn_mc:
            self.__conn_mc.close()
        if self.__conn_data:
            self.__conn_data.close()

    @property
    def data(self):
        self.__flush_buffer()
        return self._data

    @staticmethod
    def insert_and_count(dict_: dict, key):
        if key not in dict_:
            dict_[key] = 0
        dict_[key] += 1

    @staticmethod
    def make_a_list(dict_: dict, key, value):
        if key not in dict_:
            dict_[key] = []
        dict_[key].append(value)

    @staticmethod
    def make_a_set(dict_: dict, key, value):
        if key not in dict_:
            dict_[key] = set()
        if not isinstance(dict_[key], set):
            dict_[key] = set(dict_[key])
        dict_[key] |= set((value, ))

    @staticmethod
    def gen_bins(dict_: dict, percentage: bool = True):
        values = dict_.values()
        elements = list(values)
        max_ = max(elements)
        bins = []
        xticks = []
        num_requests = sum(values)

        for num in range(max_):
            counter = elements.count(num)
            if counter > 0:
                bins.append(counter)
                xticks.append(str(num))

        if percentage:
            bins = [float(elm / num_requests) * 100. for elm in bins]

        return bins, xticks

    @staticmethod
    @lru_cache(512)
    def __get_type(string):
        return [elm for elm in string.split("/") if elm][1]

    def __get_file_sizes(self, step: int = 100):
        set_to_add = self.__tmp_cache_sets - self.__tmp_cache_set_added
        if set_to_add:
            query = "SELECT * FROM file_sizes WHERE {}"

            if self.__cursors and not self.__redis:
                for store_type in ['data', 'mc']:
                    cur_cursor = self.__cursors[store_type]
                    sets = [
                        set_
                        for set_ in set_to_add
                        if set_.find(f"/store/{store_type}/") != -1
                    ]

                    if sets:
                        pbar = tqdm(
                            position=self.__bar_position,
                            desc=f"Get {store_type} file sizes of {len(sets)} datasets",
                            ascii=True
                        )

                        ##
                        # NOTE: total check decrease performances
                        #       used only for test
                        #
                        # total = cur_cursor.execute(
                        #     query.format(
                        #         " OR ".join(
                        #             ["f_logical_file_name LIKE ?" for _ in range(
                        #                 len(sets))]
                        #         )
                        #     ).replace("*", "Count(*)"),
                        #     tuple(f'{cur_set}%' for cur_set in sets)
                        # ).fetchone()[0]
                        # pbar.total = total

                        op = cur_cursor.execute(
                            query.format(
                                " OR ".join(
                                    ["f_logical_file_name LIKE ?" for _ in range(
                                        len(sets))]
                                )
                            ) if len(sets) > 1 else query.format(
                                "f_logical_file_name LIKE ?"
                            ),
                            tuple(f'{cur_set}%' for cur_set in sets)if len(
                                sets) > 1 else (f'{sets[0]}%', )
                        )

                        pbar.desc = "Update cache"
                        result = op.fetchmany(step)
                        while result:
                            for record in result:
                                filename, size = record
                                self.__tmp_cache[filename] = size
                                pbar.update(1)
                            result = op.fetchmany(step)
                        pbar.close()

                self.__tmp_cache_set_added |= set_to_add

            elif self.__cursors and self.__redis:
                query = "SELECT * FROM file_sizes WHERE {}"

                # print(f"{self.__bar_position} -> {set_to_add}")
                for store_type in ['data', 'mc']:
                    cur_cursor = self.__cursors[store_type]
                    sets = [
                        set_
                        for set_ in set_to_add
                        if set_.find(f"/store/{store_type}/") != -1
                    ]
                    # print(f"{self.__bar_position} -> {sets}")
                    for cur_set in sets:
                        if not self.__redis.exists(cur_set):
                            self.__redis.set(cur_set, "PENDING")
                            # print(f"{self.__bar_position} -> ADD IN PENDING {cur_set}")
                        else:
                            sets.remove(cur_set)
                            self.__tmp_cache_set_added |= set((cur_set,))

                    # print(f"{self.__bar_position} -> {sets}")
                    if sets:
                        pbar = tqdm(
                            position=self.__bar_position,
                            desc=f"Get {store_type} file sizes of {len(sets)} sets",
                            ascii=True
                        )

                        ##
                        # NOTE: total check decrease performances
                        #       used only for test
                        #
                        # total = cur_cursor.execute(
                        #     query.format(
                        #         " OR ".join(
                        #             ["f_logical_file_name LIKE ?" for _ in range(
                        #                 len(sets))]
                        #         )
                        #     ).replace("*", "Count(*)"),
                        #     tuple(f'{cur_set}%' for cur_set in sets)
                        # ).fetchone()[0]
                        # pbar.total = total

                        op = cur_cursor.execute(
                            query.format(
                                " OR ".join(
                                    ["f_logical_file_name LIKE ?" for _ in range(
                                        len(sets))]
                                )
                            ) if len(sets) > 1 else query.format(
                                "f_logical_file_name LIKE ?"
                            ),
                            tuple(f'{cur_set}%' for cur_set in sets)if len(
                                sets) > 1 else (f'{sets[0]}%', )
                        )

                        pbar.desc = f"Update {store_type} REDIS cache of {len(sets)} sets"
                        result = op.fetchmany(step)
                        while result:
                            self.__redis.mset(dict([
                                (key, value) for key, value in result
                            ]))
                            pbar.update(step)
                            result = op.fetchmany(step)

                        for cur_set in sets:
                            self.__redis.set(cur_set, "ADDED")
                            self.__tmp_cache_set_added |= set((cur_set,))
                            # print(f"{self.__bar_position} -> ADDED {cur_set}")

                        pbar.close()

            return set_to_add

    def __flush_buffer(self):
        if self.__buffer:
            set_to_add = self.__get_file_sizes()
            pbar = tqdm(
                total=len(self.__buffer),
                position=self.__bar_position,
                desc="Inject file sizes",
                ascii=True
            )
            if self.__cursors and not self.__redis:
                for record in self.__buffer:
                    record['size'] = self.__tmp_cache[record['filename']]
                    pbar.update(1)

            elif self.__redis:
                pbar.desc = "Inject file sizes [WAITING]"
                if set_to_add:
                    set_cache = self.__redis.mget(set_to_add)
                    while not all(
                        [
                            set_status.decode("ascii") == "ADDED"
                            for set_status in set_cache
                        ]
                    ):
                        set_cache = self.__redis.mget(set_to_add)
                        pbar.desc = "Inject file sizes [WAITING]"

                pbar.desc = "Inject file sizes"
                results = self.__redis.mget(
                    [record['filename'] for record in self.__buffer]
                )
                for idx, result in enumerate(results):
                    try:
                        self.__buffer[idx]['size'] = float(result)
                    except TypeError:
                        pass
                    pbar.update(1)

            pbar.desc = "Concat dataframe"
            new_df = pd.DataFrame(
                self.__buffer,
                columns=self.__columns
            )
            pbar.update(1)
            self._data = pd.concat([
                self._data,
                new_df
            ])
            pbar.update(1)
            self.__buffer = []
            pbar.update(1)

            pbar.close()

    @staticmethod
    def get_bins(dict_: dict, integer_x: bool = True, to_dict: bool = False):
        if integer_x:
            xticks = [str(elm) for elm in sorted([int(elm) for elm in dict_])]
        else:
            xticks = list(sorted(dict_))

        bins = [dict_[key] for key in xticks]

        if to_dict:
            return OrderedDict(zip(xticks, bins))
        else:
            return bins, xticks

    def add(self, date: tuple, record: dict):
        # print(json.dumps(record, indent=2, sort_keys=True))
        filename = record['FileName']
        user_id = record['UserId']
        site_name = record['SiteName']
        task_monitor_id = record['TaskMonitorId']
        task_id = record['TaskId']
        job_id = record['JobId']
        protocol_type = record['ProtocolUsed']
        exit_code = record['JobExecExitCode']

        num_cores = int(record['NCores'])
        wrap_wc = float(record['WrapWC'])
        wrap_cpu = float(record['WrapCPU'])
        cpu_time = wrap_cpu / num_cores
        io_time = wrap_wc - cpu_time

        job_success = int(exit_code if exit_code else 255) == 0
        job_start = date_from_timestamp_ms(record['StartedRunningTimeStamp'])
        job_end = date_from_timestamp_ms(record['FinishedTimeStamp'])

        delta_h = (job_end - job_start) // timedelta(hours=1)
        delta_m = (job_end - job_start) // timedelta(minutes=1)

        if self.__last_date != date:
            self.__cur_date = datetime(*date).timestamp()
            self.__last_date = date

        self.__buffer.append(
            {
                'day': self.__cur_date,
                'filename': filename,
                'task_monitor_id': task_monitor_id,
                'task_id': task_id,
                'job_id': job_id,
                'site_name': site_name,
                'protocol': protocol_type,
                'job_success': job_success,
                'job_length_h': delta_h,
                'job_length_m': delta_m,
                'user': user_id,
                'cpu_time': cpu_time,
                'io_time': io_time,
                'size': np.nan
            }
        )

        cur_set = "/".join([elm for elm in filename.split("/")][:7])
        if cur_set not in self.__tmp_cache_sets:
            self.__tmp_cache_sets |= set((cur_set, ))

        if len(self.__buffer) == 42000:
            self.__flush_buffer()


def sort_bins(bins, xticks):
    new_bins = []
    new_xticks = []
    for idx, value in sorted(enumerate(bins), key=lambda elm: elm[1], reverse=True):
        new_bins.append(value)
        new_xticks.append(xticks[idx])

    return new_bins, new_xticks


def extract_first(bins, xticks, num):
    new_bins = []
    new_xticks = []
    num_last_bucket = 0
    sum_val_last_bucket = 0
    for idx, value in enumerate(bins):
        if len(new_bins) < num - 1:
            new_bins.append(value)
            new_xticks.append(xticks[idx])
        else:
            num_last_bucket += 1
            sum_val_last_bucket += value

    if num_last_bucket != 0 and len(new_bins) == num - 1:
        new_bins.append(sum_val_last_bucket / num_last_bucket)
        new_xticks.append("Others AVG.")

    return new_bins, new_xticks


def plot_bins(bins, xticks,
              y_label: str, x_label: str,
              figure_num: int, label_step: int = 1,
              y_step: int = -1,
              calc_perc: bool = True,
              ignore_x_step: bool = False,
              ignore_y_step: bool = False,
              sort: bool = False, extract_first_n: int = 0,
              n_cols: int = 2, n_rows: int = 5):
    if sort:
        bins, xticks = sort_bins(bins, xticks)

    if extract_first_n != 0:
        bins, xticks = extract_first(bins, xticks, extract_first_n)

    axes = plt.subplot(n_rows, n_cols, figure_num)
    tot = float(sum(bins))
    plt.bar(
        range(len(bins)),
        [(elm / tot) * 100. for elm in bins] if calc_perc else bins
    )
    axes.set_ylabel(y_label)
    axes.set_xlabel(x_label)

    if y_step == -1:
        y_step = label_step
    if not calc_perc:
        max_bin_perc = int(max(bins)) + y_step + 1
        if ignore_y_step:
            y_range = range(0, max_bin_perc)
        else:
            y_range = range(0, max_bin_perc, y_step)
        axes.set_yticks(y_range)
        axes.set_yticklabels([f"{val}%" for val in y_range])
    else:
        max_bin_perc = int((max(bins) / tot * 100.)) + y_step + 1
        if ignore_y_step:
            y_range = range(0, max_bin_perc)
        else:
            y_range = range(0, max_bin_perc, y_step)
        axes.set_yticks(y_range)
        axes.set_yticklabels([f"{val}%" for val in y_range])

    x_range = range(len(xticks)) if ignore_x_step else range(
        0,
        len(xticks),
        label_step
    )
    axes.set_xticks(x_range)
    axes.set_xticklabels(
        [xticks[idx]for idx in x_range],
        rotation='vertical'
    )
    plt.grid()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()


def merge_stats(dict_list: list):
    tmp = OrderedDict()

    for dict_ in dict_list:
        for key, value in dict_.items():
            if key not in tmp:
                tmp[key] = []
            tmp[key].append(value)

    return tmp


def merge_and_plot_top10(axes, top10_list: list, tot_num_records: int, xlabel: str):
    top10 = merge_stats(top10_list)
    top10 = OrderedDict(
        (key, value)
        for key, value in sorted(
            top10.items(), key=lambda elm: sum(elm[1]),
            reverse=True
        )
    )
    ticks = list(top10.keys())
    bottom_values = [0 for _ in range(len(top10))]
    for cur_index in range(max([len(elm) for elm in top10.values()])):
        values = []
        for value in top10.values():
            try:
                values.append(
                    float(value[cur_index] / tot_num_records)*100.)
            except IndexError:
                values.append(0)
        plt.bar(
            range(len(top10)),
            values,
            bottom=bottom_values
        )
        for idx, value in enumerate(values):
            bottom_values[idx] += value

    plt.grid()
    plt.legend()
    plt.ylabel("%")
    plt.xlabel(xlabel)
    axes.set_xticks(range(len(ticks)))
    axes.set_xticklabels(
        cut_ticks(ticks),
        rotation='vertical'
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()


def plot_global(stats, result_folder, dpi: int = 300):
    pbar = tqdm(total=4, desc=f"Plot global stats")

    days_list = range(len(stats))
    bar_width = 0.1

    plt.clf()
    fig, _ = plt.subplots(3, 2, figsize=(8, 8))

    axes = plt.subplot(3, 2, 1)
    plt.bar(
        [
            day + (idx * bar_width) - bar_width / 2.
            for idx, day in enumerate(days_list)
        ],
        [
            record['num_requests']
            for record in stats.values()
        ],
        width=bar_width,
        label="Num. Requests"
    )
    plt.bar(
        [
            day + (idx * bar_width) + bar_width / 2.
            for idx, day in enumerate(days_list)
        ],
        [
            record['len_file_requests']
            for record in stats.values()
        ],
        width=bar_width,
        label="Num. Files"
    )

    plt.grid()
    plt.legend()
    plt.xlabel("Day")
    axes.set_xticks(days_list)
    axes.set_xticklabels(
        [
            datetime.fromtimestamp(float(day)).strftime("%Y-%m-%d")
            for day in stats
        ],
        rotation='vertical'
    )

    pbar.update(1)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()

    axes = plt.subplot(3, 2, 2)
    plt.bar(
        [
            day + (idx * bar_width) - bar_width
            for idx, day in enumerate(days_list)
        ],
        [record['len_users'] for record in stats.values()],
        width=bar_width,
        label="Num. Unique Users"
    )
    plt.bar(
        [
            day + (idx * bar_width)
            for idx, day in enumerate(days_list)
        ],
        [record['len_sites'] for record in stats.values()],
        width=bar_width,
        label="Num. Unique Sites"
    )
    plt.bar(
        [
            day + (idx * bar_width) + bar_width
            for idx, day in enumerate(days_list)
        ],
        [record['len_tasks'] for record in stats.values()],
        width=bar_width,
        label="Num. Unique Tasks"
    )

    plt.grid()
    plt.legend()
    plt.xlabel("Day")
    axes.set_xticks(days_list)
    axes.set_xticklabels(
        [
            datetime.fromtimestamp(float(day)).strftime("%Y-%m-%d")
            for day in stats
        ],
        rotation='vertical'
    )

    pbar.update(1)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()

    axes = plt.subplot(3, 2, 3)
    plt.bar(
        [
            day + (idx * bar_width)
            for idx, day in enumerate(days_list)
        ],
        [
            (sum(
                elm.count(True)
                for elm in record['job_success'].values()
            ) / record['num_requests']) * 100.
            for record in stats.values()
        ],
        width=bar_width,
        label="Num. successed jobs"
    )
    plt.grid()
    plt.legend()
    plt.ylabel("%")
    plt.xlabel("Day")
    axes.set_xticks(days_list)
    axes.set_xticklabels(
        [
            datetime.fromtimestamp(float(day)).strftime("%Y-%m-%d")
            for day in stats
        ],
        rotation='vertical'
    )

    pbar.update(1)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()

    num_global_requests = sum(
        [record['num_requests'] for record in stats.values()]
    )

    axes = plt.subplot(3, 2, 4)
    merge_and_plot_top10(
        axes,
        [record['top_10_users'] for record in stats.values()],
        num_global_requests,
        "User ID"
    )

    pbar.update(1)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()

    axes = plt.subplot(3, 2, 5)
    merge_and_plot_top10(
        axes,
        [record['top_10_sites'] for record in stats.values()],
        num_global_requests,
        "Site Name"
    )

    pbar.update(1)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()

    axes = plt.subplot(3, 2, 6)
    merge_and_plot_top10(
        axes,
        [record['top_10_tasks'] for record in stats.values()],
        num_global_requests,
        "Task ID"
    )

    pbar.update(1)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()

    plt.savefig(
        os.path.join(result_folder, "global_stats.png"),
        dpi=dpi
    )
    plt.close(fig)

    pbar.update(1)
    pbar.close()


def split_bins(bins, xticks, threshold: float = 0.1):
    start_from = 0
    for idx in range(len(bins) - 2):
        value = bins[idx]
        if float(value - bins[idx+1]) <= float(value * threshold):
            start_from = idx + 1
            break

    return (
        (bins[:start_from], xticks[:start_from]),  # HEAD
        (bins[start_from:], xticks[start_from:])  # TAIL
    )


def cut_ticks(ticks):
    return [
        tick if len(tick) < 11 else tick[:4] + "..." + tick[-4:]
        for tick in ticks
    ]


def plot_day_stats(input_data):
    proc_num, cur_stats = input_data
    pbar = tqdm(
        total=11, desc=f"Plot day {cur_stats['day']}", position=proc_num)

    plt.clf()
    fig, _ = plt.subplots(5, 2, figsize=(8, 12))

    file_request_bins, file_request_ticks = Statistics.gen_bins(
        cur_stats['file_requests'])
    plot_bins(
        file_request_bins, file_request_ticks,
        "%", "Num. Requests x File", 1, label_step=10, calc_perc=False
    )
    pbar.update(1)

    job_length_h_bins, job_length_h_ticks = Statistics.get_bins(
        cur_stats['job_length_h'])
    plot_bins(
        job_length_h_bins, job_length_h_ticks,
        "%", "Job Length (num. Hours)", 2, label_step=10
    )
    pbar.update(1)

    _, (file_request_bins, file_request_ticks) = split_bins(
        file_request_bins, file_request_ticks)
    if file_request_bins:
        plot_bins(
            file_request_bins, file_request_ticks,
            "%", "Num. Requests x File [TAIL]", 3, label_step=10,
            calc_perc=False, y_step=2
        )
    pbar.update(1)

    job_length_h_bin_head, (job_length_h_bins, job_length_h_ticks) = split_bins(
        job_length_h_bins, job_length_h_ticks)
    plot_bins(
        job_length_h_bins, job_length_h_ticks,
        "%", "Job Length (num. Hours) [TAIL]", 4, label_step=10,
        y_step=5
    )
    pbar.update(1)

    job_length_h_head_bins, job_length_h_thead_icks = job_length_h_bin_head
    plot_bins(
        job_length_h_head_bins, job_length_h_thead_icks,
        "%", "Job Length (num. Hours) [HEAD]", 5, label_step=5,
        y_step=10
    )
    pbar.update(1)

    job_length_m_bins, job_length_m_ticks = Statistics.get_bins(
        cur_stats['job_length_m'])
    plot_bins(
        job_length_m_bins, job_length_m_ticks,
        "%", "Job Length (num. Minutes) (top 100)", 6, label_step=10,
        y_step=5, extract_first_n=100
    )
    pbar.update(1)

    protocol_bins, protocol_ticks = Statistics.get_bins(
        cur_stats['protocols'], integer_x=False)
    plot_bins(
        protocol_bins, protocol_ticks,
        "% of Requests", "Protocol Type", 7, label_step=20,
        ignore_x_step=True
    )
    pbar.update(1)

    users_bins, users_ticks = Statistics.get_bins(
        cur_stats['users'], integer_x=False)
    plot_bins(
        users_bins, users_ticks,
        "% of Requests", "User ID (top 10)", 8,
        label_step=10, ignore_x_step=True,
        sort=True, extract_first_n=10
    )
    pbar.update(1)

    sites_bins, sites_ticks = Statistics.get_bins(
        cur_stats['sites'], integer_x=False)
    plot_bins(
        sites_bins, sites_ticks,
        "% of Requests", "Site Name (top 10)", 9,
        label_step=10, ignore_x_step=True,
        sort=True, extract_first_n=10
    )
    pbar.update(1)

    task_bins, task_ticks = Statistics.get_bins(
        cur_stats['tasks'], integer_x=False)
    task_ticks = cut_ticks(task_ticks)
    plot_bins(
        task_bins, task_ticks,
        "% of Requests", "Task ID (top 10)", 10,
        label_step=10, ignore_x_step=True,
        sort=True, extract_first_n=10
    )
    pbar.update(1)

    plt.savefig(
        os.path.join(
            cur_stats['result_folder'],
            f"stats.{cur_stats['day']}.png"
        ),
        dpi=cur_stats['plot_dpi'],
        bbox_inches='tight'
    )
    pbar.update(1)

    plt.close(fig)
    pbar.close()

    return f"stats.{cur_stats['day']}.png"


def plot_windows(windows, result_folder, dpi):
    bar_width = 0.2
    pbar = tqdm(desc="Plot windows", total=10, position=1, ascii=True)

    ############################################################################
    # window_request_stats
    ############################################################################
    plt.clf()
    grid = plt.GridSpec(4, len(windows), wspace=2.42, hspace=4.33)

    axes = plt.subplot(grid[0:2, 0:])
    axes.bar(
        [
            idx - bar_width / 2.
            for idx, _ in enumerate(windows)
        ],
        [
            record['num_requests']
            for record in windows
        ],
        width=bar_width,
        label="Num. Requests"
    )
    axes.bar(
        [
            idx + bar_width / 2.
            for idx, _ in enumerate(windows)
        ],
        [
            record['num_files']
            for record in windows
        ],
        width=bar_width,
        label="Num. Files"
    )
    axes.set_xticks(range(len(windows)))
    axes.set_xticklabels(
        [str(idx) for idx in range(len(windows))]
    )
    axes.grid()
    axes.legend()
    axes.set_xlabel("Window")

    axes = plt.subplot(grid[2:4, :])
    axes.bar(
        [
            idx - bar_width / 2.
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_req_x_file']
            for record in windows
        ],
        width=bar_width,
        label="Mean Num. Requests"
    )
    axes.bar(
        [
            idx + bar_width / 2.
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_req_x_file_gmin']
            for record in windows
        ],
        width=bar_width,
        label="Mean Num. Requests > min"
    )
    axes.set_xticks(range(len(windows)))
    axes.set_xticklabels(
        [str(idx) for idx in range(len(windows))]
    )
    axes.grid()
    axes.legend()
    axes.set_xlabel("Window")
    pbar.update(1)

    plt.savefig(
        os.path.join(result_folder, "window_request_stats.png"),
        dpi=dpi
    )
    pbar.update(1)

    ############################################################################
    # window_frequency_stats
    ############################################################################
    plt.clf()
    grid = plt.GridSpec(8, len(windows), wspace=2.42, hspace=2.33)

    for win_idx, window in enumerate(windows):
        axes = plt.subplot(grid[0:4, win_idx])
        labels = sorted(window['num_req_x_file_frequencies'].keys())
        sizes = [
            (window['num_req_x_file_frequencies'][label] /
             window['num_requests']) * 100.
            for label in labels
        ]
        cut_idx = -1
        for idx, size in enumerate(sizes):
            if size < 2.:
                cut_idx = idx
                break
        labels = labels[:cut_idx] + ['< 2%']
        sizes = sizes[:cut_idx] + [sum(sizes[cut_idx:])]
        axes.pie(sizes, radius=2.8, labels=labels,
                 autopct='%1.0f%%', startangle=90)
        axes.set_xlabel(f"\n\nWindow {win_idx}")

    for win_idx, window in enumerate(windows):
        axes = plt.subplot(grid[4:8, win_idx])
        labels = sorted(window['num_req_x_file_frequencies'].keys())[1:]
        sizes = [
            (window['num_req_x_file_frequencies'][label] /
             window['num_requests']) * 100.
            for label in labels
        ]
        cut_idx = -1
        for idx, size in enumerate(sizes):
            if size < 2.:
                cut_idx = idx
                break
        labels = labels[:cut_idx] + ['< 2%']
        sizes = sizes[:cut_idx] + [sum(sizes[cut_idx:])]
        axes.pie(sizes, radius=2.8, labels=labels,
                 autopct='%1.0f%%', startangle=90)
        axes.set_xlabel(f"\n\nWindow {win_idx}\nWithout 1 request")

    pbar.update(1)
    plt.savefig(
        os.path.join(result_folder, "window_frequency_stats.png"),
        dpi=dpi
    )
    pbar.update(1)

    ############################################################################
    # window_size_stats
    ############################################################################
    plt.clf()
    grid = plt.GridSpec(24, len(windows), wspace=2.42, hspace=5.)

    axes = plt.subplot(grid[0:9, 0:])
    axes.bar(
        [
            idx - (bar_width + bar_width / 2.)
            for idx, _ in enumerate(windows)
        ],
        [
            record['size_all_files']
            for record in windows
        ],
        width=bar_width,
        label="Size all files (GB)"
    )
    axes.bar(
        [
            idx - (bar_width / 2.)
            for idx, _ in enumerate(windows)
        ],
        [
            record['size_file_1req']
            for record in windows
        ],
        width=bar_width,
        label="Size files with 1 request (GB)"
    )
    axes.bar(
        [
            idx + (bar_width / 2.)
            for idx, _ in enumerate(windows)
        ],
        [
            record['size_file_g1req']
            for record in windows
        ],
        width=bar_width,
        label="Size files with more than 1 req. (GB)"
    )
    axes.set_xticks(range(len(windows)))
    axes.set_xticklabels(
        [str(idx) for idx in range(len(windows))]
    )
    axes.grid()
    axes.legend()
    axes.set_xlabel("Window")

    for win_idx, window in enumerate(windows):
        axes = plt.subplot(grid[12:18, win_idx])
        labels = sorted(window['desc_file_sizes'].keys())
        sizes = [
            float(window['desc_file_sizes'][label] /
                  sum(window['desc_file_sizes'].values())) * 100.
            for label in labels
        ]
        # to_remove = []
        # for idx, size in enumerate(sizes):
        #     if size <= 25.:
        #         to_remove.append(idx)
        # for idx in reversed(sorted(to_remove)):
        #     sizes.pop(idx)
        #     labels.pop(idx)
        labels = [
            f"{int(label/1000.)} GB" if label >= 1000. else f"{label} MB"
            for label in labels
        ]
        axes.pie(sizes, radius=2.4, labels=labels,
                 autopct='%1.0f%%', startangle=90)
        axes.set_xlabel(f"\nWin. {win_idx}")

    for win_idx, window in enumerate(windows):
        axes = plt.subplot(grid[19:24, win_idx])
        labels = sorted(window['desc_file_sizes'].keys())
        sizes = [
            float(window['desc_file_sizes'][label] /
                  sum(window['desc_file_sizes'].values())) * 100.
            for label in labels
        ]
        to_remove = []
        for idx, size in enumerate(sizes):
            if size > 25. or size <= 1.:
                to_remove.append(idx)
        for idx in reversed(sorted(to_remove)):
            sizes.pop(idx)
            labels.pop(idx)
        labels = [
            f"{int(label/1000.)} GB" if label >= 1000. else f"{label} MB"
            for label in labels
        ]
        axes.pie(sizes, radius=2.4, labels=labels,
                 autopct='%1.0f%%', startangle=90)
        axes.set_xlabel(f"\nWin. {win_idx}\n1% < size <=25%")

    pbar.update(1)
    plt.savefig(
        os.path.join(result_folder, "window_size_stats.png"),
        dpi=dpi
    )
    pbar.update(1)

    ############################################################################
    # window_cache_size_stats
    ############################################################################
    plt.clf()
    grid = plt.GridSpec(8*len(windows), len(windows), wspace=1.42, hspace=5.)

    for win_idx, window in enumerate(windows):
        start_idx = win_idx*8
        axes = plt.subplot(grid[start_idx:start_idx+7, 0:])
        keys = sorted(window['sizes_x_min_num_requests'].keys())
        axes.bar(
            [key - 1 for key in keys],
            [window['sizes_x_min_num_requests'][key] for key in keys],
            label="Cache size based on min num requests to store. (GB)"
        )
        axes.set_xticks(range(len(keys)))
        axes.set_xticklabels(
            [str(key) for key in keys]
        )
        axes.grid()
        axes.legend()
        axes.set_xlabel(f"Window {win_idx}")

    pbar.update(1)
    plt.savefig(
        os.path.join(result_folder, "window_cache_size_stats.png"),
        dpi=dpi
    )
    pbar.update(1)

    ############################################################################
    # window_task_stats
    ############################################################################
    plt.clf()
    grid = plt.GridSpec(18, len(windows)*2,wspace=1, hspace=1.)

    axes = plt.subplot(grid[0:5, 0:])
    cur_bar_width = bar_width / 3.
    axes.bar(
        [
            idx - cur_bar_width * 2
            for idx, _ in enumerate(windows)
        ],
        [
            record['num_users']
            for record in windows
        ],
        width=cur_bar_width,
        label="Num. users"
    )
    axes.bar(
        [
            idx - cur_bar_width
            for idx, _ in enumerate(windows)
        ],
        [
            record['num_sites']
            for record in windows
        ],
        width=cur_bar_width,
        label="Num. sites"
    )
    axes.bar(
        [
            idx
            for idx, _ in enumerate(windows)
        ],
        [
            record['num_tasks']
            for record in windows
        ],
        width=cur_bar_width,
        label="Num. tasks"
    )
    axes.bar(
        [
            idx + cur_bar_width
            for idx, _ in enumerate(windows)
        ],
        [
            record['num_task_monitors']
            for record in windows
        ],
        width=cur_bar_width,
        label="Num. task monitors"
    )
    axes.bar(
        [
            idx + cur_bar_width * 2
            for idx, _ in enumerate(windows)
        ],
        [
            record['num_jobs']
            for record in windows
        ],
        width=cur_bar_width,
        label="Num. jobs"
    )
    axes.set_yscale('log')
    axes.set_xticks(range(len(windows)))
    axes.set_xticklabels(
        [str(idx) for idx in range(len(windows))]
    )
    axes.grid()
    legend = axes.legend(bbox_to_anchor=(0.42, 2.3))

    for win_idx, window in enumerate(windows):
        axes = plt.subplot(grid[6:8, win_idx*2:win_idx*2+2])
        labels = sorted(window['protocols'].keys())
        sizes = [
            window['protocols'][label]
            for label in labels
        ]
        # to_remove = []
        # for idx, size in enumerate(sizes):
        #     if size <= 25.:
        #         to_remove.append(idx)
        # for idx in reversed(sorted(to_remove)):
        #     sizes.pop(idx)
        #     labels.pop(idx)
        axes.pie(sizes, radius=1.6, labels=labels,
                 autopct='%1.0f%%', startangle=90)
        if win_idx == 0:
            text = axes.text(-14, 0.1, "Protocols %")
        # axes.set_xlabel(f"\nWin. {win_idx}\nprotocols")

    for win_idx, window in enumerate(windows):
        axes = plt.subplot(grid[9:11, win_idx*2:win_idx*2+2])
        labels = ['CPU', 'I/O']
        sizes = [window['all_cpu_time'], window['all_io_time']]
        axes.pie(sizes, radius=1.6, labels=labels,
                 autopct='%1.0f%%', startangle=90)
        if win_idx == 0:
            text = axes.text(-14, 0.1, "Time %")
        # axes.set_xlabel(f"\nWin. {win_idx}\ntime")

    for win_idx, window in enumerate(windows):
        axes = plt.subplot(grid[12:14, win_idx*2:win_idx*2+2])
        labels = ['CPU', 'I/O']
        sizes = [window['local_cpu_time'], window['local_io_time']]
        axes.pie(sizes, radius=1.6, labels=labels,
                 autopct='%1.0f%%', startangle=90)
        if win_idx == 0:
            text = axes.text(-14, 0.1, "Local Time %")
        # axes.set_xlabel(f"\nWin. {win_idx}\ntime (local)")

    for win_idx, window in enumerate(windows):
        axes = plt.subplot(grid[15:17, win_idx*2:win_idx*2+2])
        labels = ['CPU', 'I/O']
        sizes = [window['remote_cpu_time'], window['remote_io_time']]
        axes.pie(sizes, radius=1.6, labels=labels,
                 autopct='%1.0f%%', startangle=90)
        if win_idx == 0:
            text = axes.text(-14, 0.1, "Remote time %")
        # axes.set_xlabel(f"\nWin. {win_idx}\ntime (remote)")

    # with warnings.catch_warnings():
    #     warnings.simplefilter("ignore")
    #     plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    pbar.update(1)
    plt.savefig(
        os.path.join(result_folder, "window_task_stats.png"),
        dpi=dpi,
        bbox_extra_artists=(legend, text),
        bbox_inches='tight'
    )
    pbar.update(1)

    pbar.close()


def transform_sizes(size):
    GB_size=size // 1024**3
    if GB_size == 0.0:
        return size // 1024**2
    return GB_size * 1000.


def make_dataframe_stats(data: list):
    """Plot window stats.

    Data: list of dataframes (df)

        df columns:
            - day
            - filename
            - protocol
            - task_monitor_id
            - task_id
            - job_id
            - site_name
            - job_success
            - job_length_h
            - job_length_m
            - user
            - cpu_time
            - io_time
            - size
    """
    pbar=tqdm(desc = "Make dataframe stats",
              total = 7, position = 1, ascii = True)

    df=pd.concat(data)
    pbar.update(1)

    # Num requests and num files
    num_requests=df.shape[0]
    num_files=len(df['filename'].unique().tolist())
    size_all_files=df[['filename', 'size']].dropna().drop_duplicates(
        subset = 'filename')['size'].sum() / 1024. ** 3.
    pbar.update(1)

    # Mean num. request x file and
    # Mean num. request x file with num requests > min
    num_req_x_file=df['filename'].value_counts()
    num_req_x_file_gmin=num_req_x_file[
        num_req_x_file > num_req_x_file.describe()['min']]
    mean_num_req_x_file= num_req_x_file.describe()['mean']
    mean_num_req_x_file_gmin= num_req_x_file_gmin.describe()['mean']
    pbar.update(1)

    # Num of requests x file
    num_req_x_file_frequencies= num_req_x_file.value_counts().to_dict()
    assert num_requests == sum(
        [key * value for key, value in num_req_x_file_frequencies.items()])
    pbar.update(1)

    # Size files with 1 request and greater than 1 request
    size_by_filename=df[['filename', 'size']].dropna().drop_duplicates(
        subset = 'filename')
    file_1req_list=num_req_x_file[
        num_req_x_file <= num_req_x_file.describe()['min']
    ].keys().to_list()
    file_g1req_list= num_req_x_file[
        num_req_x_file > num_req_x_file.describe()['min']
    ].keys().to_list()
    size_1req_files= size_by_filename[size_by_filename['filename'].isin(
        file_1req_list)]['size'].sum() / 1024. ** 3
    size_g1req_files= size_by_filename[size_by_filename['filename'].isin(
        file_g1req_list)]['size'].sum() / 1024. ** 3
    desc_file_sizes= size_by_filename['size'].apply(
        transform_sizes).value_counts().sort_index().to_dict()
    pbar.update(1)

    sizes_x_min_num_requests={}
    for min_num_request in range(1, 21):
        num_request_filter=num_req_x_file[
            num_req_x_file >= min_num_request
        ].keys().to_list()
        sizes_x_min_num_requests[min_num_request]= size_by_filename[
            size_by_filename['filename'].isin(
                num_request_filter)]['size'].sum() / 1024. ** 3
    pbar.update(1)

    # Task and job stats
    num_users= df['user'].unique().shape[0]
    num_sites= df['site_name'].unique().shape[0]
    num_task_monitors= df['task_monitor_id'].unique().shape[0]
    num_tasks= df['task_id'].unique().shape[0]
    num_jobs= df['job_id'].unique().shape[0]
    protocols= df['protocol'].value_counts().to_dict()
    all_cpu_time= df['cpu_time'].sum()
    all_io_time= df['io_time'].sum()
    local_cpu_time= df['cpu_time'][
        df.protocol == 'Local'
    ].sum()
    local_io_time= df['io_time'][
        df.protocol == 'Local'
    ].sum()
    remote_cpu_time= df['cpu_time'][
        df.protocol == 'Remote'
    ].sum()
    remote_io_time= df['io_time'][
        df.protocol == 'Remote'
    ].sum()
    pbar.update(1)

    # # mean_num_file_x_job = df['job_id'].value_counts().describe()['mean'] # Alternate method
    # # mean_num_file_x_user1 = df['users'].value_counts().describe()['mean']  # alternative method
    # mean_num_job_x_task = df[['task_id', 'job_id']].groupby(
    #     'task_id').size().describe()['mean']  # .apply(lambda x: x.sample(frac=0.3))
    # mean_num_file_x_job = df[['filename', 'job_id']].groupby(
    #     'job_id').size().describe()['mean']
    # mean_num_file_x_user = df[['filename', 'user']].groupby(
    #     'user').size().describe()['mean']

    # # print(mean_num_file_x_user, mean_num_file_x_user1)

    # print(mean_num_job_x_task)
    # assert num_task_monitors == num_tasks
    # # print(json.dumps(num_local, indent=2, sort_keys=True))

    # def split_filename(filename):
    #     parts = [part for part in filename.split("/") if part]
    #     if len(parts) > 1:
    #         return parts[1]
    #     else:
    #         return filename

    # print(df['filename'].apply(split_filename).value_counts())

    pbar.close()
    return {
        'num_requests': num_requests,
        'num_files': num_files,

        'size_all_files': size_all_files,
        'size_file_1req': size_1req_files,
        'size_file_g1req': size_g1req_files,
        'desc_file_sizes': desc_file_sizes,
        'sizes_x_min_num_requests': sizes_x_min_num_requests,

        'mean_num_req_x_file': mean_num_req_x_file,
        'mean_num_req_x_file_gmin': mean_num_req_x_file_gmin,

        'num_req_x_file_frequencies': num_req_x_file_frequencies,

        'num_users': num_users,
        'num_sites': num_sites,
        'num_task_monitors': num_task_monitors,
        'num_tasks': num_tasks,
        'num_jobs': num_jobs,
        'protocols': protocols,
        'all_cpu_time': all_cpu_time,
        'all_io_time': all_io_time,
        'local_cpu_time': local_cpu_time,
        'local_io_time': local_io_time,
        'remote_cpu_time': remote_cpu_time,
        'remote_io_time': remote_io_time,
    }


def make_stats(input_data):
    (num_process, date, out_folder, minio_config,
     file_size_db_path, file_size_redis_url)=input_data
    year, month, day=date

    minio_client, bucket=create_minio_client(minio_config)
    stats=Statistics(file_size_db_path, file_size_redis_url,
                       bar_position = num_process)

    try:
        minio_client.fget_object(
            f"{bucket}",
            f'year{year}_month{month}_day{day}.json.gz',
            os.path.join(out_folder, f"tmp_{year}-{month}-{day}.json.gz")
        )
    except ResponseError:
        raise

    collector = DataFile(os.path.join(
        out_folder, f"tmp_{year}-{month}-{day}.json.gz"))

    # TEST
    # counter = 0
    for record in tqdm(
        collector,
        desc=f"Extract statistics from {year}-{month}-{day}]",
        position=num_process, ascii=True
    ):
        # print(json.dumps(record, indent=2, sort_keys=True))
        # break
        stats.add((year, month, day), record)
        # TEST
        # counter += 1
        # if counter == 1000:
        #     break

    os.remove(os.path.join(out_folder, f"tmp_{year}-{month}-{day}.json.gz"))

    with gzip.GzipFile(
            os.path.join(
                out_folder, f"results_{year}-{month:02}-{day:02}.feather.gz"
            ), mode="wb"
    ) as output_file:
        new_df = stats.data.reset_index()
        new_df.to_feather(output_file)

    return year, month, day


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['extract', 'plot'],
                        help="A string date: \"2019 5 5\"")
    parser.add_argument('--start-date', '-s', type=str,
                        help="A string date: \"2019 5 5\"")
    parser.add_argument('--plot-dpi', type=int, default=300,
                        help="DPI for plot output")
    parser.add_argument('--window-size', '-ws', type=int,
                        help="Num. of days to extract")
    parser.add_argument('--plot-window-size', '-pws', type=int, default=7,
                        help="Num. of days to plot")
    parser.add_argument('--minio-config', '-mcfg', type=str,
                        help='MinIO configuration in the form: "url key secret bucket"')
    parser.add_argument('--result-folder', type=str, default="./results",
                        help='The folder where the json results are stored.')
    parser.add_argument('--out-folder', type=str, default="./results",
                        help='The output folder.')
    parser.add_argument('--jobs', '-j', type=int, default=2,
                        help="Num. of concurrent jobs")
    parser.add_argument('--file-size-db-path', type=str, default=None,
                        help="Path to size database")
    parser.add_argument('--redis-url', type=str, default=None,
                        help="URL of redis database")

    args, _ = parser.parse_known_args()

    if args.command == "extract":
        os.makedirs(args.out_folder, exist_ok=True)

        if args.minio_config:
            day_list = list(zip(
                [(elm % args.jobs) + 1 for elm in range(0, args.window_size)],
                list(period(args.start_date, args.window_size)),
                [args.out_folder for _ in range(args.window_size)],
                [args.minio_config for _ in range(args.window_size)],
                [args.file_size_db_path for _ in range(args.window_size)],
                [args.redis_url for _ in range(args.window_size)]
            ))

            pool = Pool(processes=args.jobs)

            pbar = tqdm(
                total=len(day_list), desc="Extract stats", position=0, ascii=True
            )
            for year, month, day in pool.imap(make_stats, day_list, chunksize=1):
                pbar.write(f"==> [DONE] Date {year}-{month}-{day}")
                pbar.update(1)
            pbar.close()
            pool.close()

            pool.join()

    elif args.command == 'plot':
        files = list(sorted(os.listdir(args.result_folder)))

        pool = Pool(processes=args.jobs)

        data_frames = []
        windows = []

        # TO TEST
        # counter = 0
        for file_ in tqdm(
            files, desc="Search stat results", position=0, ascii=True
        ):
            head, tail0 = os.path.splitext(file_)
            _, tail1 = os.path.splitext(head)

            # counter += 1
            # if counter == 5:
            #     break

            if tail0 == ".gz" and tail1 == ".feather":
                cur_file = os.path.join(args.result_folder, file_)
                with gzip.GzipFile(
                    cur_file, mode="rb"
                ) as stats_file:
                    tqdm.write(f"Open file: '{cur_file}'")
                    data_frames.append(pd.read_feather(stats_file))

            if len(data_frames) == args.plot_window_size:
                tqdm.write("Build window")
                windows.append(make_dataframe_stats(data_frames))
                data_frames = []

        if len(data_frames) > 0:
            windows.append(make_dataframe_stats(data_frames))
            data_frames = []

        print("[Plot window stats...]")
        plot_windows(windows, args.result_folder, args.plot_dpi)
        print("[DONE!]")

    else:
        parser.print_usage()


if __name__ == "__main__":
    main()
