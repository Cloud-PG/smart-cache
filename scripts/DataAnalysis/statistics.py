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

    def __init__(self, file_size_db_path: str = None, file_size_redis_url: str = None, bar_position: int = 0):
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
                file_size_db_path, "files_mc.db"))
            self.__conn_data = sqlite3.connect(os.path.join(
                file_size_db_path, "files_data.db"))

            self.__cursors = {
                'mc': self.__conn_mc.cursor(),
                'data': self.__conn_data.cursor()
            }
        elif file_size_redis_url:
            self.__redis = redis.Redis(
                connection_pool=redis.BlockingConnectionPool(
                    host=file_size_redis_url,
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
            # print(set_to_add)
            if self.__cursors:
                for db_type in ['mc', 'data']:
                    cur_cursor = self.__cursors[db_type]
                    op = cur_cursor.execute("SELECT * FROM 'file_sizes'")

                    result = op.fetchmany(step)
                    pbar = tqdm(position=self.__bar_position, desc="Get file sizes")
                    while result:
                        for record in result:
                            filename, size = record
                            for set_ in set_to_add:
                                if filename.decode("ascii").find(set_) != -1:
                                    self.__tmp_cache[filename] = float(size)
                            pbar.update(1)
                        result = op.fetchmany(step)
                    pbar.close()
            # print(self.__tmp_cache_set_added)
            self.__tmp_cache_set_added |= set_to_add

    def __flush_buffer(self):
        if self.__buffer:
            if self.__cursors:
                self.__get_file_sizes()
                for record in self.__buffer:
                    filename = record['filename']
                    if filename in self.__tmp_cache:
                        record['size'] = self.__tmp_cache[filename]
            elif self.__redis:
                results = self.__redis.mget(
                    [record['filename'] for record in self.__buffer]
                )
                for idx, result in enumerate(results):
                    if result:
                        self.__buffer[idx]['size'] = float(result)

            new_df = pd.DataFrame(
                self.__buffer,
                columns=self.__columns
            )
            self._data = pd.concat([
                self._data,
                new_df
            ])
            self.__buffer = []

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

        if len(self.__buffer) == 100000:
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
    plt.clf()
    fig, _ = plt.subplots(3, 1, figsize=(8, 8))
    bar_width = 0.4

    axes = plt.subplot(3, 1, 1)
    plt.bar(
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
    plt.bar(
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
    plt.grid()
    plt.legend()
    plt.xlabel("Window")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()

    axes = plt.subplot(3, 1, 2)
    plt.bar(
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
    plt.bar(
        [
            idx + bar_width / 2.
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_req_x_file_g1']
            for record in windows
        ],
        width=bar_width,
        label="Mean Num. Requests > 1"
    )
    axes.set_xticks(range(len(windows)))
    axes.set_xticklabels(
        [str(idx) for idx in range(len(windows))]
    )
    plt.grid()
    plt.legend()
    plt.xlabel("Window")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()

    axes = plt.subplot(3, 1, 3)
    plt.bar(
        [
            idx - bar_width / 2.
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_file_x_job']
            for record in windows
        ],
        width=bar_width,
        label="Mean Num. File x Job"
    )
    plt.bar(
        [
            idx + bar_width / 2.
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_file_x_user']
            for record in windows
        ],
        width=bar_width,
        label="Mean Num. File x User"
    )
    # plt.bar(
    #     [
    #         idx + bar_width / 2.
    #         for idx, _ in enumerate(windows)
    #     ],
    #     [
    #         record['mean_num_job_x_task']
    #         for record in windows
    #     ],
    #     width=bar_width,
    #     label="Mean Num. Job x Task"
    # )
    axes.set_xticks(range(len(windows)))
    axes.set_xticklabels(
        [str(idx) for idx in range(len(windows))]
    )
    plt.grid()
    plt.legend()
    plt.xlabel("Window")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.tight_layout()

    plt.savefig(
        os.path.join(result_folder, "window_stats.png"),
        dpi=dpi
    )
    plt.close(fig)


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
    df = pd.concat(data)

    num_requests = df.shape[0]

    num_files = len(df['filename'].unique().tolist())
    # num_users = len(df['user'].unique().tolist())
    num_sites = len(df['site_name'].unique().tolist())
    num_task_monitors = len(df['task_monitor_id'].unique().tolist())
    num_tasks = len(df['task_id'].unique().tolist())
    num_jobs = len(df['job_id'].unique().tolist())
    num_local = df['protocol'].value_counts().to_dict()

    # mean_num_file_x_job = df['job_id'].value_counts().describe()['mean'] # Alternate method
    # mean_num_file_x_user1 = df['users'].value_counts().describe()['mean']  # alternative method
    mean_num_job_x_task = df[['task_id', 'job_id']].groupby(
        'task_id').size().describe()['mean']  # .apply(lambda x: x.sample(frac=0.3))
    mean_num_file_x_job = df[['filename', 'job_id']].groupby(
        'job_id').size().describe()['mean']
    mean_num_file_x_user = df[['filename', 'users']].groupby(
        'users').size().describe()['mean']

    # print(mean_num_file_x_user, mean_num_file_x_user1)

    print(mean_num_job_x_task)
    assert num_task_monitors == num_tasks
    # print(json.dumps(num_local, indent=2, sort_keys=True))

    num_req_x_file = df['filename'].value_counts()
    # print(num_req_x_file.describe())
    num_req_x_file_g1 = num_req_x_file[
        num_req_x_file > num_req_x_file.describe()['min']]
    # print(num_req_x_file_g1.describe())
    mean_num_req_x_file = num_req_x_file.describe()['mean']
    mean_num_req_x_file_g1 = num_req_x_file_g1.describe()['mean']
    # print(f"MEAN: '{mean_num_req_x_file}'")
    # print(f"MEAN g1: '{mean_num_req_x_file_g1}'")

    def split_filename(filename):
        parts = [part for part in filename.split("/") if part]
        if len(parts) > 1:
            return parts[1]
        else:
            return filename

    print(df['filename'].apply(split_filename).value_counts())

    return {
        'num_requests': num_requests,
        'num_files': num_files,
        'mean_num_req_x_file': mean_num_req_x_file,
        'mean_num_req_x_file_g1': mean_num_req_x_file_g1,
        'mean_num_file_x_job': mean_num_file_x_job,
        'mean_num_file_x_user': mean_num_file_x_user,
        'mean_num_job_x_task': mean_num_job_x_task,
        'mean_num_file_x_job': mean_num_file_x_job,
        # 'num_users': num_users,
        'num_sites': num_sites,
        'num_task_monitors': num_task_monitors,
        'num_tasks': num_tasks,
        'num_jobs': num_jobs,
        'num_local': num_local
    }


def make_stats(input_data):
    (num_process, date, out_folder, minio_config,
     file_size_db_path, file_size_redis_url) = input_data
    year, month, day = date

    minio_client, bucket = create_minio_client(minio_config)
    stats = Statistics(file_size_db_path, file_size_redis_url,
                       bar_position=num_process)

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
        position=num_process
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
    parser.add_argument('--file-size-redis-url', type=str, default=None,
                        help="URL of redis database")

    args, _ = parser.parse_known_args()

    os.makedirs(args.out_folder, exist_ok=True)

    if args.command == "extract":
        if args.minio_config:
            day_list = list(zip(
                [(elm % args.jobs) + 1 for elm in range(0, args.window_size)],
                list(period(args.start_date, args.window_size)),
                [args.out_folder for _ in range(args.window_size)],
                [args.minio_config for _ in range(args.window_size)],
                [args.file_size_db_path for _ in range(args.window_size)],
                [args.file_size_redis_url for _ in range(args.window_size)]
            ))

            pool = Pool(processes=args.jobs)

            pbar = tqdm(total=len(day_list), desc="Extract stats", position=0)
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
        for file_ in tqdm(files, desc="Search stat results", position=0):
            head, tail0 = os.path.splitext(file_)
            _, tail1 = os.path.splitext(head)

            # counter += 1
            # if counter == 3:
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
