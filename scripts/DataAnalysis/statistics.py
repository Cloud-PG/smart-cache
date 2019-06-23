import argparse
import gzip
import json
import os
import warnings
from collections import OrderedDict
from datetime import datetime, timedelta
from multiprocessing import Pool

import matplotlib.pyplot as plt
import urllib3
from minio import Minio
from minio.error import (BucketAlreadyExists, BucketAlreadyOwnedByYou,
                         ResponseError)
from tqdm import tqdm

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

    def __init__(self):
        self._data = OrderedDict()
        self.__last_date = None
        self.__cur_date = None

    @property
    def data(self):
        return self._data

    @staticmethod
    def insert_and_count(dict_: dict, key):
        if key not in dict_:
            dict_[key] = 0
        dict_[key] += 1

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

    def make_buckets(self, date: tuple):
        if self.__last_date != date:
            self.__cur_date = int(datetime(*date).timestamp())
            self.__last_date = date

    def add(self, date: tuple, record: dict):
        if self.__last_date != date:
            self.__cur_date = int(datetime(*date).timestamp())
            self.__last_date = date

        if self.__cur_date not in self._data:
            self._data[self.__cur_date] = {
                'num_requests': 0,
                'file_requests': OrderedDict(),
                'users': OrderedDict(),
                'user_files': OrderedDict(),
                'sites': OrderedDict(),
                'tasks': OrderedDict(),
                'protocols': OrderedDict(),
                'job_length_h': OrderedDict(),
                'job_length_MxH': OrderedDict(),
                'job_success': False
            }

        cur_obj = self._data[self.__cur_date]

        filename = record['FileName']
        user_id = record['UserId']
        site_name = record['SiteName']
        task_id = record['TaskMonitorId']
        protocol_type = record['ProtocolUsed']

        job_start = record['StartedRunningTimeStamp']
        job_end = record['FinishedTimeStamp']

        job_start = date_from_timestamp_ms(job_start)
        job_end = date_from_timestamp_ms(job_end)
        delta_h = int((job_end - job_start) // timedelta(hours=1))
        delta_m = int((job_end - job_start) // timedelta(minutes=1))

        self.insert_and_count(cur_obj, 'num_requests')
        self.insert_and_count(cur_obj['file_requests'], filename)
        self.insert_and_count(cur_obj['users'], user_id)
        self.make_a_set(cur_obj['user_files'], user_id, filename)
        self.insert_and_count(cur_obj['sites'], site_name)
        self.insert_and_count(cur_obj['tasks'], task_id)
        self.insert_and_count(cur_obj['protocols'], protocol_type)
        self.insert_and_count(cur_obj['job_length_h'], delta_h)
        self.make_a_set(cur_obj['job_length_MxH'], delta_h, delta_m)
        cur_obj['job_success'] = int(record['JobExecExitCode']) == 0

    def to_dict(self):
        for day in self._data.values():
            day['user_files'] = list(day['user_files'])
        return self._data


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
              n_cols: int = 2, n_rows: int = 4):
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
    user_ticks = list(top10.keys())
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
    axes.set_xticks(range(len(user_ticks)))
    axes.set_xticklabels(
        user_ticks,
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
        label="Num. Unique Files"
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
            day + (idx * bar_width) - bar_width
            for idx, day in enumerate(days_list)
        ],
        [
            record['num_requests'] // record['len_users']
            for record in stats.values()
        ],
        width=bar_width,
        label="Avg Num. Requests x User"
    )
    plt.bar(
        [
            day + (idx * bar_width)
            for idx, day in enumerate(days_list)
        ],
        [
            record['num_requests'] // record['len_tasks']
            for record in stats.values()
        ],
        width=bar_width,
        label="Avg Num. Requests x Task"
    )
    plt.bar(
        [
            day + (idx * bar_width) + bar_width
            for idx, day in enumerate(days_list)
        ],
        [
            record['num_requests'] // record['len_sites']
            for record in stats.values()
        ],
        width=bar_width,
        label="Avg Num. Requests x Site"
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

    num_global_requests = sum(
        [record['num_requests']for record in stats.values()]
    )

    axes = plt.subplot(3, 2, 4)
    merge_and_plot_top10(
        axes,
        [record['top_10_users']for record in stats.values()],
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
        [record['top_10_sites']for record in stats.values()],
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
        [record['top_10_tasks']for record in stats.values()],
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


def extract_tail(bins, xticks, threshold: float = 0.05):
    start_from = 0
    for idx in range(len(bins) - 2):
        value = bins[idx]
        if float(value - bins[idx+1]) <= float(value * threshold):
            start_from = idx + 1
            break

    return bins[start_from:], xticks[start_from:]


def plot_day_stats(input_data):
    proc_num, cur_stats = input_data
    pbar = tqdm(
        total=9, desc=f"Plot day {cur_stats['day']}", position=proc_num)

    plt.clf()
    fig, _ = plt.subplots(4, 2, figsize=(8, 8))

    file_request_bins, file_request_ticks = Statistics.gen_bins(
        cur_stats['file_requests'])
    plot_bins(
        file_request_bins, file_request_ticks,
        "%", "Num. Requests x File", 1, label_step=10, calc_perc=False
    )
    pbar.update(1)

    job_length_bins, job_length_ticks = Statistics.get_bins(
        cur_stats['job_length'])
    plot_bins(
        job_length_bins, job_length_ticks,
        "%", "Job Length (num. Hours)", 2, label_step=10
    )
    pbar.update(1)

    file_request_bins, file_request_ticks = extract_tail(
        file_request_bins, file_request_ticks)
    if file_request_bins:
        plot_bins(
            file_request_bins, file_request_ticks,
            "%", "Num. Requests x File [TAIL]", 3, label_step=10,
            calc_perc=False, y_step=2
        )
    pbar.update(1)

    job_length_bins, job_length_ticks = extract_tail(
        job_length_bins, job_length_ticks)
    plot_bins(
        job_length_bins, job_length_ticks,
        "%", "Job Length (num. Hours) [TAIL]", 4, label_step=10,
        y_step=5
    )
    pbar.update(1)

    protocol_bins, protocol_ticks = Statistics.get_bins(
        cur_stats['protocols'], integer_x=False)
    plot_bins(
        protocol_bins, protocol_ticks,
        "% of Requests", "Protocol Type", 5, label_step=20,
        ignore_x_step=True
    )
    pbar.update(1)

    users_bins, users_ticks = Statistics.get_bins(
        cur_stats['users'], integer_x=False)
    plot_bins(
        users_bins, users_ticks,
        "% of Requests", "User ID (top 10)", 6,
        label_step=10, ignore_x_step=True,
        sort=True, extract_first_n=10
    )
    pbar.update(1)

    sites_bins, sites_ticks = Statistics.get_bins(
        cur_stats['sites'], integer_x=False)
    plot_bins(
        sites_bins, sites_ticks,
        "% of Requests", "Site Name (top 10)", 7,
        label_step=10, ignore_x_step=True,
        sort=True, extract_first_n=10
    )
    pbar.update(1)

    task_bins, task_ticks = Statistics.get_bins(
        cur_stats['tasks'], integer_x=False)
    plot_bins(
        task_bins, task_ticks,
        "% of Requests", "Task ID (top 10)", 8,
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


def make_stats(input_data):
    num_process, date, out_folder, minio_config = input_data
    year, month, day = date

    minio_client, bucket = create_minio_client(minio_config)
    stats = Statistics()

    print(f"[Original Data][{year}-{month}-{day}]")
    print("[Original Data][{year}-{month}-{day}][Download...]")
    try:
        minio_client.fget_object(
            f"{bucket}",
            f'year{year}_month{month}_day{day}.json.gz',
            os.path.join(out_folder, f"tmp_{year}-{month}-{day}.json.gz")
        )
    except ResponseError:
        raise
    print("[Original Data][{year}-{month}-{day}][Downloaded]")
    print("[Original Data][{year}-{month}-{day}][Open File]")
    collector = DataFile(os.path.join(
        out_folder, f"tmp_{year}-{month}-{day}.json.gz"))

    # TEST
    # counter = 0
    for record in tqdm(
        collector,
        desc=f"Extract statistics from {year}-{month}-{day}]",
        position=num_process
    ):
        stats.add((year, month, day), record)
        # TEST
        # counter += 1
        # if counter == 10000:
        #     break

    os.remove(os.path.join(out_folder, f"tmp_{year}-{month}-{day}.json.gz"))

    with gzip.GzipFile(
            os.path.join(
                out_folder, f"results_{year}-{month:02}-{day:02}.json.gz"
            ), mode="wb"
    ) as output_file:
        output_file.write(json.dumps(stats.to_dict(), indent=2, sort_keys=True).encode("utf-8"))

    print("[Original Data][{year}-{month}-{day}][Statistics extracted]")

    return date


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
    parser.add_argument('--minio-config', '-mcfg', type=str,
                        help='MinIO configuration in the form: "url key secret bucket"')
    parser.add_argument('--result-folder', type=str, default="./results",
                        help='The folder where the json results are stored.')
    parser.add_argument('--out-folder', type=str, default="./results",
                        help='The output folder.')
    parser.add_argument('--jobs', '-j', type=int, default=2,
                        help="Num. of concurrent jobs")

    args, _ = parser.parse_known_args()

    os.makedirs(args.out_folder, exist_ok=True)

    if args.command == "extract":
        if args.minio_config:
            day_list = list(zip(
                range(1, args.window_size + 1),
                list(period(args.start_date, args.window_size)),
                [args.out_folder for _ in range(args.window_size)],
                [args.minio_config for _ in range(args.window_size)]
            ))

            pool = Pool(processes=args.jobs)

            pbar = tqdm(total=len(day_list), desc="Extract stats", position=0)
            for day in tqdm(pool.imap(make_stats, day_list)):
                pbar.write(f"File {day} done!")
                pbar.update(1)
            pbar.close()

            pool.close()
            pool.join()

    elif args.command == 'plot':
        files = list(sorted(os.listdir(args.result_folder)))
        global_stats = OrderedDict()

        pool = Pool(processes=args.jobs)
        day_stats = []

        for file_ in tqdm(files, desc="Search stat results"):
            _, tail = os.path.splitext(file_)

            if tail == ".json":
                with gzip.GzipFile(
                    os.path.join(args.result_folder, file_), mode="rb"
                ) as stats_file:
                    result = json.loads(stats_file.read())

                for day in result:
                    cur_stats = result[day]
                    if day not in global_stats:
                        # Get stats for top 10
                        top_10_users = Statistics.get_bins(
                            cur_stats['users'], integer_x=False
                        )
                        top_10_sites = Statistics.get_bins(
                            cur_stats['sites'], integer_x=False
                        )
                        top_10_tasks = Statistics.get_bins(
                            cur_stats['tasks'], integer_x=False
                        )

                        # Extract Top 10
                        top_10_users = extract_first(
                            *sort_bins(*top_10_users), 11)
                        top_10_sites = extract_first(
                            *sort_bins(*top_10_sites), 11)
                        top_10_tasks = extract_first(
                            *sort_bins(*top_10_tasks), 11)

                        # Make top 10 dictionary
                        top_10_users = OrderedDict(
                            zip(top_10_users[1][:-1], top_10_users[0][:-1]))
                        top_10_sites = OrderedDict(
                            zip(top_10_sites[1][:-1], top_10_sites[0][:-1]))
                        top_10_tasks = OrderedDict(
                            zip(top_10_tasks[1][:-1], top_10_tasks[0][:-1]))

                        global_stats[day] = {
                            'num_requests': cur_stats['num_requests'],
                            'len_file_requests': len(cur_stats['file_requests']),
                            'len_users': len(cur_stats['users']),
                            'len_tasks': len(cur_stats['tasks']),
                            'len_sites': len(cur_stats['sites']),
                            'top_10_users': top_10_users,
                            'top_10_sites': top_10_sites,
                            'top_10_tasks': top_10_tasks
                        }

                    cur_stats['result_folder'] = args.result_folder
                    cur_stats['plot_dpi'] = args.plot_dpi
                    cur_stats['day'] = day

                    day_stats.append(cur_stats)

        day_stats = list(enumerate(day_stats, 1))

        pbar = tqdm(total=len(day_stats),
                    desc="Create daily plots", position=0)
        for day in tqdm(pool.imap(plot_day_stats, day_stats)):
            pbar.write(f"File {day} done!")
            pbar.update(1)
        pbar.close()

        pool.close()
        pool.join()

        print("[Plot global stats...]")
        plot_global(
            global_stats,
            args.result_folder,
            dpi=args.plot_dpi
        )
        print("[DONE!]")

    else:
        parser.print_usage()


if __name__ == "__main__":
    main()
