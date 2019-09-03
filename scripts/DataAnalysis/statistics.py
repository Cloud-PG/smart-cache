import argparse
import gzip
import os
import sqlite3
from datetime import datetime, timedelta
from functools import wraps
from itertools import cycle
from multiprocessing import Pool

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import redis
import urllib3
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, FactorRange, LabelSet, Span
from bokeh.palettes import Category10, Accent
from bokeh.plotting import figure, output_file, save
from bokeh.transform import cumsum, factor_cmap
from minio import Minio
from minio.error import ResponseError
from tqdm import tqdm

from DataManager import DataFile, date_from_timestamp_ms

COLORS = cycle(Accent[6])


def create_minio_client(minio_config: str):
    """Prepare a minio client.

    Args:
        minio_config (str): the minio configuration in the following format:
                            f"{minio_url} {minio_key} {minio_secret} {bucket}"

    Returns
        tuple (minio client, bucket name)
    """
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


def period(start_date: str, num_days: int):
    """Generate a period of time.

    Args:
        start_date (str): a date string in the format f"{year} {month} {day}"
        num_days (int): number of the period days

    Returns:
        generator
    """
    delta = timedelta(days=1)

    year, month, day = [int(elm) for elm in start_date.split()]
    cur_date = datetime(year, month, day)

    for _ in range(num_days):
        yield (cur_date.year, cur_date.month, cur_date.day)
        cur_date = cur_date+delta


class Statistics(object):

    """Object that make statistics of a day."""

    def __init__(self, file_size_db_path: str = None, redis_url: str = None,
                 bar_position: int = 0):
        """Prepare the environment for statistic extraction.

        Args:
            file_size_db_path (str): the folder path to file size database in 
                                     sqlite
            redis_url (str): the redis cache url (ex: 'localhost')
            bar_position (int): position for progress bar
        """
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

        # SQLite databases are used as source for file sizes
        if file_size_db_path:
            self.__conn_mc = sqlite3.connect(os.path.join(
                file_size_db_path, "mc_file_sizes.db"))
            self.__conn_data = sqlite3.connect(os.path.join(
                file_size_db_path, "data_file_sizes.db"))

            self.__cursors = {
                'mc': self.__conn_mc.cursor(),
                'data': self.__conn_data.cursor()
            }
        # Redis cache is used to speedup the extraction
        if redis_url:
            self.__redis = redis.Redis(
                connection_pool=redis.BlockingConnectionPool(
                    host=redis_url,
                    port=6379, db=0
                )
            )

    def __del__(self):
        """Close SQLite connections."""
        if self.__conn_mc:
            self.__conn_mc.close()
        if self.__conn_data:
            self.__conn_data.close()

    @property
    def data(self):
        """The statistic data.

        Returns:
            DataFrame
        """
        self.__flush_buffer()
        return self._data

    def __get_file_sizes(self, step: int = 100):
        """Update a local cache with sizes of all set files.

        Args:
            step (int): stride of the extraction from SQLite

        Returns:
            list or None: the list of sets to add or None

        Note:
            Because extraction is a multiprocessing task, if
            Redis is not available each process uses a local cache (a
            Python dictionary), otherwise, Redis will serve as a
            global cache and each process can access it. This
            second option is faster because the cache with the file
            sizes is populated by all the processes.
        """
        set_to_add = self.__tmp_cache_sets - self.__tmp_cache_set_added
        if set_to_add:
            query = "SELECT * FROM file_sizes WHERE {}"

            # If only SQLite is available it creates a local cache
            # with a Python dictionary named '__tmp_cache'
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

            # If Redis is available it uses Redis as cache system
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
        """Update the data frame with the buffered records."""
        if self.__buffer:
            set_to_add = self.__get_file_sizes()
            pbar = tqdm(
                total=len(self.__buffer),
                position=self.__bar_position,
                desc="Inject file sizes",
                ascii=True
            )
            # Update using local cache
            if self.__cursors and not self.__redis:
                for record in self.__buffer:
                    record['size'] = self.__tmp_cache.get(
                        record['filename'], np.nan)
                    pbar.update(1)
            # Update using Redis cache
            elif self.__redis:
                pbar.desc = "Inject file sizes [WAITING]"
                # Check if all sets are already in cache
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
                # Add file sizes
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

    def add(self, date: tuple, record: dict):
        """Extract and add a record statistics.

        Args:
            date (tuple): the date of the record
            record (dict): all record attributes

        """
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

        # Check and extract file set. It will be used to add file sizes
        # Example: /store/type/campain/.../set/filename.root ->
        #          /store/type/campain/.../set
        cur_set = "/".join([elm for elm in filename.split("/")][:7])
        if cur_set not in self.__tmp_cache_sets:
            self.__tmp_cache_sets |= set((cur_set, ))

        # Flush the current buffer to transform all data in DataFrame
        if len(self.__buffer) == 42000:
            self.__flush_buffer()


def plot_windows(windows: list, result_folder: str, dpi: int):
    """Plot and save the extracted windows.

    Args:
        windows (list): the list of window statistics
        result_folder (str): destination folder name
        dpi (int): resolution of the output plot
    """
    output_file(
        os.path.join(result_folder, "statistics.html"),
        "Statistics",
        mode="inline"
    )

    bar_width = 0.2
    pbar = tqdm(desc="Plot windows", total=10, ascii=True)

    ###########################################################################
    # num req and num files
    ###########################################################################
    cur_windows = [f"Window {idx}" for idx, _ in enumerate(windows)]
    p0_cur_types = ['Num. Requests', 'Num. Files']
    p0_cur_data = {
        'windows': cur_windows,
        'Num. Requests': [window['num_requests'] for window in windows],
        'Num. Files': [window['num_files'] for window in windows],
    }
    p0_cur_palette = [next(COLORS) for _ in range(len(p0_cur_types))]

    p0_x = [
        (window, type_)
        for window in cur_windows
        for type_ in p0_cur_types
    ]
    p0_counts = sum(zip(*[p0_cur_data[name] for name in p0_cur_types]), ())

    p0_source = ColumnDataSource(data=dict(x=p0_x, counts=p0_counts))

    fig_num_req_num_files = figure(
        x_range=FactorRange(*p0_x),
        plot_height=320,
        title="Req. and file counts",
        tools="box_zoom,pan,reset,save",
    )

    fig_num_req_num_files.vbar(
        x='x', top='counts', width=1.0,
        source=p0_source, line_color="white",
        fill_color=factor_cmap(
            'x', palette=p0_cur_palette,
            factors=p0_cur_types,
            start=1,
            end=2
        ))

    fig_num_req_num_files.y_range.start = 0
    fig_num_req_num_files.x_range.range_padding = 0.1
    fig_num_req_num_files.xaxis.major_label_orientation = 1
    fig_num_req_num_files.xgrid.grid_line_color = None

    ###########################################################################
    # Mean num req and num files
    ###########################################################################

    p1_cur_types = ['Num. Request x file', 'Num. Request x file (req > 1)']
    p1_cur_data = {
        'windows': cur_windows,
        'Num. Request x file': [window['mean_num_req_x_file'] for window in windows],
        'Num. Request x file (req > 1)': [window['mean_num_req_x_file_gmin'] for window in windows],
    }
    p1_cur_palette = [next(COLORS) for _ in range(len(p1_cur_types))]

    p1_x = [
        (window, type_)
        for window in cur_windows
        for type_ in p1_cur_types
    ]
    p1_counts = sum(zip(*[p1_cur_data[name] for name in p1_cur_types]), ())

    p1_source = ColumnDataSource(data=dict(x=p1_x, counts=p1_counts))

    fig_mean_num_req_num_files = figure(
        x_range=FactorRange(*p1_x),
        plot_height=320,
        title="Mean Req. and file counts",
        tools="box_zoom,pan,reset,save",
    )

    fig_mean_num_req_num_files.vbar(
        x='x', top='counts', width=1.0,
        source=p1_source, line_color="white",
        fill_color=factor_cmap(
            'x', palette=p1_cur_palette,
            factors=p1_cur_types,
            start=1,
            end=2
        ))

    fig_mean_num_req_num_files.y_range.start = 0
    fig_mean_num_req_num_files.x_range.range_padding = 0.1
    fig_mean_num_req_num_files.xaxis.major_label_orientation = 1
    fig_mean_num_req_num_files.xgrid.grid_line_color = None

    save(column(fig_num_req_num_files, fig_mean_num_req_num_files))

    return
    ###########################################################################
    # window_request_stats
    ###########################################################################
    plt.clf()
    grid = plt.GridSpec(24, len(windows), wspace=2.42, hspace=4.33)

    axes = plt.subplot(grid[0:7, 0:])
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

    axes = plt.subplot(grid[8:15, :])
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

    axes = plt.subplot(grid[16:24, :])
    cur_bar_width = bar_width / 4.
    axes.bar(
        [
            idx - cur_bar_width * 3
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_files_x_job']
            for record in windows
        ],
        width=cur_bar_width,
        label="Mean Num. Files x Job"
    )
    axes.bar(
        [
            idx - cur_bar_width * 2
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_files_x_task']
            for record in windows
        ],
        width=cur_bar_width,
        label="Mean Num. Files x Task"
    )
    axes.bar(
        [
            idx - cur_bar_width
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_files_x_user']
            for record in windows
        ],
        width=cur_bar_width,
        label="Mean Num. Files x User"
    )
    axes.bar(
        [
            idx
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_jobs_x_task']
            for record in windows
        ],
        width=cur_bar_width,
        label="Mean Num. Jobs x Task"
    )
    axes.bar(
        [
            idx + cur_bar_width
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_jobs_x_user']
            for record in windows
        ],
        width=cur_bar_width,
        label="Mean Num. Jobs x User"
    )
    axes.bar(
        [
            idx + cur_bar_width * 2
            for idx, _ in enumerate(windows)
        ],
        [
            record['mean_num_tasks_x_user']
            for record in windows
        ],
        width=cur_bar_width,
        label="Mean Num. Tasks x User"
    )
    axes.set_yscale('log')
    axes.set_xticks(range(len(windows)))
    axes.set_xticklabels(
        [str(idx) for idx in range(len(windows))]
    )
    axes.grid()
    legend = axes.legend(bbox_to_anchor=(0.36, -1.8))
    axes.set_xlabel("Window")
    pbar.update(1)

    plt.savefig(
        os.path.join(result_folder, "stats_window-requests.png"),
        dpi=dpi,
        bbox_extra_artists=(legend, ),
        bbox_inches='tight'
    )
    pbar.update(1)

    ###########################################################################
    # window_frequency_stats
    ###########################################################################
    # plt.clf()
    # grid = plt.GridSpec(8, len(windows), wspace=2.42, hspace=2.33)

    # for win_idx, window in enumerate(windows):
    #     axes = plt.subplot(grid[0:4, win_idx])
    #     labels = sorted(window['num_req_x_file_frequencies'].keys())
    #     sizes = [
    #         (window['num_req_x_file_frequencies'][label] /
    #          window['num_requests']) * 100.
    #         for label in labels
    #     ]
    #     cut_idx = -1
    #     for idx, size in enumerate(sizes):
    #         if size < 2.:
    #             cut_idx = idx
    #             break
    #     labels = labels[:cut_idx] + ['< 2%']
    #     sizes = sizes[:cut_idx] + [sum(sizes[cut_idx:])]
    #     axes.pie(sizes, radius=2.8, labels=labels,
    #              autopct='%1.0f%%', startangle=90)
    #     axes.set_xlabel(f"\n\nWindow {win_idx}")

    # for win_idx, window in enumerate(windows):
    #     axes = plt.subplot(grid[4:8, win_idx])
    #     labels = sorted(window['num_req_x_file_frequencies'].keys())[1:]
    #     sizes = [
    #         (window['num_req_x_file_frequencies'][label] /
    #          window['num_requests']) * 100.
    #         for label in labels
    #     ]
    #     cut_idx = -1
    #     for idx, size in enumerate(sizes):
    #         if size < 2.:
    #             cut_idx = idx
    #             break
    #     labels = labels[:cut_idx] + ['< 2%']
    #     sizes = sizes[:cut_idx] + [sum(sizes[cut_idx:])]
    #     axes.pie(sizes, radius=2.8, labels=labels,
    #              autopct='%1.0f%%', startangle=90)
    #     axes.set_xlabel(f"\n\nWindow {win_idx}\nWithout 1 request")

    # pbar.update(1)
    # plt.savefig(
    #     os.path.join(result_folder, "stats_window-frequencies.png"),
    #     dpi=dpi
    # )
    # pbar.update(1)

    # ###########################################################################
    # # window_size_stats
    # ###########################################################################
    # plt.clf()
    # grid = plt.GridSpec(24, len(windows), wspace=2.42, hspace=5.)

    # axes = plt.subplot(grid[0:9, 0:])
    # axes.bar(
    #     [
    #         idx - (bar_width + bar_width / 2.)
    #         for idx, _ in enumerate(windows)
    #     ],
    #     [
    #         record['size_all_files']
    #         for record in windows
    #     ],
    #     width=bar_width,
    #     label="Size all files (GB)"
    # )
    # axes.bar(
    #     [
    #         idx - (bar_width / 2.)
    #         for idx, _ in enumerate(windows)
    #     ],
    #     [
    #         record['size_file_1req']
    #         for record in windows
    #     ],
    #     width=bar_width,
    #     label="Size files with 1 request (GB)"
    # )
    # axes.bar(
    #     [
    #         idx + (bar_width / 2.)
    #         for idx, _ in enumerate(windows)
    #     ],
    #     [
    #         record['size_file_g1req']
    #         for record in windows
    #     ],
    #     width=bar_width,
    #     label="Size files with more than 1 req. (GB)"
    # )
    # axes.set_xticks(range(len(windows)))
    # axes.set_xticklabels(
    #     [str(idx) for idx in range(len(windows))]
    # )
    # axes.grid()
    # axes.legend()
    # axes.set_xlabel("Window")

    # for win_idx, window in enumerate(windows):
    #     axes = plt.subplot(grid[12:18, win_idx])
    #     labels = sorted(window['desc_file_sizes'].keys())
    #     sizes = [
    #         float(window['desc_file_sizes'][label] /
    #               sum(window['desc_file_sizes'].values())) * 100.
    #         for label in labels
    #     ]
    #     # to_remove = []
    #     # for idx, size in enumerate(sizes):
    #     #     if size <= 25.:
    #     #         to_remove.append(idx)
    #     # for idx in reversed(sorted(to_remove)):
    #     #     sizes.pop(idx)
    #     #     labels.pop(idx)
    #     labels = [
    #         f"{int(label/1000.)} GB" if label >= 1000. else f"{label} MB"
    #         for label in labels
    #     ]
    #     axes.pie(sizes, radius=2.4, labels=labels,
    #              autopct='%1.0f%%', startangle=90)
    #     axes.set_xlabel(f"\nWin. {win_idx}")

    # for win_idx, window in enumerate(windows):
    #     axes = plt.subplot(grid[19:24, win_idx])
    #     labels = sorted(window['desc_file_sizes'].keys())
    #     sizes = [
    #         float(window['desc_file_sizes'][label] /
    #               sum(window['desc_file_sizes'].values())) * 100.
    #         for label in labels
    #     ]
    #     to_remove = []
    #     for idx, size in enumerate(sizes):
    #         if size > 25. or size <= 1.:
    #             to_remove.append(idx)
    #     for idx in reversed(sorted(to_remove)):
    #         sizes.pop(idx)
    #         labels.pop(idx)
    #     labels = [
    #         f"{int(label/1000.)} GB" if label >= 1000. else f"{label} MB"
    #         for label in labels
    #     ]
    #     axes.pie(sizes, radius=2.4, labels=labels,
    #              autopct='%1.0f%%', startangle=90)
    #     axes.set_xlabel(f"\nWin. {win_idx}\n1% < size <=25%")

    # pbar.update(1)
    # plt.savefig(
    #     os.path.join(result_folder, "stats_window-sizes.png"),
    #     dpi=dpi,
    #     bbox_inches='tight'
    # )
    # pbar.update(1)

    # ###########################################################################
    # # window_cache_size_stats
    # ###########################################################################
    # plt.clf()
    # grid = plt.GridSpec(8*len(windows), len(windows), wspace=1.42, hspace=5.)

    # for win_idx, window in enumerate(windows):
    #     start_idx = win_idx*8
    #     axes = plt.subplot(grid[start_idx:start_idx+7, 0:])
    #     keys = sorted(window['sizes_x_min_num_requests'].keys())
    #     axes.bar(
    #         [key - 1 for key in keys],
    #         [window['sizes_x_min_num_requests'][key] for key in keys],
    #         label="Cache size based on min num requests to store. (GB)"
    #     )
    #     axes.set_xticks(range(len(keys)))
    #     axes.set_xticklabels(
    #         [str(key) for key in keys]
    #     )
    #     axes.grid()
    #     axes.legend()
    #     axes.set_xlabel(f"Window {win_idx}")

    # pbar.update(1)
    # plt.savefig(
    #     os.path.join(result_folder, "stats_window-cache-sizes.png"),
    #     dpi=dpi,
    #     bbox_inches='tight'
    # )
    # pbar.update(1)

    # ###########################################################################
    # # window_task_stats
    # ###########################################################################
    # plt.clf()
    # grid = plt.GridSpec(18, len(windows)*2, wspace=1, hspace=1.)

    # axes = plt.subplot(grid[0:5, 0:])
    # cur_bar_width = bar_width / 2.
    # axes.bar(
    #     [
    #         idx - cur_bar_width * 2
    #         for idx, _ in enumerate(windows)
    #     ],
    #     [
    #         record['num_users']
    #         for record in windows
    #     ],
    #     width=cur_bar_width,
    #     label="Num. users"
    # )
    # axes.bar(
    #     [
    #         idx - cur_bar_width
    #         for idx, _ in enumerate(windows)
    #     ],
    #     [
    #         record['num_sites']
    #         for record in windows
    #     ],
    #     width=cur_bar_width,
    #     label="Num. sites"
    # )
    # axes.bar(
    #     [
    #         idx
    #         for idx, _ in enumerate(windows)
    #     ],
    #     [
    #         record['num_tasks']
    #         for record in windows
    #     ],
    #     width=cur_bar_width,
    #     label="Num. tasks"
    # )
    # axes.bar(
    #     [
    #         idx + cur_bar_width
    #         for idx, _ in enumerate(windows)
    #     ],
    #     [
    #         record['num_jobs']
    #         for record in windows
    #     ],
    #     width=cur_bar_width,
    #     label="Num. jobs"
    # )
    # axes.set_yscale('log')
    # axes.set_xticks(range(len(windows)))
    # axes.set_xticklabels(
    #     [str(idx) for idx in range(len(windows))]
    # )
    # axes.grid()
    # legend = axes.legend(bbox_to_anchor=(0.32, 2.3))

    # for win_idx, window in enumerate(windows):
    #     axes = plt.subplot(grid[6:8, win_idx*2:win_idx*2+2])
    #     labels = sorted(window['protocols'].keys())
    #     sizes = [
    #         window['protocols'][label]
    #         for label in labels
    #     ]
    #     # to_remove = []
    #     # for idx, size in enumerate(sizes):
    #     #     if size <= 25.:
    #     #         to_remove.append(idx)
    #     # for idx in reversed(sorted(to_remove)):
    #     #     sizes.pop(idx)
    #     #     labels.pop(idx)
    #     axes.pie(sizes, radius=1.6, labels=labels,
    #              autopct='%1.0f%%', startangle=90)
    #     if win_idx == 0:
    #         text = axes.text(-14, 0.1, "Protocols %")
    #     # axes.set_xlabel(f"\nWin. {win_idx}\nprotocols")

    # for win_idx, window in enumerate(windows):
    #     axes = plt.subplot(grid[9:11, win_idx*2:win_idx*2+2])
    #     labels = ['CPU', 'I/O']
    #     sizes = [window['all_cpu_time'], window['all_io_time']]
    #     axes.pie(sizes, radius=1.6, labels=labels,
    #              autopct='%1.0f%%', startangle=90)
    #     if win_idx == 0:
    #         text = axes.text(-14, 0.1, "Time %")
    #     # axes.set_xlabel(f"\nWin. {win_idx}\ntime")

    # for win_idx, window in enumerate(windows):
    #     axes = plt.subplot(grid[12:14, win_idx*2:win_idx*2+2])
    #     labels = ['CPU', 'I/O']
    #     sizes = [window['local_cpu_time'], window['local_io_time']]
    #     axes.pie(sizes, radius=1.6, labels=labels,
    #              autopct='%1.0f%%', startangle=90)
    #     if win_idx == 0:
    #         text = axes.text(-14, 0.1, "Local Time %")
    #     # axes.set_xlabel(f"\nWin. {win_idx}\ntime (local)")

    # for win_idx, window in enumerate(windows):
    #     axes = plt.subplot(grid[15:17, win_idx*2:win_idx*2+2])
    #     labels = ['CPU', 'I/O']
    #     sizes = [window['remote_cpu_time'], window['remote_io_time']]
    #     axes.pie(sizes, radius=1.6, labels=labels,
    #              autopct='%1.0f%%', startangle=90)
    #     if win_idx == 0:
    #         text = axes.text(-14, 0.1, "Remote time %")
    #     # axes.set_xlabel(f"\nWin. {win_idx}\ntime (remote)")

    # # with warnings.catch_warnings():
    # #     warnings.simplefilter("ignore")
    # #     plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    # pbar.update(1)
    # plt.savefig(
    #     os.path.join(result_folder, "stats_window-tasks.png"),
    #     dpi=dpi,
    #     bbox_extra_artists=(legend, text),
    #     bbox_inches='tight'
    # )
    # pbar.update(1)

    pbar.close()


def transform_sizes(size: float):
    """Convert the size to MB if it is less than 1GB

    Args:
        size (float): the current size in bytes

    Returns:
        float: the GB or MB size
    """
    GB_size = size // 1024**3
    if GB_size == 0.0:
        return size // 1024**2
    return GB_size * 1000.


def star_decorator(func):
    @wraps(func)
    def star_wrapper(inputs):
        return func(*inputs)
    return star_wrapper


@star_decorator
def make_dataframe_stats(data: list, window_index: int = 0,
                         process_num: int = 0):
    """Create window stats.

    Args:
        data (list): a list of dataframes (df)

    Returns:
        dict: extracted statistics from the DataFrame

    Note:
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
    pbar = tqdm(desc=f"Make dataframe stats of window {window_index}",
                total=32+len(data), position=process_num, ascii=True,
                leave=False)

    df = []
    for filename in data:
        with gzip.GzipFile(
            filename, mode="rb"
        ) as stats_file:
            df.append(pd.read_feather(stats_file))
        pbar.update(1)

    df = pd.concat(df)
    pbar.update(1)

    # Num requests and num files
    num_requests = df.shape[0]
    num_files = len(df['filename'].unique().tolist())
    size_all_files = df[['filename', 'size']].dropna().drop_duplicates(
        subset='filename')['size'].sum() / 1024. ** 3.
    pbar.update(1)

    # Mean num. request x file and
    # Mean num. request x file with num requests > min
    num_req_x_file = df['filename'].value_counts()
    num_req_x_file_gmin = num_req_x_file[
        num_req_x_file > num_req_x_file.min()
    ]
    mean_num_req_x_file = num_req_x_file.mean()
    mean_num_req_x_file_gmin = num_req_x_file_gmin.mean()
    pbar.update(1)

    # Num of requests x file
    num_req_x_file_frequencies = num_req_x_file.value_counts().to_dict()
    assert num_requests == sum(
        [key * value for key, value in num_req_x_file_frequencies.items()])
    pbar.update(1)

    # # Size files with 1 request and greater than 1 request
    # size_by_filename = df[['filename', 'size']].dropna().drop_duplicates(
    #     subset='filename')
    # file_1req_list = num_req_x_file[
    #     num_req_x_file <= num_req_x_file.min()
    # ].keys().to_list()
    # file_g1req_list = num_req_x_file[
    #     num_req_x_file > num_req_x_file.min()
    # ].keys().to_list()
    # size_1req_files = size_by_filename[size_by_filename['filename'].isin(
    #     file_1req_list)]['size'].sum() / 1024. ** 3
    # size_g1req_files = size_by_filename[size_by_filename['filename'].isin(
    #     file_g1req_list)]['size'].sum() / 1024. ** 3
    # desc_file_sizes = size_by_filename['size'].apply(
    #     transform_sizes).value_counts().sort_index().to_dict()
    # pbar.update(1)

    # sizes_x_min_num_requests = {}
    # for min_num_request in range(1, 21):
    #     num_request_filter = num_req_x_file[
    #         num_req_x_file >= min_num_request
    #     ].keys().to_list()
    #     sizes_x_min_num_requests[min_num_request] = size_by_filename[
    #         size_by_filename['filename'].isin(
    #             num_request_filter)]['size'].sum() / 1024. ** 3
    #     pbar.update(1)

    # # Task and job stats
    # num_users = df['user'].unique().shape[0]
    # num_sites = df['site_name'].unique().shape[0]
    # num_tasks = df['task_id'].unique().shape[0]
    # num_jobs = df['job_id'].unique().shape[0]
    # protocols = df['protocol'].value_counts().to_dict()
    # all_cpu_time = df['cpu_time'].sum()
    # all_io_time = df['io_time'].sum()
    # local_cpu_time = df['cpu_time'][
    #     df.protocol == 'Local'
    # ].sum()
    # local_io_time = df['io_time'][
    #     df.protocol == 'Local'
    # ].sum()
    # remote_cpu_time = df['cpu_time'][
    #     df.protocol == 'Remote'
    # ].sum()
    # remote_io_time = df['io_time'][
    #     df.protocol == 'Remote'
    # ].sum()
    # pbar.update(1)

    # mean_num_files_x_job = df[['job_id', 'filename']].groupby(
    #     'job_id')['filename'].nunique().mean()
    # pbar.update(1)
    # mean_num_files_x_task = df[['task_id', 'filename']].groupby(
    #     'task_id')['filename'].nunique().mean()
    # pbar.update(1)
    # mean_num_files_x_user = df[['user', 'filename']].groupby(
    #     'user')['filename'].nunique().mean()
    # pbar.update(1)
    # mean_num_jobs_x_task = df[['job_id', 'task_id']].groupby(
    #     'task_id')['job_id'].nunique().mean()
    # pbar.update(1)
    # mean_num_jobs_x_user = df[['job_id', 'user']].groupby(
    #     'user')['job_id'].nunique().mean()
    # pbar.update(1)
    # mean_num_tasks_x_user = df[['task_id', 'user']].groupby(
    #     'user')['task_id'].nunique().mean()
    # pbar.update(1)

    pbar.close()

    return {
        'num_requests': num_requests,
        'num_files': num_files,

        'mean_num_req_x_file': mean_num_req_x_file,
        'mean_num_req_x_file_gmin': mean_num_req_x_file_gmin,

        'num_req_x_file_frequencies': num_req_x_file_frequencies,

        # 'size_all_files': size_all_files,
        # 'size_file_1req': size_1req_files,
        # 'size_file_g1req': size_g1req_files,
        # 'desc_file_sizes': desc_file_sizes,
        # 'sizes_x_min_num_requests': sizes_x_min_num_requests,

        # 'num_users': num_users,
        # 'num_sites': num_sites,
        # 'num_tasks': num_tasks,
        # 'num_jobs': num_jobs,
        # 'protocols': protocols,
        # 'all_cpu_time': all_cpu_time,
        # 'all_io_time': all_io_time,
        # 'local_cpu_time': local_cpu_time,
        # 'local_io_time': local_io_time,
        # 'remote_cpu_time': remote_cpu_time,
        # 'remote_io_time': remote_io_time,

        # 'mean_num_files_x_job': mean_num_files_x_job,
        # 'mean_num_files_x_task': mean_num_files_x_task,
        # 'mean_num_files_x_user': mean_num_files_x_user,
        # 'mean_num_jobs_x_task': mean_num_jobs_x_task,
        # 'mean_num_jobs_x_user': mean_num_jobs_x_user,
        # 'mean_num_tasks_x_user': mean_num_tasks_x_user,
    }


def make_stats(input_data: list):
    """Start the process to make statistics.

    It downloads the source data and extracts the statistics.
    At the end it stores the resulting DataFrame in a gzipped
    file in the feather format

    Args:
        input_data (list): a list of input which includes:
                           1. The num. of the current process
                           2. a tuple of the date, (year, month, day)
                           3. the output folder string
                           4. the minio configuration string
                           5. the path to SQLite db for file sizes
                           6. the Redis url string

    Returns:
        tuple: the date tuple -> (year, month, day)
    """
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
    """Program entrypoint.

    You can use this program to extract data from a preprocessed
    data in the json.gz format.

    Example to extract data:
        python statistics.py extract --out-folder results_8w \
          --start-date "2018 5 1" \
          --window-size 56 \
          --minio-config "localhost:9000 minioname miniopassword bucketname" \
          -j 4 --file-size-db-path /foo/bar/file_sizes_folder \
          --redis-url localhost

    Example to plot data:
        python statistics.py plot --result-folder results_8w -pws 7
    """
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
    parser.add_argument('--jobs', '-j', type=int, default=4,
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
                total=len(day_list), desc="Extract stats",
                position=0, ascii=True
            )
            for year, month, day in pool.imap(
                make_stats, day_list, chunksize=1
            ):
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

        counter = 0
        for file_idx, file_ in enumerate(tqdm(
            files, desc="Search stat results", position=0, ascii=True
        )):
            head, tail0 = os.path.splitext(file_)
            head, tail1 = os.path.splitext(head)

            if head.find("results_") == 0 and tail0 == ".gz"\
                    and tail1 == ".feather":
                cur_file = os.path.join(args.result_folder, file_)
                data_frames.append(cur_file)

            if len(data_frames) == args.plot_window_size:
                windows.append((
                    data_frames,
                    counter,
                    counter % args.jobs
                ))
                data_frames = []
                counter += 1

            # TO TEST
            # if file_idx == 0:
            #     break
            if len(windows) == 2:
                break

        if len(data_frames) > 0:
            windows.append((
                data_frames,
                counter,
                counter % args.jobs
            ))
            data_frames = []

        for idx, window in enumerate(pool.imap(
            make_dataframe_stats, windows)
        ):
            windows[idx] = window

        plot_windows(windows, args.result_folder, args.plot_dpi)

    else:
        parser.print_usage()


if __name__ == "__main__":
    main()
