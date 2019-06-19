import argparse
import json
import os
from collections import OrderedDict
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import urllib3
from minio import Minio
from minio.error import (BucketAlreadyExists, BucketAlreadyOwnedByYou,
                         ResponseError)
from tqdm import tqdm

from DataManager import DataFile


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

    def add(self, date: tuple, filename: str):
        if self.__last_date != date:
            self.__cur_date = int(datetime(*date).timestamp())
            self.__last_date = date

        if self.__cur_date not in self._data:
            self._data[self.__cur_date] = {
                'num_requests': 0,
                'file_requests': OrderedDict()
            }

        cur_obj = self._data[self.__cur_date]

        if filename not in cur_obj['file_requests']:
            cur_obj['file_requests'][filename] = 0

        cur_obj['num_requests'] += 1
        cur_obj['file_requests'][filename] += 1

    def to_dict(self):
        return self._data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['extract', 'plot'],
                        help="A string date: \"2019 5 5\"")
    parser.add_argument('--stats-file', '-sf', type=str,
                        help="The json file to plot")
    parser.add_argument('--start-date', '-s', type=str,
                        help="A string date: \"2019 5 5\"")
    parser.add_argument('--plot-dpi', type=int, default=600,
                        help="DPI for plot output")
    parser.add_argument('--window-size', '-ws', type=int,
                        help="Num. of days to extract")
    parser.add_argument('--minio-config', '-mcfg', type=str,
                        help='MinIO configuration in the form: "url key secret bucket"')

    args, _ = parser.parse_known_args()

    stats = Statistics()

    if args.command == "extract":
        if args.minio_config:
            minio_client, bucket = create_minio_client(args.minio_config)
            for year, month, day in period(args.start_date, args.window_size):
                print(f"[Original Data][{year}-{month}-{day}]")
                print("[Original Data][Download...]")
                try:
                    minio_client.fget_object(
                        f"{bucket}",
                        f'year{year}_month{month}_day{day}.json.gz',
                        './tmp.json.gz'
                    )
                except ResponseError:
                    raise
                print("[Original Data][Downloaded]")
                print("[Original Data][Open File]")
                collector = DataFile("./tmp.json.gz")

                for record in tqdm(collector, desc=f"Extract statistics from {year}-{month}-{day}]"):
                    stats.add((year, month, day), record['FileName'])

                os.remove("./tmp.json.gz")

            with open("results.json", "w") as output_file:
                json.dump(stats.to_dict(), output_file)
    elif args.command == 'plot':
        if args.stats_file:
            with open(args.stats_file) as stats_file:
                result = json.load(stats_file)

            days_list = range(len(result))
            plt.clf()
            plt.bar(
                days_list,
                [record['num_requests'] for record in result.values()]
            )
            plt.ylabel("Num. Requests")
            plt.xlabel("Day")
            plt.xticks(
                days_list,
                [
                    datetime.fromtimestamp(float(day)).strftime("%Y-%m-%d")
                    for day in result
                ]
            )
            plt.tight_layout()
            plt.savefig(f"{args.stats_file}.dayrequests.png",
                        dpi=args.plot_dpi)

            for day, stats in result.items():
                plt.clf()
                elements = list(stats['file_requests'].values())
                max_ = max(elements)
                bins = []
                xtics = []
                num_requests = stats['num_requests']

                for num in tqdm(range(max_), desc="Make bins"):
                    counter = elements.count(num)
                    if counter > 0:
                        bins.append(counter)
                        xtics.append(str(num))

                plt.bar(
                    range(len(bins)),
                    [float(elm / num_requests) * 100. for elm in bins]
                )
                plt.ylabel("Num. File %")
                plt.xlabel("Num. Requests")
                max_bin_perc = int(float(max(bins) / num_requests) * 100) + 5
                plt.yticks(
                    range(0, max_bin_perc, 5),
                    [f"{val}%" for val in range(0, max_bin_perc, 5)]
                )
                plt.xticks(
                    range(len(xtics)),
                    xtics,
                    rotation='vertical'
                )
                plt.tight_layout()
                plt.savefig(
                    f"{args.stats_file}.nrequestxfile.{day}.png", dpi=args.plot_dpi
                )

        else:
            raise Exception("You have to pass the '--stats-file' argument...")
    else:
        parser.print_usage()


if __name__ == "__main__":
    main()
