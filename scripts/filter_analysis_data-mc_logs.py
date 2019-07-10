import subprocess
import sys
from datetime import datetime, timedelta
from os import remove

import urllib3
from minio import Minio
from minio.error import (BucketAlreadyExists, BucketAlreadyOwnedByYou,
                         ResponseError)
from tqdm import tqdm

from DataManager import DataFile, JSONDataFileWriter, get_or_create_descriptor


def period(start_date, num_days):
    delta = timedelta(days=1)

    year, month, day = [int(elm) for elm in start_date.split()]
    cur_date = datetime(year, month, day)

    for _ in range(num_days):
        yield (cur_date.year, cur_date.month, cur_date.day)
        cur_date = cur_date+delta


def split_filename(filename):
    parts = [part for part in filename.split("/") if part]
    if len(parts) > 1:
        return parts[1]
    else:
        return False


def main():

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

    minioClient = Minio(
        sys.argv[1],  # URL
        access_key=sys.argv[2],
        secret_key=sys.argv[3],
        secure=True,
        http_client=httpClient
    )

    start_date = sys.argv[5]  # Ex: "2018 1 1"
    num_days = int(sys.argv[6])

    for year, month, day in period(start_date, num_days):
        print(f"[Original Data][{year}-{month}-{day}]")
        print("[Original Data][Download...]")
        try:
            minioClient.fget_object(
                f"{sys.argv[4]}",
                f'year{year}_month{month}_day{day}.avro',
                './tmp.avro'
            )
        except ResponseError:
            raise
        print("[Original Data][Downloaded]")
        print("[Original Data][Open File]")
        collector = DataFile("./tmp.avro")

        print("[Original Data][Create New File]")
        with JSONDataFileWriter("./tmp.json.gz") as new_data:

            pbar = tqdm(desc="Filtering records")
            for record in collector:
                type_ = record['Type'].lower()
                type_storage = split_filename(record['FileName'])
                if type_ == "analysis" and type_storage in ['data', 'mc']:
                    new_data.append(record)
                pbar.update(1)
            pbar.close()

        try:
            print("[Original Data][Copying...]")
            minioClient.fput_object(
                f'{sys.argv[4]}-analysis-data-mc',  # Bucket name
                f'year{year}_month{month}_day{day}.json.gz',
                "./tmp.json.gz"
            )
        except ResponseError:
            raise

        remove("./tmp.avro")
        remove("./tmp.json.gz")

        print(f"[Original Data][DONE][{year}-{month}-{day}]")


if __name__ == "__main__":
    main()
