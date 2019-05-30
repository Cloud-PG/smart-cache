import subprocess
import sys
from datetime import datetime, timedelta
from os import remove

import urllib3
from minio import Minio
from minio.error import (BucketAlreadyExists, BucketAlreadyOwnedByYou,
                         ResponseError)


def period(start_date, num_days):
    delta = timedelta(days=1)

    year, month, day = [int(elm) for elm in start_date.split()]
    cur_date = datetime(year, month, day)

    for _ in range(num_days):
        yield (cur_date.year, cur_date.month, cur_date.day)
        cur_date = cur_date+delta


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
        print("[Original Data][{}-{}-{}]".format(year, month, day))
        print("[Original Data][Download...]")
        ret = subprocess.check_call(
            "hdfs dfs -get /project/awg/cms/jm-data-popularity/avro-snappy/year={}/month={}/day={}/part-m-00000.avro".format(
                year, month, day),
            shell=True
        )
        print("[Original Data][Downloaded]")
        if ret == 0:
            # Initialize minioClient with an endpoint and access/secret keys.
            try:
                print("[Original Data][Copying...]")
                minioClient.fput_object(
                    sys.argv[4],  # Bucket name
                    'year{}_month{}_day{}.avro'.format(year, month, day),
                    'part-m-00000.avro'
                )
            except ResponseError as err:
                print(err)

            remove("part-m-00000.avro")
            print("[Original Data][DONE][{}-{}-{}]".format(year, month, day))


if __name__ == "__main__":
    main()
