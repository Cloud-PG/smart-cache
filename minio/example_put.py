# Import MinIO library.
import os

from dotenv import load_dotenv
from minio import Minio
from minio.error import (BucketAlreadyExists, BucketAlreadyOwnedByYou,
                         ResponseError)

load_dotenv()

# Initialize minioClient with an endpoint and access/secret keys.
minioClient = Minio(
    'play.min.io',
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=True
)

# Make a bucket with the make_bucket API call.
try:
    minioClient.make_bucket("testbucket")
except BucketAlreadyOwnedByYou as err:
    pass
except BucketAlreadyExists as err:
    pass
except ResponseError as err:
    raise

# Put an object 'pumaserver_debug.log' with contents from 'pumaserver_debug.log'.
try:
    minioClient.fput_object(
        'testbucket', 'testfile.txt', './testfile.txt'
    )
except ResponseError as err:
    print(err)
