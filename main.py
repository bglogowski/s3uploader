import getopt
import hashlib
import os
import sys
import threading
from timeit import default_timer as timer

import boto3.s3.transfer
import botocore.exceptions


def hash_file(file_path) -> str:
    file_buffer: int = 65536

    sha256 = hashlib.sha256()

    with open(file_path, 'rb') as f:
        while True:
            data = f.read(file_buffer)
            if not data:
                break
            sha256.update(data)

    return sha256.hexdigest()


def s3_upload(file, bucket, obj, metadata) -> float:
    client = boto3.client('s3')
    transfer = boto3.s3.transfer.S3Transfer(client)

    size: float = float(os.path.getsize(file))

    start = timer()
    transfer.upload_file(file,
                         bucket,
                         obj,
                         extra_args=metadata,
                         callback=_progress(file, size, 'Upload'))
    end = timer()

    return end - start


def _progress(filename, size, ops):
    _filename = filename
    _size = size
    _seen_so_far = 0
    _ops = ops
    _lock = threading.Lock()

    _msg_count = 0
    _msg_throttle = 50

    def call(bytes_amount):
        with _lock:
            nonlocal _seen_so_far
            nonlocal _msg_count
            _seen_so_far += bytes_amount
            percentage = (_seen_so_far / _size) * 100

            if _msg_count % _msg_throttle == 0:
                print(f"{_ops}: {_filename}  {_seen_so_far} / {round(_size)}  ({percentage:.2f}%)")

            _msg_count += 1

    return call


def fs_get_files(directory):
    exclude_list: set[str] = {".DS_Store"}
    catalog: dict[str, str] = {}

    for (path, dirs, files) in os.walk(directory):
        if not any(x in files for x in exclude_list):
            for f in files:
                catalog[f] = path + "/" + f

    return catalog


def s3_put_files(catalog, bucket):
    s3 = boto3.client('s3')

    for file in catalog:

        sha256 = hash_file(catalog[file])
        m = {"Metadata": {"sha256": sha256}}

        try:
            s3_metadata = s3.head_object(Bucket=bucket, Key=file)['Metadata']

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print(file + " does not exist in " + bucket)

                elapsed_time = s3_upload(catalog[file], bucket, file, m)
                print("Upload completed in " + str(elapsed_time) + " seconds.")
                break

        else:

            s3_hash = s3_metadata['sha256']

            if s3_hash == sha256:
                print(file + " object exists in S3 with matching hash. Skipping...")
            else:
                print(file + " hash (" + sha256 + ") doesn't match S3 object (" + s3_hash + ").")
                print("Uploading again...")

                elapsed_time = s3_upload(catalog[file], bucket, file, m)
                print("Upload completed in " + str(elapsed_time) + " seconds.")
                break


def main(argv):
    directory: str = ""
    bucket: str = ""

    try:
        opts, args = getopt.getopt(argv, "hb:d:", ["bucket=", "directory="])

    except getopt.GetoptError:
        print(sys.argv[0] + ' -d <base directory> -b <S3 bucket>')
        sys.exit(2)

    for opt, arg in opts:

        if opt == '-h':
            print(sys.argv[0] + ' -d <base directory> -b <S3 bucket>')
            sys.exit()

        elif opt in ("-d", "--directory"):
            directory = arg

        elif opt in ("-b", "--bucket"):
            bucket = arg

    files: dict[str, str] = fs_get_files(directory)
    assert isinstance(bucket, str)

    return s3_put_files(files, bucket)


if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
