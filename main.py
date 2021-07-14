import os
import sys
import getopt

import threading

import boto3
import botocore

import hashlib

from timeit import default_timer as timer

print("Starting...")

exclude_list: list[str] = ['.DS_Store']


def hash_file(file):
    BUF_SIZE: int = 65536
    sha256 = hashlib.sha256()

    with open(file, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)

    return sha256.hexdigest()


def s3_add_metadata(bucket, object, metadata):
    s3 = boto3.resource('s3')
    s3_object = s3.Object(bucket, object)

    s3_object.metadata.update(metadata)
    s3_object.copy_from(CopySource={'Bucket': bucket, 'Key': object}, Metadata=s3_object.metadata,
                        MetadataDirective='REPLACE')


def s3_upload(file, bucket, object, metadata):
    client = boto3.client('s3')
    transfer = boto3.s3.transfer.S3Transfer(client)
    size = float(os.path.getsize(file))

    start = timer()
    transfer.upload_file(file, bucket, object, extra_args=metadata, callback=_progress(file, size, 'Upload'))
    end = timer()

    print("Upload completed in " + str(end - start) + " seconds.")


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
            # logging.info(
            if _msg_count % _msg_throttle == 0:
                print("%s: %s  %s / %s  (%.2f%%)" % (
                    _ops, _filename, _seen_so_far, round(_size),
                    percentage))

            _msg_count += 1

    return call


def get_files(directory):
    return local_get_files(directory)


def local_get_files(directory):
    print("Getting files...")
    catalog = {}

    for (dirpath, dirnames, filenames) in os.walk(directory):
        if not any(item in filenames for item in exclude_list):
            for f in filenames:
                catalog[f] = dirpath + '/' + f
    return catalog


def put_files(catalog, bucket):
    return s3_put_files(catalog, bucket)


def s3_put_files(catalog, bucket):
    # s3 = boto3.resource('s3')

    s3 = boto3.client('s3')

    for file in catalog:

        sha256 = hash_file(catalog[file])
        m = {"Metadata": {'sha256': sha256}}

        try:
            metadata = s3.head_object(Bucket=bucket, Key=file)['Metadata']

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print(file + " does not exist in " + bucket)

                # Upload filepath, bucket, objectname
                start = timer()

                s3_upload(catalog[file], bucket, file, m)
                end = timer()
                print("Upload completed in " + str(end - start) + " seconds.")
                break

        else:

            # metadata = boto3.client('s3').head_object(Bucket=bucket, Key=file)['Metadata']

            old_hash = metadata['sha256']
            # new_hash = hash_file(catalog[file])
            new_hash = sha256

            if old_hash == new_hash:
                print(file + " object exists in S3 with matching hash. Skipping...")
            else:
                print(file + " hash (" + new_hash + ") doesn't match S3 object (" + old_hash + "). Uploading again...")

                start = timer()
                s3_upload(catalog[file], bucket, file)
                end = timer()
                print("Upload completed in " + str(end - start) + " seconds.")
                break


def main(argv):
    directory = ''
    bucket = ''

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

        files = get_files(directory)
        put_files(files, bucket)


if __name__ == "__main__":
    main(sys.argv[1:])
