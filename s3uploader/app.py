import getopt
import hashlib
import logging
import os
import random
import re
import sys
import threading
from functools import reduce
from timeit import default_timer as timer

import boto3.s3.transfer
import botocore.exceptions

logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Use ISO 8601 timestamp standard
formatter = logging.Formatter('%(asctime)s %(pathname)s[%(process)d] (%(name)s) %(levelname)s: %(message)s',
                              '%Y-%m-%dT%H:%M:%S%z')

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

logger.addHandler(stdout_handler)

from s3uploader.common.files import LocalFile
from s3uploader.common.cloud import S3Bucket

def main(argv):

    directory: str = ""
    bucket_name: str = ""

    file_limit: int = 0
    time_limit: int = 0
    size_limit: int = 0

    use_folders: bool = False
    random_shuffle: bool = False

    start_time = timer()
    file_sizes = []

    try:
        opts, args = getopt.getopt(argv,
                                   "hd:b:fl:rs:t:",
                                   ["directory=", "bucket=", "file-limit=", "size-limit=", "time-limit="])

    except getopt.GetoptError:
        print(sys.argv[0] + " -d <base directory> -b <S3 bucket>")
        sys.exit(2)

    for opt, arg in opts:

        if opt == "-h":
            print(sys.argv[0] + " -d <base directory> -b <S3 bucket>")
            sys.exit()

        elif opt in ("-d", "--directory"):
            directory = arg

        elif opt in ("-b", "--bucket"):
            bucket_name = arg

        elif opt in ("-l", "--file-limit"):
            try:
                file_limit = int(arg)
                logging.debug("File limit set to: " + str(file_limit))
            except ValueError:
                logging.error("File limit is not an integer")
                sys.exit(2)

        elif opt in ("-s", "--size-limit"):
            try:
                size_limit = int(arg)
                logging.debug("Size limit set to: " + str(size_limit))
            except ValueError:
                logging.error("Size limit is not an integer")
                sys.exit(2)

        elif opt in ("-t", "--time-limit"):
            try:
                time_limit = int(arg)
                logging.debug("Time limit set to: " + str(time_limit))
            except ValueError:
                logging.error("Time limit is not an integer")
                sys.exit(2)

        elif opt == "-f":
            use_folders = True

        elif opt == "-r":
            random_shuffle = True

    assert isinstance(bucket_name, str)
    bucket = S3Bucket(bucket_name)

    files = fs_get_files(directory)
    if random_shuffle:
        random.shuffle(files)

    for file in files:

        if len(file_sizes) >= file_limit:
            logging.warning("File upload limit reached. Exiting...")
            break

        if len(file_sizes) > 0:
            total_data_uploaded = reduce(lambda x, y: round(x + y), file_sizes)
            if size_limit > 0 and total_data_uploaded >= size_limit:
                for msg in [str(round(x)) + " bytes" for x in file_sizes]:
                    logging.debug("Uploaded file of size " + msg)
                logging.debug("Total data uploaded = " + str(total_data_uploaded) + " bytes")
                logging.warning("Upload size limit reached. Exiting...")
                break

        elapsed_seconds = round(timer() - start_time)
        logging.debug("Elapsed time: " + str(elapsed_seconds) + " seconds")

        if elapsed_seconds >= time_limit:
            logging.warning("Upload time limit reached. Exiting...")
            break

        if use_folders:
            file.s3key = file.relative_path


        logging.debug("Using " + file.s3key + " as S3 object key.")

        s3_object_metadata = bucket.metadata(file.s3key)

        if s3_object_metadata is None:
            file.uploadable = True
        else:

            if s3_object_metadata["sha256"] == file.hash:
                logging.info(file.s3key + " object exists in S3 with matching hash. Skipping...")
            else:
                logging.info(file.s3key +
                             " hash (" +
                             file.hash +
                             ") doesn't match S3 object (" +
                             s3_metadata["sha256"] + ").")

                file.uploadable = True
                logging.info("Uploading again...")


        if file.uploadable:
            elapsed_time = bucket.upload(file)
            file_sizes.append(file.size)
            logging.info("Upload completed in " + str(elapsed_time) + " seconds.")
