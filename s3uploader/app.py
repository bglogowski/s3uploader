import getopt
import os
import random
import sys

from functools import reduce
from timeit import default_timer as timer

from s3uploader import log
from s3uploader.common.cloud import S3Bucket
from s3uploader.common.files import LocalFile


def fs_get_files(directory: str) -> list:
    """
    Gather list of files to upload to S3

    :param directory: Root directory with files to upload to S3
    :type directory: str
    :return: returns a list of LocalFile objects
    :rtype: list
    """

    # fix for macOS metadata files
    exclude_list: set[str] = {".DS_Store"}

    file_list: list = []

    for (path, dirs, files) in os.walk(directory):
        if not any(x in files for x in exclude_list):
            for f in files:
                file_list.append(LocalFile(f, path, directory))

    return file_list


def run(argv):
    directory: str = ""
    bucket_name: str = ""

    file_limit: int = 0
    time_limit: int = 0
    size_limit: int = 0

    use_folders: bool = False
    random_shuffle: bool = False

    start_time = timer()
    file_sizes = []

    # Parse the command line arguments
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
                log.debug(f"File limit = {str(file_limit)}")
            except ValueError:
                log.error("File limit is not an integer")
                sys.exit(2)

        elif opt in ("-s", "--size-limit"):
            try:
                size_limit = int(arg)
                log.debug(f"Size limit = {str(size_limit)}")
            except ValueError:
                log.error("Size limit is not an integer")
                sys.exit(2)

        elif opt in ("-t", "--time-limit"):
            try:
                time_limit = int(arg)
                log.debug(f"Time limit = {str(time_limit)}")
            except ValueError:
                log.error("Time limit is not an integer")
                sys.exit(2)

        elif opt == "-f":
            use_folders = True

        elif opt == "-r":
            random_shuffle = True

    # Create a Bucket object
    bucket = S3Bucket(bucket_name)

    # Get a list of files to upload
    files = fs_get_files(directory)

    # Randomize the list if desired
    if random_shuffle:
        random.shuffle(files)

    # Process the list of files
    for file in files:
        # First, verify several conditions are met before uploading

        # Do not upload if the maximum number of files has been reached
        if len(file_sizes) >= file_limit:
            log.warning("File upload limit reached. Exiting...")
            break

        # If files have already been uploaded, verify the upload size
        # limit has not been reached (in bytes).
        if len(file_sizes) > 0:
            # Don't bother to do calculations unless there's a limit
            if size_limit > 0:
                total_data_uploaded = reduce(lambda x, y: round(x + y), file_sizes)
                if total_data_uploaded >= size_limit:
                    for msg in [str(round(x)) + " bytes" for x in file_sizes]:
                        log.debug(f"Uploaded file of size = {msg}")
                    log.debug(f"Total data uploaded = {str(total_data_uploaded)} bytes")
                    log.warning("Upload size limit reached. Exiting...")
                    break

        # Determine if the upload time limit has been reached (in seconds)
        elapsed_seconds = round(timer() - start_time)
        log.debug(f"Elapsed time = {str(elapsed_seconds)} seconds")
        if elapsed_seconds >= time_limit:
            log.warning("Upload time limit reached. Exiting...")
            break

        # Optionally use the relative file paths of the local files
        # as the key for the S3 object (the default is to only use the
        # name of the file).
        if use_folders:
            file.s3key = file.relative_path

        original_size = bucket.size
        if bucket.upload(file):

            bucket_percent_increase = ((float(bucket.size) / float(original_size)) - 1) * 100
            file_sizes.append(file.size)
            log.info(f"S3 Bucket size increased by {str(round(bucket_percent_increase, 2))}%")
