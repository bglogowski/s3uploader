

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


# Use ISO 8601 timestamp standard
formatter = logging.Formatter('%(asctime)s %(pathname)s[%(process)d] (%(name)s) %(levelname)s: %(message)s',
                              '%Y-%m-%dT%H:%M:%S%z')

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

log = logging.getLogger()
log.setLevel(logging.INFO)
log.addHandler(stdout_handler)


class LocalFile(object):
    """

    """
    def __init__(self, name: str, path: str, base_path: str):

        # Use setters to validate input
        self.base_path = base_path
        self.name = name
        self.path = path
        self.s3key = name
        self.uploadable = False

        # Allow lazy loading of these values
        self._size = None
        self._hash = None
        self._metadata = None

    def _identify(self):
        return self.__class__.__name__ + "." + sys._getframe(1).f_code.co_name

    @staticmethod
    def exists(path: str) -> bool:
        return os.path.isfile(path)

    @staticmethod
    def sha256(path: str) -> str:

        file_buffer: int = 65536
        sha256 = hashlib.sha256()

        with open(path, 'rb') as f:
            while True:
                data = f.read(file_buffer)
                if not data:
                    break
                sha256.update(data)

        log.debug(__class__.__name__ + "." +
                  sys._getframe().f_code.co_name + " = " +
                  sha256.hexdigest())

        return sha256.hexdigest()

    @property
    def base_path(self):
        return self._base_path

    @base_path.setter
    def base_path(self, value):
        self._base_path = value
        log.debug(self._identify() + " = " + self._base_path)

    @property
    def full_path(self) -> str:
        return self._full_path

    @property
    def hash(self) -> str:
        if self._hash is None:
            self._hash = self.sha256(self.full_path)
            log.debug(self._identify() + " = " + self._hash)
        return self._hash

    @property
    def metadata(self) -> dict:
        if self._metadata is None:
            self._metadata = {"Metadata": {"sha256": self.hash}}
        return self._metadata

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value
        log.debug(self._identify() + " = " + self._name)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):



        self._path = value
        log.debug(self._identify() + " = " + self._path)

        self._full_path = self.path + "/" + self.name
        log.debug(self.__class__.__name__ + ".full_path = " + self._full_path)

        relative_path = self.full_path.replace(self.base_path, "")
        relative_path = re.sub(r'^/', '', relative_path)
        relative_path = re.sub(r'^\./', '', relative_path)
        self._relative_path = relative_path
        log.debug(self.__class__.__name__ + ".relative_path = " + self._relative_path)

    @property
    def relative_path(self) -> str:
        return self._relative_path

    @property
    def s3key(self):
        return self._s3key

    @s3key.setter
    def s3key(self, value):
        self._s3key = value
        log.debug(self._identify() + " = " + self._s3key)

    @property
    def size(self) -> float:
        if self._size is None:
            self._size = float(os.path.getsize(self.full_path))
            log.debug(self._identify() + " = " + str(self._size))
        return self._size

    @property
    def uploadable(self):
        return self._uploadable

    @uploadable.setter
    def uploadable(self, value):

        if type(value) is bool:
            self._uploadable = value
            log.debug(self._identify() + " = " + str(self._uploadable))
        else:
            raise ValueError("Cannot set " + self._identify() + " to non-boolean type.")



class CloudError(Exception):
    """Base class for exceptions in this module."""

    def __init__(self, message):
        self.message = message


class S3Bucket(object):

    def __init__(self, name: str, region=None):

        self.name = name

        if region is None:
            self._client = boto3.client('s3')
        else:
            self._client = boto3.client('s3', region_name=region)

        self._session = boto3.session.Session()
        self._region = self._session.region_name
        self._transfer = boto3.s3.transfer.S3Transfer(self._client)
        self._is_valid = False

    def _identify(self):
        return self.__class__.__name__ + "." + sys._getframe(1).f_code.co_name

    def create(self, region=None) -> bool:

        try:
            if region is None:
                self._client.create_bucket(Bucket=self.name)
            else:
                location = {'LocationConstraint': region}
                self._client.create_bucket(Bucket=self.name, CreateBucketConfiguration=location)
                self._region = region
        except ClientError as e:
            log.error(e)
            return False
        return True

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):

        if self.valid_name(value):
            self._name = value
            log.debug(self._identify() + " = " + self._name)
        else:
            log.error(self._identify() + " = " + value)
            raise ValueError("S3 Bucket name [" + value + "] is not valid.")

    def upload(self, file: LocalFile) -> float:
        if self.valid_bucket():
            start = timer()
            try:
                self._transfer.upload_file(file.full_path,
                                           self.name,
                                           file.s3key,
                                           extra_args=file.metadata,
                                           callback=self._progress(file, "Uploading"))
            except ClientError as e:
                log.error(e)
                return timer() - start

            return timer() - start
        else:
            raise CloudError("S3 Bucket [" + self.name + "] does not exist in AWS Region [" + self.region + "]")

    def metadata(self, key: str):
        if self.valid_bucket():
            try:
                response = self._client.head_object(Bucket=self.name, Key=key)['Metadata']
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    log.info(
                        "Object [" + key + "] does not exist in S3 Bucket [" + self.name + "] in AWS Region [" + self.region + "]")
                    return None
                else:
                    raise CloudError("Unknown return code from AWS: " + e.response['Error']['Code'])
            else:
                log.info("Object [" + key + "] was found in S3 Bucket [" + self.name + "] in AWS Region [" + self.region + "]")
                return response
        else:
            raise CloudError("S3 Bucket " + self.name + " does not exist in AWS Region " + self.region)

    def valid_bucket(self) -> bool:
        if self._is_valid:
            return True
        else:
            try:
                self._client.head_bucket(Bucket=self.name)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    log.error("S3 Bucket [" + self.name + "] does not exist in AWS Region [" + self.region + "]")
                    return False
                else:
                    raise CloudError("Unknown return code from AWS: " + e.response['Error']['Code'])
            else:
                self._is_valid = True
                log.info("S3 Bucket [" + self.name + "] was found in AWS Region [" + self.region + "]")
                return True

    @property
    def region(self):
        return self._region

    @property
    def size(self):
        if self.valid_bucket():
            try:
                response = self._client.list_objects(Bucket=self.name)['Contents']
                bucket_size = sum(obj['Size'] for obj in response)
                return bucket_size
            except ClientError as e:
                log.error("AWS responded with: " + e)
                return 0
        else:
            raise CloudError("S3 Bucket [" + self.name + "] does not exist in AWS Region [" + self.region + "]")

    @staticmethod
    def valid_name(value: str) -> bool:

        # Reduce textual repetition
        log_prefix = "S3 Bucket name [" + str(value) + "]"

        # Make sure input is a string
        if not type(value) == str:
            log.error(error_prefix + " is not a string")
            return False

        # The string must be between 3 and 63 characters long
        if len(value) < 3:
            log.error(log_prefix + " is too short: it must be more than 2 characters")
            return False
        if len(value) > 63:
            log.error(log_prefix + " is too long: it must be fewer than 64 characters")
            return False

        # The first and last characters may not be a hyphen
        if value.startswith("-") or value.endswith("-"):
            log.error(log_prefix + " cannot begin or end with a hyphen")
            return False

        # All characters must be lowercase alphanumeric or a hyphen
        valid_characters = re.compile(r'[^a-z0-9-]').search
        if bool(valid_characters(value)):
            log.error(log_prefix + " contains invalid characters")
            return False
        else:
            log.info(log_prefix + " passed validation")
            return True

    # For large files it is useful to get progress updates
    @staticmethod
    def _progress(file: LocalFile, ops: str):

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
                percentage = (_seen_so_far / file.size) * 100
                if _msg_count % _msg_throttle == 0 or int(percentage) == 100:
                    log.info(f"{_ops} [{file.name}]  {_seen_so_far} / {round(file.size)}  ({percentage:.2f}%)")
                _msg_count += 1

        return call




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
                log.debug("File limit = " + str(file_limit))
            except ValueError:
                log.error("File limit is not an integer")
                sys.exit(2)

        elif opt in ("-s", "--size-limit"):
            try:
                size_limit = int(arg)
                log.debug("Size limit = " + str(size_limit))
            except ValueError:
                log.error("Size limit is not an integer")
                sys.exit(2)

        elif opt in ("-t", "--time-limit"):
            try:
                time_limit = int(arg)
                log.debug("Time limit = " + str(time_limit))
            except ValueError:
                log.error("Time limit is not an integer")
                sys.exit(2)

        elif opt == "-f":
            use_folders = True

        elif opt == "-r":
            random_shuffle = True

    # Ensure a bucket name was specified
    #assert isinstance(bucket_name, str)
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
                        log.debug("Uploaded file of size = " + msg)
                    log.debug("Total data uploaded = " + str(total_data_uploaded) + " bytes")
                    log.warning("Upload size limit reached. Exiting...")
                    break

        # Determine if the upload time limit has been reached (in seconds)
        elapsed_seconds = round(timer() - start_time)
        log.debug("Elapsed time = " + str(elapsed_seconds) + " seconds")
        if elapsed_seconds >= time_limit:
            log.warning("Upload time limit reached. Exiting...")
            break

        # Optionally use the relative file paths of the local files
        # as the key for the S3 object (the default is to only use the
        # name of the file).
        if use_folders:
            file.s3key = file.relative_path

        # Get the metadata of the file if it exists in S3
        s3_object_metadata = bucket.metadata(file.s3key)

        if s3_object_metadata is None:
            file.uploadable = True
        else:

            if s3_object_metadata["sha256"] == file.hash:
                log.info("Object [" + file.s3key + "] in S3 Bucket [" + bucket.name + "] has matching hash. Skipping...")
            else:
                log.info("File hash doesn't match Object hash in S3 Bucket")
                log.debug("File " + file.s3key + " hash = " + file.hash)
                log.debug("Object " + file.s3key + " hash = " + s3_object_metadata["sha256"])

                file.uploadable = True
                log.info("Uploading again...")

        if file.uploadable:
            original_size = bucket.size
            elapsed_time = bucket.upload(file)
            post_upload_size = bucket.size
            percent = ((float(post_upload_size) / float(original_size)) - 1) * 100
            file_sizes.append(file.size)
            log.info("Upload completed in " + str(round(elapsed_time, 2)) + " seconds")
            log.info("S3 Bucket size increased by " + str(round(percent, 2)) + "%")




if __name__ == "__main__":
    main(sys.argv[1:])
