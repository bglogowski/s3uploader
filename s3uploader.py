"""
Amazon S3 file uploader script
======================

Uploads local files to S3 while optionally retaining the
local directory structure.

S3 Folder Structure
-------------------
S3 is not a filesystem, it is an object store. Therefore,
the default behavior is simply to upload files as objects
into a bucket using a flat namespace. However, owing to
the complexity of real world data, the script will optionally
organize the data in S3 using the local directory structure.

File integrity checks
-------------------
S3 Etags cannot be guaranteed to be MD5 hashes of the original
file, such as when multi-part uploads are used to create the
object. Therefore a cryptographic hash of the original file
will be stored as metadata of the S3 object.

SHA-256 was chosen because MD5 and SHA-1 are compromised and
therefore unreliable measures of file integrity. SHA-256 is
also reasonably performant as compared to alternative hash
algorithms.

"""

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

    @classmethod
    def from_csv(cls, text: str):
        name, path, base_path = [v.strip() for v in text.split(',')]
        return cls(name, path, base_path)

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

        logging.debug(str(__class__.__name__) + ".sha256 = " + sha256.hexdigest())
        return sha256.hexdigest()

    @property
    def base_path(self):
        return self._base_path

    @base_path.setter
    def base_path(self, value):
        self._base_path = value
        logging.debug(str(self.__class__.__name__) + ".base_path = " + self._base_path)

    @property
    def full_path(self) -> str:
        return self._full_path

    @property
    def hash(self) -> str:
        if self._hash is None:
            self._hash = self.sha256(self.full_path)
            logging.debug(str(self.__class__.__name__) + ".hash = " + self._hash)
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
        logging.debug(str(self.__class__.__name__) + ".name = " + self._name)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = value
        logging.debug(str(self.__class__.__name__) + ".path = " + self._path)

        self._full_path = self.path + "/" + self.name
        logging.debug(str(self.__class__.__name__) + ".full_path = " + self._full_path)

        relative_path = self.full_path.replace(self.base_path, "")
        relative_path = re.sub(r'^/', '', relative_path)
        relative_path = re.sub(r'^\./', '', relative_path)
        self._relative_path = relative_path
        logging.debug(str(self.__class__.__name__) + ".relative_path = " + self._relative_path)

    @property
    def relative_path(self) -> str:
        return self._relative_path

    @property
    def s3key(self):
        return self._s3key

    @s3key.setter
    def s3key(self, value):
        self._s3key = value
        logging.debug(str(self.__class__.__name__) + ".s3key = " + self._s3key)

    @property
    def size(self) -> float:
        if self._size is None:
            self._size = float(os.path.getsize(self.full_path))
            logging.debug(str(self.__class__.__name__) + ".size = " + str(self._size))
        return self._size

    @property
    def uploadable(self):
        return self._uploadable

    @uploadable.setter
    def uploadable(self, value):
        if type(value) is bool:
            self._uploadable = value
        else:
            logging.error("Trying to set variable to non-boolean type. Exiting...")
            sys.exit(2)
        logging.debug(str(self.__class__.__name__) + ".uploadable = " + str(self._uploadable))


class S3Bucket(object):

    def __init__(self, name: str):
        self.name = name
        self._client = boto3.client('s3')
        self._transfer = boto3.s3.transfer.S3Transfer(self._client)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value
        logging.debug(str(self.__class__.__name__) + ".name = " + self._name)

    def upload(self, file: LocalFile) -> float:
        start = timer()
        self._transfer.upload_file(file.full_path, self.name, file.s3key, extra_args=file.metadata, callback=self._progress(file, "Upload"))
        end = timer()

        return end - start

    def metadata(self, key: str):
        try:
            metadata = self._client.head_object(Bucket=self.name, Key=key)['Metadata']
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logging.info(key + " does not exist in " + self.name)
                return None
        else:
            logging.info(key + " found in " + self.name)
            return metadata




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

                if _msg_count % _msg_throttle == 0:
                    logging.info(f"{_ops}: {file.name}  {_seen_so_far} / {round(file.size)}  ({percentage:.2f}%)")

                _msg_count += 1

        return call




def fs_get_files(directory: str) -> list:
    exclude_list: set[str] = {".DS_Store"}
    file_catalog: list = []

    for (path, dirs, files) in os.walk(directory):
        if not any(x in files for x in exclude_list):
            for f in files:
                file_catalog.append(LocalFile(f, path, directory))

    return file_catalog


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
                             s3_object_metadata["sha256"] + ").")

                file.uploadable = True
                logging.info("Uploading again...")


        if file.uploadable:
            elapsed_time = bucket.upload(file)
            file_sizes.append(file.size)
            logging.info("Upload completed in " + str(elapsed_time) + " seconds.")



if __name__ == "__main__":
    main(sys.argv[1:])
