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
from timeit import default_timer as timer

import boto3.s3.transfer
import botocore.exceptions

logging.basicConfig(filename='/tmp/s3.log', level=logging.INFO)

class LocalFile:
    """

    """
    def __init__(self, name: str, path: str, base_path: str):

        self.base_path = base_path
        self.name = name
        self.path = path

        self._sha256 = None
        self._metadata = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value
        logging.debug(str(self.__class__.__name__) + ".name = " + self._name)

    @property
    def base_path(self):
        return self._base_path

    @base_path.setter
    def base_path(self, value):
        self._base_path = value
        logging.debug(str(self.__class__.__name__) + ".base_path = " + self._base_path)

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
        relative_path = re.sub('^/', '', relative_path)
        relative_path = re.sub('^\./', '', relative_path)
        self._relative_path = relative_path
        logging.debug(str(self.__class__.__name__) + ".relative_path = " + self._relative_path)


    @property
    def full_path(self) -> str:
        return self._full_path

    @property
    def relative_path(self) -> str:
        return self._relative_path

    @property
    def sha256(self) -> str:
        if self._sha256 is None:
            self._sha256 = self.generate_sha256_hash()
            logging.debug(str(self.__class__.__name__) + ".sha256 = " + self._sha256)
        return self._sha256

    @property
    def hash(self) -> str:
        return self.sha256

    @property
    def metadata(self) -> str:
        if self._metadata is None:
            self._metadata = {"Metadata": {"sha256": self.sha256}}
        return self._metadata

    def generate_sha256_hash(self) -> str:
        """
        Generate a SHA-256 cryptographic hash of file

        :param file_path: File from which to obtain hash
        :type file_path: str
        :return: Hexadecimal digest of hash
        :rtype: str
        """

        # Limit the amount of data read into memory
        file_buffer: int = 65536

        sha256 = hashlib.sha256()

        with open(self.full_path, 'rb') as f:
            while True:
                data = f.read(file_buffer)
                if not data:
                    break
                sha256.update(data)

        return sha256.hexdigest()


def s3_upload(file: str, bucket: str, obj_name: str, metadata: dict) -> float:
    """
    Upload a local file to Amazon S3

    :param file: The full path of the file to upload
    :type file: str
    :param bucket: The name of an S3 bucket
    :type bucket: str
    :param obj_name: The name of the S3 object to create
    :type obj_name: str
    :param metadata: Metadata to add to the S3 object
    :type metadata: dict
    :return: returns the duration of the upload in seconds
    :rtype: float
    """

    client = boto3.client('s3')
    transfer = boto3.s3.transfer.S3Transfer(client)

    size: float = float(os.path.getsize(file))

    start = timer()
    transfer.upload_file(file,
                         bucket,
                         obj_name,
                         extra_args=metadata,
                         callback=_progress(file, size, 'Upload'))
    end = timer()

    return end - start


def _progress(filename: str, size: float, ops: str):

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


def fs_get_files(directory: str):
    exclude_list: set[str] = {".DS_Store"}
    catalog: dict[str, str] = {}

    file_catalog = []

    for (path, dirs, files) in os.walk(directory):
        if not any(x in files for x in exclude_list):
            for f in files:
                file_catalog.append(LocalFile(f, path, directory))


    return file_catalog




def main(argv):
    directory: str = ""
    bucket: str = ""
    file_limit: int = 0
    time_limit: int = 0
    use_folders: bool = False
    random_shuffle:bool = False

    try:
        opts, args = getopt.getopt(argv, "hd:b:frl:t:", ["directory=", "bucket=", "file-limit=", "time-limit="])

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
            bucket = arg

        elif opt in ("-l", "--file-limit"):
            try:
                file_limit = int(arg)
            except ValueError:
                print("File upload limit is not an integer")
                sys.exit(2)

        elif opt in ("-t", "--time-limit"):
            try:
                time_limit = int(arg)
            except ValueError:
                print("Time limit is not an integer")
                sys.exit(2)

        elif opt == "-f":
            use_folders = True

        elif opt == "-r":
            random_shuffle = True

    assert isinstance(bucket, str)
    files = fs_get_files(directory)

    if random_shuffle:
        random.shuffle(files)


    client = boto3.client('s3')

    upload_count = 0
    start_time = timer()

    for file in files:

        current_time = timer()
        elapsed_seconds = round(current_time - start_time)

        if elapsed_seconds >= time_limit or upload_count >= file_limit:
            break


        if use_folders:
            key = file.relative_path
        else:
            key = file.name

        try:
            s3_metadata = client.head_object(Bucket=bucket, Key=key)['Metadata']

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print(key + " does not exist in " + bucket)

                elapsed_time = s3_upload(file.full_path, bucket, key, file.metadata)
                print("Upload completed in " + str(elapsed_time) + " seconds.")
                upload_count += 1

        else:

            s3_hash = s3_metadata['sha256']

            if s3_hash == file.sha256:
                print(key + " object exists in S3 with matching hash. Skipping...")
            else:
                print(key + " hash (" + file.sha256 + ") doesn't match S3 object (" + s3_hash + ").")
                print("Uploading again...")

                elapsed_time = s3_upload(file.full_path, bucket, key, file.metadata)
                print("Upload completed in " + str(elapsed_time) + " seconds.")
                upload_count += 1


if __name__ == "__main__":
    main(sys.argv[1:])
