# Copyright (c) 2021 Bryan Glogowski
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import getopt
import hashlib
import logging
import os
import json
import random
import re
import sys
import threading
from functools import reduce
from timeit import default_timer as timer

from boto3 import client
from boto3.session import Session
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

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


class Crypto(object):
    """Base class that contains common cryptographic methods"""

    @staticmethod
    def sha256(path: str) -> str:
        """Calculate the 256-bit SHA-2 cryptographic hash (SHA-256) of a file

        :param path: the full path to the files
        :return: str
        """

        # anecdotal evidence suggests the best chunk/buffer size is 65536 (2**16) bytes
        # (optimal chunk size may be different for different hash algorithms)
        file_buffer: int = 65536

        # named constructors are much faster than new() and should be preferred
        sha256 = hashlib.sha256()

        with open(path, 'rb') as f:
            while True:
                data = f.read(file_buffer)
                if not data:
                    break
                sha256.update(data)

        log.debug(f"{__class__.__name__}.{sys._getframe().f_code.co_name} = {sha256.hexdigest()}")
        return sha256.hexdigest()


class Common(object):
    """Base class for methods used by many classes"""
    def _identify(self) -> str:
        """Get the name of the class and function executing the code

        :return: the name of the class and function executing the code
        :rtype: str
        """
        return f"{self.__class__.__name__}.{sys._getframe(1).f_code.co_name}"


class LocalFile(Common, Crypto):
    """Class for working with files on the local file system"""
    def __init__(self, name: str, path: str, base_path: str):
        """Constructor for local file class

        :param name: the name of the file
        :type name: str
        :param path: the directory which contains the file
        :type path: str
        :param base_path: the base directory that contains all the files and directories
        :type base_path: str
        """

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

    @staticmethod
    def exists(path: str) -> bool:
        """Check if the file exists in the local filesystem

        :param path: full path to the file in the local filesystem
        :type path: str
        :return: if the file exists or not
        :rtype: bool
        """
        return os.path.isfile(path)

    @property
    def base_path(self) -> str:
        """Get the local file base path

        :return: the local file base path
        :rtype: str
        """
        return self._base_path

    @base_path.setter
    def base_path(self, base_path: str):
        """Set the local file base path

        :param base_path: the local file base path
        :type base_path: str
        """
        self._base_path = base_path
        log.debug(f"{self._identify()} = {self._base_path}")

    @property
    def file_path(self) -> str:
        """Get the full path to the file (including the filename)

        :return: the full path to the file (including the filename)
        :rtype: str
        """
        return self._file_path

    @property
    def hash(self) -> str:
        """Get the cryptographic hash of the local file

        :return: the cryptographic hash of the local file
        :rtype: str
        """
        if self._hash is None:
            self._hash = self.sha256(self.file_path)
            log.debug(f"{self._identify()} = {self._hash}")
        return self._hash

    @property
    def metadata(self) -> dict:
        """Generate S3-compatible metadata for the local file

        :return: S3-compatible metadata for the local file
        :rtype: dict
        """
        if self._metadata is None:
            self._metadata = {"Metadata": {"sha256": self.hash}}
        return self._metadata

    @property
    def name(self) -> str:
        """Get the name of the file

        :return: the name of the file
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """Set the name of the file

        :param name: the name of the file
        :type name: str
        """
        self._name = name
        log.debug(f"{self._identify()} = {self._name}")

    @property
    def path(self) -> str:
        """Get the name of the directory that contains the local file

        :return: the name of the directory that contains the local file
        :rtype: str
        """
        return self._path

    @path.setter
    def path(self, path: str):
        """Set the name of the directory that contains the local file

        :param path: the name of the directory that contains the local file
        :type path: str
        """

        self._path = path
        log.debug(f"{self._identify()} = {self._path}")

        self._file_path = self.path + "/" + self.name
        log.debug(f"{self.__class__.__name__}.file_path = {self._file_path}")

        relative_path = self.file_path.replace(self.base_path, "")
        relative_path = re.sub(r'^/', '', relative_path)
        relative_path = re.sub(r'^\./', '', relative_path)
        self._relative_path = relative_path
        log.debug(f"{self.__class__.__name__}.relative_path = {self._relative_path}")

    @property
    def relative_path(self) -> str:
        """Get the path of the file relative to the base path

        :return: the path of the file relative to the base path
        :rtype: str
        """
        return self._relative_path

    @property
    def s3key(self) -> str:
        """Get the key that should be used to represent the file in an S3 Bucket

        :return: key that should be used to represent the file in an S3 Bucket
        :rtype: str
        """
        return self._s3key

    @s3key.setter
    def s3key(self, key: str):
        """Set the key that should be used to represent the file in an S3 Bucket

        :param key: key that should be used to represent the file in an S3 Bucket
        :type key: str
        """
        self._s3key = key
        log.debug(f"{self._identify()} = {self._s3key}")

    @property
    def size(self) -> int:
        """Get the size of the file in bytes

        :return: number of bytes as a float
        :rtype: int
        """
        if self._size is None:
            self._size = os.path.getsize(self.file_path)
            log.debug(f"{self._identify()} = {str(self._size)}")
        return self._size


class CloudError(Exception):
    """Base class for exceptions related to cloud operations"""

    def __init__(self, message):
        self.message = message


class AmazonError(CloudError):

    def __str__(self):
        return f"AWS {self.message}"


class S3Bucket(Common, Crypto):
    """AWS S3 Bucket class"""

    def __init__(self, name: str, region=None):
        """Constructor for AWS S3 Bucket class

        :param name: the name of an AWS S3 Bucket
        :type name: str
        :param region: an AWS region
        """

        self.name: str = name
        self._exists: bool = False
        self._objects = None

        self._object_metadata_cache = {}
        self.load_object_metadata_cache()

        if region is None:
            session = Session()
            region = session.region_name

        self.region: str = region

    def _object_hash(self, key: str) -> str:
        """

        :param key: the key of the Object in the S3 Bucket
        :type key: str
        :return: hexadecimal digest of the cryptographic hash
        :rtype: str
        """
        return self._object_hash_sha256(key)

    def _object_metadata(self, key: str):
        """

        :param key: the key of the Object in the S3 Bucket
        :return:
        """

        if key in self._object_metadata_cache:
            return self._object_metadata_cache[key]

        elif self.exists():
            try:
                self._object_metadata_cache[key] = self._client.head_object(Bucket=self.name, Key=key)
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    log.info(f"Object [{key}] does not exist in S3 Bucket [{self.name}] in AWS Region [{self.region}]")
                    return None
                else:
                    raise AmazonError(f"error code: {e.response['Error']['Code']}")
            else:
                log.info(f"Object [{key}] was found in S3 Bucket [{self.name}] in AWS Region [{self.region}]")
                self.save_object_metadata_cache()
                return self._object_metadata_cache[key]
        else:
            raise AmazonError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

    def _object_hash_sha256(self, key: str):
        """Get the SHA-256 cryptographic hash of the object in S3

        :param key: the key of the Object in the S3 Bucket
        :type key: str
        :return: the SHA-256 cryptographic hash of the object in S3
        """
        metadata = self._object_metadata(key)
        if metadata is None:
            return metadata
        else:
            return metadata["ResponseMetadata"]["HTTPHeaders"]["x-amz-meta-sha256"]

    def _object_size(self, key: str):
        """Get the size of the object in bytes

        :param key: the key of the Object in the S3 Bucket
        :type key: str
        :return: the size of the object in bytes or None
        """

        metadata = self._object_metadata(key)
        if metadata is None:
            return metadata
        else:
            return int(metadata["ContentLength"])

    @staticmethod
    def _progress(name: str, size: int, ops: str):
        """Progress indicator for uploading and downloading files

        :param name: name of Object being uploaded to S3
        :type name: str
        :param size: size of Object being uploaded to S3
        :type size: int
        :param ops: type of operation being performed
        :type ops: str
        :return: status of the operation
        :rtype: function
        """

        _seen_so_far = 0
        _ops = ops
        _lock = threading.Lock()
        _msg_count = 0
        _msg_throttle = 500

        def call(bytes_amount):
            with _lock:
                nonlocal _seen_so_far
                nonlocal _msg_count
                _seen_so_far += bytes_amount
                percentage = (float(_seen_so_far) / float(size)) * 100
                if _msg_count % _msg_throttle == 0 or int(percentage) == 100:
                    log.info(f"{_ops} [{name}]  {_seen_so_far} / {size}  ({percentage:.2f}%)")
                _msg_count += 1

        return call

    def clear_object_metadata_cache(self):
        if os.path.exists(self.name + ".json"):
            os.remove(self.name + ".json")
        self._object_metadata_cache = {}

    def load_object_metadata_cache(self):
        # https://stackoverflow.com/questions/39450065/python-3-read-write-compressed-json-objects-from-to-gzip-file
        if os.path.exists(self.name + ".json"):
            log.info(f"Loading metadata from file [{self.name}.json]")
            with open(self.name + ".json") as f:
                self._object_metadata_cache = json.load(f)

    def save_object_metadata_cache(self):
        # https://stackoverflow.com/questions/39450065/python-3-read-write-compressed-json-objects-from-to-gzip-file
        log.info(f"Saving S3 Object metadata cache to file [{self.name}.json]")
        with open(self.name + ".json", 'w') as f:
            json.dump(self._object_metadata_cache, f, ensure_ascii=False, indent=4, sort_keys=True, default=str)

    def create(self) -> bool:
        """Create an S3 Bucket

        :return: if the creation of the S3 Bucket succeeded or not
        :rtype: bool
        """

        try:
            location = {'LocationConstraint': self._region}
            self._client.create_bucket(Bucket=self.name, CreateBucketConfiguration=location)
            log.info(f"S3 Bucket [{self.name}] was created in AWS Region [{self.region}]")

            self._client.put_public_access_block(
                Bucket=self.name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                },
            )
            log.info(f"S3 Bucket [{self.name}] ACLs were successfully applied")
        except ClientError as e:
            log.error(e)
            return False
        return True

    def exists(self) -> bool:
        """Check if Bucket exists in S3

        :return: if the Bucket exists in S3 or not
        :rtype: bool
        """

        if self._exists:
            return True
        else:
            try:
                self._client.head_bucket(Bucket=self.name)
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    self._exists = False
                    log.info(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")
                    return False
                else:
                    raise AmazonError(f"error code: {e.response['Error']['Code']}")
            else:
                self._exists = True
                log.debug(f"S3 Bucket [{self.name}] was found in AWS Region [{self.region}]")
                return True

    def download(self, key: str, destination="/tmp") -> bool:
        """Download Object from S3 Bucket to local file
        *** NOT IMPLEMENTED ***

        :param key: the key of the Object in the S3 Bucket to download
        :type key: str
        :param destination: base directory in which to download the file
        :type destination: str
        :return: if the download succeeded or not
        :rtype: bool
        """

        if self.exists():
            if "/" in key:
                full_path = f"{destination}/{'/'.join(key.split('/')[0:-1])}"
            else:
                full_path = destination

            if not os.path.isdir(full_path):
                os.makedirs(full_path)

            size = self._object_size(key)
            file_path = f"{destination}/{key}"

            start = timer()

            try:
                self._client.download_file(self.name, key, file_path,
                                           Callback=self._progress(key, size, "Downloading"))
                log.info(f"Download completed in {str(round(timer() - start, 2))} seconds")

            except ClientError as e:
                log.error(e)
                return False

            s3_hash = self._object_hash(key)
            log.debug(f"S3 Object [{key}] hash = {s3_hash}")

            file_hash = self.sha256(file_path)
            log.debug(f"Local file [{file_path}] hash = {file_hash}")

            if file_hash == s3_hash:
                log.info(f"Local file hash matches S3 Object hash in S3 Bucket [{self.name}]")
                log.info(f"Download completed in {str(round(timer() - start, 2))} seconds")
                return True
            else:
                log.error(f"File [{file_path}] hash does NOT match S3 Object hash in S3 Bucket [{self.name}]")

                return False

        else:
            raise AmazonError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

    @property
    def name(self) -> str:
        """Get the name of the Bucket Object

        :return: the name of the Bucket Object
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """Set the name of the Bucket Object

        :param name: the name of the Bucket Object
        """

        if self.valid_name(name):
            self._name = name
            log.debug(f"{self._identify()} = {self._name}")
        else:
            log.error(f"{self._identify()} != {name}")
            raise ValueError(f"S3 Bucket name [{name}] is not valid.")

    def objects(self) -> dict:
        """List all the Objects in the S3 Bucket

        :return: all the Objects in the S3 Bucket
        :rtype: dict
        """

        if self.exists():

            objects = {}

            for key in self._client.list_objects(Bucket=self.name)['Contents']:
                key = key['Key']
                size = self._object_size(key)
                s3hash = self._object_hash(key)
                objects[key] = {"size": size, "hash": s3hash}
                log.info(f"S3 Object [{key}] was found in AWS Bucket [{self.name}]")

            return objects
        else:
            raise AmazonError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

    @property
    def region(self) -> str:
        """Get the region currently being used

        :return: the region currently being used
        :rtype: str
        """
        return self._region

    @region.setter
    def region(self, region: str):
        self._region = region
        log.info(f"AWS Region is set to [{self.region}]")

        self._client = client('s3', region_name=region)

        if self.exists():
            try:
                response = self._client.get_bucket_accelerate_configuration(Bucket=self.name)
            except ClientError as e:
                log.error(e)
            else:
                try:
                    if response['Status'] == "Enabled":
                        accelerated_config = Config(s3={"use_accelerate_endpoint": True})
                        self._client = client('s3', region_name=region, config=accelerated_config)
                        log.info(f"Enabled acceleration for S3 Bucket [{self.name}] in AWS Region [{self.region}]")
                except KeyError:
                    log.debug(f"Acceleration is not enabled for S3 Bucket [{self.name}] in AWS Region [{self.region}]")

    @property
    def size(self) -> int:
        """Get the total data size of the S3 Bucket

        :return: the total data size of the S3 Bucket
        :rtype: int
        """

        if self.exists():
            try:
                response = self._client.list_objects(Bucket=self.name)['Contents']
                bucket_size = sum(obj['Size'] for obj in response)
                return bucket_size
            except KeyError:
                return 0
            except ClientError as e:
                log.error(e)
                return 0
        else:
            raise AmazonError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

    def upload(self, file: LocalFile):
        """Upload a local file to AWS S3

        A note about uploads to S3:
        ==================
        Partial uploads do not exist in S3 -- either the file upload
        completes and an object appears in the Bucket or the upload
        fails and no object is visible. Additionally, uploads are
        not atomic operations in the sense that S3 is only
        eventually consistent from a user's perspective, such that
        an uploaded file may not be accessible via a query until some
        time after it has been uploaded successfully, making upload
        validation potentially unreliable on the time scale intended
        for this code to run. Therefore only minimal upload validation
        is provided.

        :param file: Object representing a file in the local filesystem
        :type file: LocalFile
        :return: if the upload completed successfully or not
        :rtype: bool
        """

        if self.exists():

            if self._object_metadata(file.s3key) is None:
                file_should_be_uploaded = True

            else:
                log.debug(f"File [{file.s3key}] hash = {file.hash}")
                log.debug(f"Object [{file.s3key}] hash = {self._object_hash(file.s3key)}")
                if self._object_hash(file.s3key) == file.hash:
                    file_should_be_uploaded = False
                    log.info(f"File hash for [{file.name}] matches "
                             f"Object hash in S3 Bucket [{self.name}]. Skipping...")

                else:
                    file_should_be_uploaded = True
                    log.info(f"File hash for [{file.name}] does not match "
                             f"Object hash in S3 Bucket [{self.name}]. Uploading again...")


            if file_should_be_uploaded:

                config = TransferConfig(multipart_threshold=1024 * 25,
                                        max_concurrency=10,
                                        multipart_chunksize=1024 * 25,
                                        use_threads=True)

                start = timer()
                try:
                    self._client.upload_file(file.file_path,
                                             self.name,
                                             file.s3key,
                                             ExtraArgs=file.metadata,
                                             Config=config,
                                             Callback=self._progress(file.name,
                                                                     file.size,
                                                                     "Uploading"))

                    if file.s3key in self._object_metadata_cache:
                        self._object_metadata_cache.pop(file.s3key)
                        log.info(f"Removed cached metadata for [{file.s3key}]")

                except ClientError as e:
                    log.error(e)
                    log.error(f"Upload failed after {str(round(timer() - start, 2))} seconds")
                    return False
                else:
                    if self._object_metadata(file.s3key) is None:
                        log.info(f"Metadata for [{file.s3key}] not found in S3 Bucket [{self.name}]")
                    else:
                        log.info(f"Metadata for [{file.s3key}] successfully cached")

                    log.info(f"Upload completed in {str(round(timer() - start, 2))} seconds")
                    return True
        else:
            raise AmazonError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

    @staticmethod
    def valid_name(name: str) -> bool:
        """Check string against S3 Bucket naming rules

        :param name: the name of an S3 Bucket
        :type name: str
        :return: whether the string is a valid S3 Bucket name or not
        :rtype: bool
        """

        # Reduce textual repetition
        log_prefix = "S3 Bucket name [" + str(name) + "]"

        # Make sure input is a string
        if not type(name) == str:
            log.error(f"{log_prefix} is not a string")
            return False

        # The string must be between 3 and 63 characters long
        if len(name) < 3:
            log.error(f"{log_prefix} is too short: it must be more than 2 characters")
            return False
        if len(name) > 63:
            log.error(f"{log_prefix} is too long: it must be fewer than 64 characters")
            return False

        # The first and last characters may not be a hyphen
        if name.startswith("-") or name.endswith("-"):
            log.error(f"{log_prefix} cannot begin or end with a hyphen")
            return False

        # All characters must be lowercase alphanumeric or a hyphen
        valid_characters = re.compile(r'[^a-z0-9-]').search
        if bool(valid_characters(name)):
            log.error(f"{log_prefix} contains invalid characters")
            return False
        else:
            log.debug(f"{log_prefix} passed validation")
            return True


def fs_get_files(directory: str) -> list:
    """Gather list of files to upload to S3

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


if __name__ == "__main__":
    main(sys.argv[1:])
