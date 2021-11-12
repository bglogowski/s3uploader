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

import os
import re
import threading
from timeit import default_timer as timer

from boto3 import client
from boto3.session import Session
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

from uploader import log
from uploader.cloud.shared import CloudError
from uploader.common.crypto import Crypto
from uploader.common.files import LocalFile
from uploader.common.shared import Common

#import json

from json import dump, load



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



    def _object_metadata(self, key: str, force: bool = False):
        """

        :param key: the key of the Object in the S3 Bucket
        :return:
        """

        if key in self._object_metadata_cache and not force:
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
        _msg_throttle = 50

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
                self._object_metadata_cache = load(f)

    def save_object_metadata_cache(self):
        # https://stackoverflow.com/questions/39450065/python-3-read-write-compressed-json-objects-from-to-gzip-file
        log.info(f"Saving S3 Object metadata cache to file [{self.name}.json]")
        with open(self.name + ".json", 'w') as f:
            dump(self._object_metadata_cache, f, ensure_ascii=False, indent=4, sort_keys=True, default=str)

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
