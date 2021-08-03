import os
import re
import threading
from timeit import default_timer as timer

import boto3.s3.transfer
import botocore.exceptions

from s3uploader import log
from s3uploader.common.crypto import Crypto
from s3uploader.common.files import LocalFile
from s3uploader.common.shared import Common


class CloudError(Exception):
    """
    Base class for exceptions related to cloud operations
    """
    def __init__(self, message):
        self.message = message


class S3Bucket(Common, Crypto):
    """
    AWS S3 Bucket class
    """

    def __init__(self, name: str, region=None):
        """
        Constructor for AWS S3 Bucket class

        :param name: the name of an AWS S3 Bucket
        :param region: an AWS region
        """

        self.name = name

        if region is None:
            self._client = boto3.client('s3')
        else:
            self._client = boto3.client('s3', region_name=region)

        self._session = boto3.session.Session()
        self._region = self._session.region_name
        self._transfer = boto3.s3.transfer.S3Transfer(self._client)
        self._exists = False
        self._objects = None
        self._object_metadata_cache = {}

    def _object_hash(self, key) -> str:
        return self._object_sha256(key)

    def _object_metadata(self, key):

        if key in self._object_metadata_cache:
            return self._object_metadata_cache[key]

        elif self.exists():
            try:
                self._object_metadata_cache[key] = self._client.head_object(Bucket=self.name, Key=key)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    log.info(f"Object [{key}] does not exist in S3 Bucket [{self.name}] in AWS Region [{self.region}]")
                    return None
                else:
                    raise CloudError(f"Unknown return code from AWS: {e.response['Error']['Code']}")
            else:
                log.info(f"Object [{key}] was found in S3 Bucket [{self.name}] in AWS Region [{self.region}]")
                return self._object_metadata_cache[key]
        else:
            raise CloudError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

    def _object_sha256(self, key):
        metadata = self._object_metadata(key)
        if metadata is None:
            return metadata
        else:
            return metadata["ResponseMetadata"]["HTTPHeaders"]["x-amz-meta-sha256"]

    def _object_size(self, key):
        metadata = self._object_metadata(key)
        if metadata is None:
            return metadata
        else:
            return metadata["ContentLength"]

    @staticmethod
    def _progress(name: str, size: float, ops: str):
        """Progress indicator for uploading and downloading files from S3

        :param name: name of Object being uploaded to S3
        :type name: str
        :param size: size of Object being uploaded to S3
        :type size: float
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
                percentage = (_seen_so_far / size) * 100
                if _msg_count % _msg_throttle == 0 or int(percentage) == 100:
                    log.info(f"{_ops} [{name}]  {_seen_so_far} / {round(size)}  ({percentage:.2f}%)")
                _msg_count += 1

        return call

    def create(self) -> bool:
        """

        :return:
        :rtype:
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
        except botocore.exceptions.ClientError as e:
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
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    log.error(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")
                    return False
                else:
                    raise CloudError(f"Unknown return code from AWS: {e.response['Error']['Code']}")
            else:
                self._exists = True
                log.info(f"S3 Bucket [{self.name}] was found in AWS Region [{self.region}]")
                return True

    def download(self, key: str, destination="/tmp") -> bool:
        """ Download Object from S3 Bucket
        *** NOT IMPLEMENTED ***

        :param key:
        :param destination:
        :return:
        """

        if self.exists():
            if "/" in key:
                full_path = f"{destination}/{'/'.join(key.split('/')[0:-1])}"
            else:
                full_path = destination

            if not os.path.isdir(full_path):
                os.makedirs(full_path)

            size = float(self._object_size(key))
            file_path = f"{destination}/{key}"

            start = timer()

            try:
                self._client.download_file(self.name, key, file_path,
                                           Callback=self._progress(key, size, "Downloading"))
                log.info(f"Download completed in {str(round(timer() - start, 2))} seconds")

            except botocore.exceptions.ClientError as e:
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
            raise CloudError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

    @property
    def name(self):
        """

        :return:
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, value):
        """

        :param value:
        :return:
        :rtype:
        """

        if self.valid_name(value):
            self._name = value
            log.debug(self._identify() + " = " + self._name)
        else:
            log.error(self._identify() + " = " + value)
            raise ValueError(f"S3 Bucket name [{value}] is not valid.")

    def objects(self) -> dict:
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
            raise CloudError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

    @property
    def region(self):
        """

        :return:
        :rtype:
        """
        return self._region

    @property
    def size(self) -> int:
        """Get the size of an S3 Bucket

        :return: the sum of the sizes of all objects in the S3 Bucket
        :rtype: int
        """

        if self.exists():
            try:
                response = self._client.list_objects(Bucket=self.name)['Contents']
                bucket_size = sum(obj['Size'] for obj in response)
                return bucket_size
            except botocore.exceptions.ClientError as e:
                log.error(e)
                return 0
        else:
            raise CloudError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

    def upload(self, file: LocalFile) -> bool:
        """

        :param file:
        :return:
        :rtype: bool
        """

        upload = False

        if self.exists():

            # s3_object_metadata = self.metadata(file.s3key)
            self._object_metadata(file.s3key)

            if self._object_metadata(file.s3key) is None:
                upload = True
            elif self._object_hash(file.s3key) == file.hash:
                log.info(f"Object [{file.s3key}] in S3 Bucket [{self.name}] has matching hash. Skipping...")
            else:
                log.info("File hash doesn't match Object hash in S3 Bucket")
                log.debug(f"File [{file.s3key}] hash = {file.hash}")
                log.debug(f"Object [{file.s3key}] hash = {self._object_hash(file.s3key)}")
                log.info("Uploading again...")
                upload = True

            if upload:
                start = timer()
                try:
                    self._transfer.upload_file(file.file_path,
                                               self.name,
                                               file.s3key,
                                               extra_args=file.metadata,
                                               callback=self._progress(file.name, file.size, "Uploading"))
                    log.info(f"Upload completed in {str(round(timer() - start, 2))} seconds")
                except botocore.exceptions.ClientError as e:
                    log.error(e)

        else:
            raise CloudError(f"S3 Bucket [{self.name}] does not exist in AWS Region [{self.region}]")

        return upload

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
            log.info(f"{log_prefix} passed validation")
            return True
