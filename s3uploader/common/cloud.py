import boto3.s3.transfer
import botocore.exceptions
import re
import sys
import threading

from s3uploader import log
from s3uploader.common.files import LocalFile
from timeit import default_timer as timer


class CloudError(Exception):
    """Base class for exceptions in this module."""

    def __init__(self, message):
        self.message = message


class S3Bucket(object):

    def __init__(self, name: str):
        if self.valid_name(name):
            self.name = name
        else:
            log.error(self.__class__.__name__ + "." + sys._getframe().f_code.co_name + " = " + name)
            raise ValueError("S3 Bucket name " + name + " is not valid")

        self._client = boto3.client('s3')
        self._transfer = boto3.s3.transfer.S3Transfer(self._client)

        self._session = boto3.session.Session()
        self._region = self._session.region_name

        self._is_valid = False

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if self.valid_name(value):
            self._name = value
            log.debug(self.__class__.__name__ + "." + sys._getframe().f_code.co_name + " = " + self._name)
        else:
            log.error(self.__class__.__name__ + "." + sys._getframe().f_code.co_name + " = " + value)
            raise ValueError("S3 Bucket name is not valid.")

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
            raise CloudError("S3 Bucket " + self.name + " does not exist in AWS Region " + self.region)

    def metadata(self, key: str):
        if self.valid_bucket():
            try:
                response = self._client.head_object(Bucket=self.name, Key=key)['Metadata']
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    log.info(
                        "Object " + key + " does not exist in S3 Bucket " + self.name + " in AWS Region " + self.region)
                    return None
                else:
                    raise CloudError("Unknown return code from AWS: " + e.response['Error']['Code'])
            else:
                log.info("Object " + key + " was found in S3 Bucket " + self.name + " in AWS Region " + self.region)
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
                    log.error("S3 Bucket " + self.name + " does not exist in AWS Region " + self.region)
                    return False
                else:
                    raise CloudError("Unknown return code from AWS: " + e.response['Error']['Code'])
            else:
                self._is_valid = True
                log.info("S3 Bucket " + self.name + " was found in AWS Region " + self.region)
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
                log.error(e)
                return 0
        else:
            raise CloudError("S3 Bucket " + self.name + " does not exist in AWS Region " + self.region)

    @staticmethod
    def valid_name(value: str) -> bool:

        # Make sure input is a string
        if not type(value) == str:
            log.error("S3 Bucket name is not a string: " + str(value))
            return False

        # The string must be between 3 and 63 characters long
        if len(value) < 3 or len(value) > 63:
            log.error("S3 Bucket name is " + str(len(value)) + " characters long: it must be > 2 and < 64")
            return False

        # The first and last characters may not be a hyphen
        if value.startswith("-") or value.endswith("-"):
            log.error("S3 Bucket name cannot begin or end with a hyphen")
            return False

        # All characters must be lowercase alphanumeric or a hyphen
        valid_characters = re.compile(r'[^a-z0-9-]').search
        if bool(valid_characters(value)):
            log.error("S3 Bucket name contains invalid characters: " + value)
            return False
        else:
            log.debug("Name validation passed for S3 Bucket " + value)
            return True

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
                    log.info(f"{_ops} {file.name}  {_seen_so_far} / {round(file.size)}  ({percentage:.2f}%)")
                _msg_count += 1

        return call
