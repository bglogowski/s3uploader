import boto3.s3.transfer
import botocore.exceptions
import re
import sys
import threading

from s3uploader import log
from s3uploader.common.files import LocalFile
from timeit import default_timer as timer


class CloudError(Exception):
    """
    Base class for exceptions related to cloud operations
    """
    def __init__(self, message):
        self.message = message


class S3Bucket(object):
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
        self._is_valid = False

    def _identify(self):
        """
        Helper function to reduce textual repetition when logging

        :return: returns the name of the class and method calling this function
        :rtype: str
        """
        return self.__class__.__name__ + "." + sys._getframe(1).f_code.co_name

    def create(self, region=None) -> bool:
        """

        :param region:
        :return:
        :rtype:
        """

        try:
            if region is None:
                self._client.create_bucket(Bucket=self.name)
            else:
                location = {'LocationConstraint': region}
                self._client.create_bucket(Bucket=self.name, CreateBucketConfiguration=location)
                self._region = region
        except botocore.exceptions.ClientError as e:
            log.error(e)
            return False
        return True

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
            raise ValueError("S3 Bucket name [" + value + "] is not valid.")

    def upload(self, file: LocalFile) -> float:
        """

        :param file:
        :return:
        :rtype:
        """

        if self.valid_bucket():
            start = timer()
            try:
                self._transfer.upload_file(file.full_path,
                                           self.name,
                                           file.s3key,
                                           extra_args=file.metadata,
                                           callback=self._progress(file, "Uploading"))
            except botocore.exceptions.ClientError as e:
                log.error(e)
                return timer() - start

            return timer() - start
        else:
            raise CloudError("S3 Bucket [" + self.name + "] does not exist in AWS Region [" + self.region + "]")

    def metadata(self, key: str):
        """

        :param key:
        :return:
        :rtype:
        """

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
        """Check if Bucket exists in S3

        :return: if the Bucket exists in S3 or not
        :rtype: bool
        """

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

        if self.valid_bucket():
            try:
                response = self._client.list_objects(Bucket=self.name)['Contents']
                bucket_size = sum(obj['Size'] for obj in response)
                return bucket_size
            except botocore.exceptions.ClientError as e:
                log.error(e)
                return 0
        else:
            raise CloudError("S3 Bucket [" + self.name + "] does not exist in AWS Region [" + self.region + "]")

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
            log.error(log_prefix + " is not a string")
            return False

        # The string must be between 3 and 63 characters long
        if len(name) < 3:
            log.error(log_prefix + " is too short: it must be more than 2 characters")
            return False
        if len(name) > 63:
            log.error(log_prefix + " is too long: it must be fewer than 64 characters")
            return False

        # The first and last characters may not be a hyphen
        if name.startswith("-") or name.endswith("-"):
            log.error(log_prefix + " cannot begin or end with a hyphen")
            return False

        # All characters must be lowercase alphanumeric or a hyphen
        valid_characters = re.compile(r'[^a-z0-9-]').search
        if bool(valid_characters(name)):
            log.error(log_prefix + " contains invalid characters")
            return False
        else:
            log.info(log_prefix + " passed validation")
            return True

    @staticmethod
    def _progress(file: LocalFile, ops: str):
        """Progress indicator for uploading and downloading files from S3

        :param file: LocalFile Object being uploaded to S3
        :type file: LocalFile
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
                percentage = (_seen_so_far / file.size) * 100
                if _msg_count % _msg_throttle == 0 or int(percentage) == 100:
                    log.info(f"{_ops} [{file.name}]  {_seen_so_far} / {round(file.size)}  ({percentage:.2f}%)")
                _msg_count += 1

        return call
