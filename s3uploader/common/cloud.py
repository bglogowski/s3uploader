import threading
from timeit import default_timer as timer

import boto3.s3.transfer
import botocore.exceptions

from s3uploader import log
from s3uploader.common.files import LocalFile


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
        log.debug(str(self.__class__.__name__) + ".name = " + self._name)

    def upload(self, file: LocalFile) -> float:
        start = timer()
        self._transfer.upload_file(file.full_path,
                                   self.name,
                                   file.s3key,
                                   extra_args=file.metadata,
                                   callback=self._progress(file, "Upload"))
        end = timer()

        return end - start

    def metadata(self, key: str):
        try:
            metadata = self._client.head_object(Bucket=self.name, Key=key)['Metadata']
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                log.info(key + " does not exist in " + self.name)
                return None
        else:
            log.info(key + " found in " + self.name)
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
                    log.info(f"{_ops}: {file.name}  {_seen_so_far} / {round(file.size)}  ({percentage:.2f}%)")

                _msg_count += 1

        return call
