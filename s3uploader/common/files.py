import os
import re

from s3uploader import log
from s3uploader.common.crypto import Crypto
from s3uploader.common.shared import Common


class LocalFile(Common, Crypto):
    """

    """
    def __init__(self, name: str, path: str, base_path: str):
        """

        :param name:
        :param path:
        :param base_path:
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
        """

        :param path:
        :return:
        :rtype:
        """
        return os.path.isfile(path)

    @property
    def base_path(self) -> str:
        """

        :return:
        :rtype:
        """
        return self._base_path

    @base_path.setter
    def base_path(self, value):
        """

        :param value:
        :return:
        :rtype:
        """
        self._base_path = value
        log.debug(f"{self._identify()} = {self._base_path}")

    @property
    def file_path(self) -> str:
        """

        :return:
        """
        return self._file_path

    @property
    def hash(self) -> str:
        """

        :return:
        """
        if self._hash is None:
            self._hash = self.sha256(self.file_path)
            log.debug(f"{self._identify()} = {self._hash}")
        return self._hash

    @property
    def metadata(self) -> dict:
        """

        :return:
        :rtype:
        """
        if self._metadata is None:
            self._metadata = {"Metadata": {"sha256": self.hash}}
        return self._metadata

    @property
    def name(self):
        """

        :return:
        :rtype:
        """
        return self._name

    @name.setter
    def name(self, value):
        """

        :param value:
        :return:
        :rtype:
        """
        self._name = value
        log.debug(f"{self._identify()} = {self._name}")

    @property
    def path(self):
        """

        :return:
        :rtype:
        """
        return self._path

    @path.setter
    def path(self, value):
        """

        :param value:
        :return:
        :rtype:
        """

        self._path = value
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
        """

        :return:
        :rtype:
        """
        return self._relative_path

    @property
    def s3key(self):
        """

        :return:
        :rtype:
        """
        return self._s3key

    @s3key.setter
    def s3key(self, value):
        """

        :param value:
        :return:
        :rtype:
        """
        self._s3key = value
        log.debug(f"{self._identify()} = {self._s3key}")

    @property
    def size(self) -> float:
        """

        :return:
        :rtype: float
        """
        if self._size is None:
            self._size = float(os.path.getsize(self.file_path))
            log.debug(f"{self._identify()} = {str(self._size)}")
        return self._size
