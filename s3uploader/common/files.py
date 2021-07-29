import hashlib
import os
import re
import sys

from s3uploader import log


class LocalFile(object):
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

    def _identify(self):
        """

        :return:
        :rtype:
        """
        return self.__class__.__name__ + "." + sys._getframe(1).f_code.co_name

    @staticmethod
    def exists(path: str) -> bool:
        """

        :param path:
        :return:
        :rtype:
        """
        return os.path.isfile(path)

    @staticmethod
    def sha256(path: str) -> str:
        """

        :param path:
        :return:
        :rtype:
        """

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
        log.debug(self._identify() + " = " + self._base_path)

    @property
    def full_path(self) -> str:
        """

        :return:
        """
        return self._full_path

    @property
    def hash(self) -> str:
        """

        :return:
        """
        if self._hash is None:
            self._hash = self.sha256(self.full_path)
            log.debug(self._identify() + " = " + self._hash)
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
        log.debug(self._identify() + " = " + self._name)

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
        log.debug(self._identify() + " = " + self._s3key)

    @property
    def size(self) -> float:
        """

        :return:
        :rtype: float
        """
        if self._size is None:
            self._size = float(os.path.getsize(self.full_path))
            log.debug(self._identify() + " = " + str(self._size))
        return self._size

    @property
    def uploadable(self):
        """

        :return:
        :rtype:
        """
        return self._uploadable

    @uploadable.setter
    def uploadable(self, value):
        """

        :param value:
        :return:
        :rtype:
        """

        if type(value) is bool:
            self._uploadable = value
            log.debug(self._identify() + " = " + str(self._uploadable))
        else:
            raise ValueError("Cannot set " + self._identify() + " to non-boolean type.")
