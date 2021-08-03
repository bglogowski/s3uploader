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

from s3uploader import log
from s3uploader.common.crypto import Crypto
from s3uploader.common.shared import Common


class LocalFile(Common, Crypto):
    """Class for working with files on the local file system
    """

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
    def size(self) -> float:
        """Get the size of the file in bytes

        :return: number of bytes as a float
        :rtype: float
        """
        if self._size is None:
            self._size = float(os.path.getsize(self.file_path))
            log.debug(f"{self._identify()} = {str(self._size)}")
        return self._size
