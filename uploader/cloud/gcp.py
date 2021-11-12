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

from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import os
import re

from uploader import log
from uploader.common.crypto import Crypto
from uploader.common.shared import Common


class GoogleBucket(Common, Crypto):
    """AWS S3 Bucket class"""

    def __init__(self, name: str, project: str, region=None):

        self.name = name
        self.project = project

        credentials_dict = {
            'type': 'service_account',
            'client_id': os.environ['BACKUP_CLIENT_ID'],
            'client_email': os.environ['BACKUP_CLIENT_EMAIL'],
            'private_key_id': os.environ['BACKUP_PRIVATE_KEY_ID'],
            'private_key': os.environ['BACKUP_PRIVATE_KEY'],
        }

        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict)


        self.client = storage.Client(
            credentials=self.credentials,
            project=self.project)

        self.bucket = self.client.get_bucket(self.name)

        # blob = bucket.blob('myfile')
        # blob.upload_from_filename('myfile')

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

    @staticmethod
    def valid_name(name: str) -> bool:
        """Check string against S3 Bucket naming rules

        :param name: the name of an S3 Bucket
        :type name: str
        :return: whether the string is a valid S3 Bucket name or not
        :rtype: bool
        """

        # https://cloud.google.com/storage/docs/naming-buckets

        # Reduce textual repetition
        log_prefix = "Google Bucket name [" + str(name) + "]"

        # Make sure input is a string
        if not type(name) == str:
            log.error(f"{log_prefix} is not a string")
            return False

        # The string must be between 3 and 63 characters long

        if len(name) < 3:
            log.error(f"{log_prefix} is too short: it must be more than 2 characters")
            return False
        if len(name) > 63:
            if name.contains("."):
                # Names containing dots can contain up to 222 characters, but each dot-separated component can be no longer than 63 characters.
                if len(name) > 222:
                    log.error(f"{log_prefix} is too long: it must be fewer than 223 characters")
                    return False
                else:
                    tokens = name.split(".")
                    for t in tokens:
                        if len(t) > 63:
                            log.error(f"{log_prefix} token [{t}] is too long: it must be fewer than 64 characters")
                            return False
            else:
                log.error(f"{log_prefix} is too long: it must be fewer than 64 characters")
                return False

        # The first and last characters may not be a hyphen
        if name.startswith("-") or name.endswith("-"):
            log.error(f"{log_prefix} cannot begin or end with a hyphen")
            return False

        if name.startswith("goog"):
            log.error(f"{log_prefix} cannot begin with [goog]")
            return False

        if name.lower().contains("google") or name.lower().contains("g00gle"):
            log.error(f"{log_prefix} cannot contain substrings like [google]")
            return False

        # All characters must be lowercase alphanumeric or a hyphen
        valid_characters = re.compile(r'[^a-z0-9-_.]').search
        if bool(valid_characters(name)):
            log.error(f"{log_prefix} contains invalid characters")
            return False
        else:
            log.debug(f"{log_prefix} passed validation")
            return True
