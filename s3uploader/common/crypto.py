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

import hashlib
import sys

from s3uploader import log


class Crypto(object):
    """Base class that contains common cryptographic methods
    """

    @staticmethod
    def sha256(path: str) -> str:
        """Calculate the 256-bit SHA-2 cryptographic hash (SHA-256) of a file

        :param path:
        :return:
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
