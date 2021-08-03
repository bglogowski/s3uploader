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

import logging
import sys

# Only log errors from 3rd-party libraries
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('s3transfer').setLevel(logging.ERROR)


# Use ISO 8601 timestamp standard
# formatter = logging.Formatter('%(asctime)s %(pathname)s[%(process)d] (%(name)s) %(levelname)s: %(message)s',
#                              '%Y-%m-%dT%H:%M:%S%z')

# formatter for development
formatter = logging.Formatter('%(levelname)s: %(message)s', '%Y-%m-%dT%H:%M:%S%z')


# Create a handler to log to the console
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

# Create a logging object
log = logging.getLogger()

# Set the logging level to INFO
log.setLevel(logging.INFO)

# Set the destination to the console
log.addHandler(stdout_handler)
