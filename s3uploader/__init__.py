import sys
import logging

# Only log errors from 3rd-party libraries
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('s3transfer').setLevel(logging.ERROR)


# Use ISO 8601 timestamp standard
formatter = logging.Formatter('%(asctime)s %(pathname)s[%(process)d] (%(name)s) %(levelname)s: %(message)s',
                              '%Y-%m-%dT%H:%M:%S%z')

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
