import hashlib
import sys

from s3uploader import log


class Crypto(object):

    @staticmethod
    def sha256(path: str) -> str:

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
