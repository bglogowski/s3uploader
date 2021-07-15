"""
Amazon S3 file uploader script
======================

Uploads local files to S3 while optionally retaining the
local directory structure.

S3 Folder Structure
-------------------
S3 is not a filesystem, it is an object store. Therefore,
the default behavior is simply to upload files as objects
into a bucket using a flat namespace. However, owing to
the complexity of real world data, the script will optionally
organize the data in S3 using the local directory structure.

File integrity checks
-------------------
S3 Etags cannot be guaranteed to be MD5 hashes of the original
file, such as when multi-part uploads are used to create the
object. Therefore a cryptographic hash of the original file
will be stored as metadata of the S3 object.

SHA-256 was chosen because MD5 and SHA-1 are compromised and
therefore unreliable measures of file integrity. SHA-256 is
also reasonably performant as compared to alternative hash
algorithms.

"""

import getopt
import hashlib
import os
import re
import sys
import threading
from timeit import default_timer as timer

import boto3.s3.transfer
import botocore.exceptions


def sha256_file_hash(file_path: str) -> str:
    """
    Generate a SHA-256 cryptographic hash of file

    :param file_path: File from which to obtain hash
    :type file_path: str
    :return: Hexadecimal digest of hash
    :rtype: str
    """

    # Limit the amount of data read into memory
    file_buffer: int = 65536

    sha256 = hashlib.sha256()

    with open(file_path, 'rb') as f:
        while True:
            data = f.read(file_buffer)
            if not data:
                break
            sha256.update(data)

    return sha256.hexdigest()


def s3_upload(file: str, bucket: str, obj_name: str, metadata: dict) -> float:
    """
    Upload a local file to Amazon S3

    :param file: The full path of the file to upload
    :type file: str
    :param bucket: The name of an S3 bucket
    :type bucket: str
    :param obj_name: The name of the S3 object to create
    :type obj_name: str
    :param metadata: Metadata to add to the S3 object
    :type metadata: dict
    :return: returns the duration of the upload in seconds
    :rtype: float
    """

    client = boto3.client('s3')
    transfer = boto3.s3.transfer.S3Transfer(client)

    size: float = float(os.path.getsize(file))

    start = timer()
    transfer.upload_file(file,
                         bucket,
                         obj_name,
                         extra_args=metadata,
                         callback=_progress(file, size, 'Upload'))
    end = timer()

    return end - start


def _progress(filename: str, size: float, ops: str):

    _filename = filename
    _size = size
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
            percentage = (_seen_so_far / _size) * 100

            if _msg_count % _msg_throttle == 0:
                print(f"{_ops}: {_filename}  {_seen_so_far} / {round(_size)}  ({percentage:.2f}%)")

            _msg_count += 1

    return call


def fs_get_files(directory: str, use_folders: bool) -> dict:
    exclude_list: set[str] = {".DS_Store"}
    catalog: dict[str, str] = {}

    for (path, dirs, files) in os.walk(directory):
        if not any(x in files for x in exclude_list):
            for f in files:
                full_path = path + "/" + f
                if use_folders:
                    key = full_path.replace(directory, "")
                    key = re.sub('^/', '', key)
                else:
                    key = f
                catalog[key] = full_path

    return catalog


def s3_put_files(catalog: dict, bucket: str) -> None:
    client = boto3.client('s3')

    for file in catalog:

        sha256 = sha256_file_hash(catalog[file])
        m = {"Metadata": {"sha256": sha256}}

        try:
            s3_metadata = client.head_object(Bucket=bucket, Key=file)['Metadata']

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print(file + " does not exist in " + bucket)

                elapsed_time = s3_upload(catalog[file], bucket, file, m)
                print("Upload completed in " + str(elapsed_time) + " seconds.")
                break

        else:

            s3_hash = s3_metadata['sha256']

            if s3_hash == sha256:
                print(file + " object exists in S3 with matching hash. Skipping...")
            else:
                print(file + " hash (" + sha256 + ") doesn't match S3 object (" + s3_hash + ").")
                print("Uploading again...")

                elapsed_time = s3_upload(catalog[file], bucket, file, m)
                print("Upload completed in " + str(elapsed_time) + " seconds.")
                break


def main(argv):
    directory: str = ""
    bucket: str = ""
    use_folders = False

    try:
        opts, args = getopt.getopt(argv, "hd:b:f", ["directory=", "bucket="])

    except getopt.GetoptError:
        print(sys.argv[0] + " -d <base directory> -b <S3 bucket>")
        sys.exit(2)

    for opt, arg in opts:

        if opt == "-h":
            print(sys.argv[0] + " -d <base directory> -b <S3 bucket>")
            sys.exit()

        elif opt in ("-d", "--directory"):
            directory = arg

        elif opt in ("-b", "--bucket"):
            bucket = arg

        elif opt == "-f":
            use_folders = True

    assert isinstance(bucket, str)
    files: dict[str, str] = fs_get_files(directory, use_folders)

    return s3_put_files(files, bucket)


if __name__ == "__main__":
    main(sys.argv[1:])
