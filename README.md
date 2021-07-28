# S3 Uploader

Uploads local files to S3 while optionally retaining the local relative directory structure.


## REQUIREMENTS

This script uses authentication provided by `awscli`. To install the tool for Python 3.9, run:

```bash
pip3.9 install awscli
```

To configure your credentials and default zone with `awscli`, run:
```bash
$ aws configure
```


## EXAMPLE

Download the Python package from Github:
```bash
$ git clone https://github.com/bglogowski/s3uploader.git
```

To run `s3uploader` from the current directory:
```bash
$ python3.9 -m s3uploader --bucket amazon-s3-bucket-name -d /path/to/files -f -r --file-limit 2 --time-limit 14400
```



### Options

#### `-d [FULL PATH]`, `--directory=[FULL PATH]`
Root directory of files to upload

#### `-b [BUCKET NAME]` `--bucket=[BUCKET NAME]`
S3 Bucket name

#### `-l [NUMBER OF FILES]` `--file-limit=[NUMBER OF FILES]`
Set file limit

#### `-s [SIZE IN BYTES]` `--size-limit=[SIZE IN BYTES]`
Set size limit

#### `-t [SECONDS]` `--time-limit=[SECONDS]`
Set Time limit

#### `-f`
Use folders for S3 object keys


#### `-r`
Randomize file list
Tries to upload the files in random order

## NOTES

### S3 Folder Structure

S3 is not a filesystem, it is an object store. Therefore,
the default behavior is simply to upload files as objects
into a bucket using a flat namespace. However, owing to
the complexity of real world data, the script will optionally
organize the data in S3 using the local directory structure.

### File integrity checks

S3 Etags cannot be guaranteed to be MD5 hashes of the original
file, such as when multi-part uploads are used to create the
object. Therefore a cryptographic hash of the original file
will be stored as metadata of the S3 object.

SHA-256 was chosen because MD5 and SHA-1 are compromised and
therefore unreliable measures of file integrity. SHA-256 is
also reasonably performant as compared to alternative hash
algorithms.

### Creating a virtual environment in your home directory
```bash
$ cd ~
$ python3.9 -m venv `pwd`/s3uploader
$ cd s3uploader
$ . bin/activate
$ git clone https://github.com/bglogowski/s3uploader.git
$ cd s3uploader
$ pip install -r s3uploader/requirements.txt
$ python -m s3uploader --bucket amazon-s3-bucket-name -d /path/to/files -f -r --file-limit 2 --time-limit 14400

```