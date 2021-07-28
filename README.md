# S3 Uploader
Uploads files from the local filesystem to S3 using the name of the file (and optionally the relative path) as an S3 object key.

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

