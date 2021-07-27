# S3 Uploader
Uploads files from the local filesystem to S3 using their filename as an S3 object key.

### Command line arguments
#### Root directory of files to upload
-d --directory=

#### Bucket name
-b --bucket=

#### Set file limit
-l --file-limit=

#### Set size limit
-s --size-limit=

#### Set Time limit
-t --time-limit=

#### Use folders for S3 object keys
-f

#### Randomize file list
-r

Tries to upload the files in random order
