import random
import string
import unittest

from s3uploader.common.cloud import S3Bucket


class TestBucket(unittest.TestCase):

    def setUp(self):
        characters = string.ascii_lowercase + string.digits
        test_name = ''.join(random.choice(characters) for i in range(24))
        self.bucket = S3Bucket(test_name)

    def test_valid_name_min(self):
        characters = string.ascii_lowercase + string.digits
        test_name = ''.join(random.choice(characters) for i in range(3))
        self.assertTrue(S3Bucket.valid_name(test_name), "Name can be 3 characters")

    def test_valid_name_max(self):
        characters = string.ascii_lowercase + string.digits
        test_name = ''.join(random.choice(characters) for i in range(63))
        self.assertTrue(S3Bucket.valid_name(test_name), "Name can be 63 characters")

    def test_valid_name_too_few(self):
        characters = string.ascii_lowercase + string.digits
        test_name = ''.join(random.choice(characters) for i in range(2))
        self.assertFalse(S3Bucket.valid_name("ab"), "Name must be more than 2 characters")

    def test_valid_name_too_many(self):
        characters = string.ascii_lowercase + string.digits
        test_name = ''.join(random.choice(characters) for i in range(64))
        self.assertFalse(S3Bucket.valid_name(test_name), "Name must be no more than 63 characters")

    def test_valid_name_valid_hyphon(self):
        characters = string.ascii_lowercase + string.digits
        test_name = ''.join(random.choice(characters) for i in range(10))
        test_name += "-"
        test_name += ''.join(random.choice(characters) for i in range(10))
        self.assertTrue(S3Bucket.valid_name(test_name), "Name can have hyphons")

    def test_valid_name_begin_hyphon(self):
        characters = string.ascii_lowercase + string.digits
        test_name = "-"
        test_name += ''.join(random.choice(characters) for i in range(10))
        self.assertFalse(S3Bucket.valid_name(test_name), "Name cannot begin with hyphon")

    def test_valid_name_end_hyphon(self):
        characters = string.ascii_lowercase + string.digits
        test_name = ''.join(random.choice(characters) for i in range(10))
        test_name += "-"
        self.assertFalse(S3Bucket.valid_name(test_name), "Name cannot end with hyphon")

    def test_valid_name_uppercase(self):
        characters = string.ascii_uppercase
        test_name = ''.join(random.choice(characters) for i in range(24))
        self.assertFalse(S3Bucket.valid_name(test_name), "Name cannot have uppercase letters")

    def test_valid_name_punctuation(self):
        characters = string.punctuation.replace('-', '')
        test_name = ''.join(random.choice(characters) for i in range(24))
        self.assertFalse(S3Bucket.valid_name(test_name), "Name cannot have punctuation characters")

    def test_bucket(self):
        self.assertIsInstance(self.bucket, S3Bucket)

    def test_bucket_get_name(self):
        self.assertIsInstance(self.bucket.name, str)

    def test_bucket_set_name(self):
        characters = string.ascii_lowercase + string.digits
        test_name = ''.join(random.choice(characters) for i in range(48))
        self.bucket.name = test_name
        bucket_name = self.bucket.name
        self.assertTrue(test_name == bucket_name)


if __name__ == '__main__':
    unittest.main()
