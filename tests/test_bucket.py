import pytest
from dotenv import load_dotenv
import os
from dtlabs.cloud.buckets.bucket import Bucket

load_dotenv()

BUCKET_NAME = "aios-lib"

assert os.getenv("AWS_ACCESS_KEY_ID") is not None
assert os.getenv("AWS_SECRET_ACCESS_KEY") is not None
assert os.getenv("AWS_DEFAULT_REGION") is not None

# Test the initialization of Bucket class
def test_bucket_initialization():
    bucket = Bucket(
        bucket=BUCKET_NAME,
        provider="AWS",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_DEFAULT_REGION"),
    )
    assert bucket.bucket == BUCKET_NAME
    assert bucket.provider == "AWS"

# Test if download_file works
def test_download_file():
    bucket = Bucket(
        bucket=BUCKET_NAME,
        provider="AWS",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_DEFAULT_REGION"),
    )
    bucket.download_file("test.mp4", "test.mp4")

    assert os.path.exists("test.mp4")
    os.remove("test.mp4")

# Test if generate_url works
def test_generate_url():
    bucket = Bucket(
        bucket=BUCKET_NAME,
        provider="AWS",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_DEFAULT_REGION"),
    )
    url = bucket.generate_url("some-object", expiration=3600)
    assert isinstance(url, str)  # Check if the URL is a string
    assert "https://" in url  # Basic check to see if it's a URL
