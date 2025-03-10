import os
import pytest
from dotenv import load_dotenv
from dtlabs.cloud.buckets.bucket import Bucket

load_dotenv()

BUCKET_NAME = "aios-lib"

# Load AWS Credentials
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION")

# Load OCI Credentials
tenancy_ocid = os.getenv("OCI_TENANCY_OCID")
user_ocid = os.getenv("OCI_USER_OCID")
fingerprint = os.getenv("OCI_FINGERPRINT")
oci_region = os.getenv("OCI_REGION")
key_path = os.getenv("OCI_PRIVATE_KEY_PATH")

assert all([aws_access_key_id, aws_secret_access_key, aws_region])
assert all([tenancy_ocid, user_ocid, fingerprint, oci_region, key_path])

@pytest.fixture(params=["AWS", "OCI"])
def cloud_bucket(request):
    if request.param == "AWS":
        return Bucket(
            bucket=BUCKET_NAME,
            provider="AWS",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region=aws_region,
        )
    elif request.param == "OCI":
        return Bucket(
            bucket=BUCKET_NAME,
            provider="OCI",
            user_ocid=user_ocid,
            tenancy_ocid=tenancy_ocid,
            region=oci_region,
            fingerprint=fingerprint,
            key_path=key_path,
        )

def test_bucket_initialization(cloud_bucket):
    assert cloud_bucket.bucket == BUCKET_NAME

def test_upload_download_file(cloud_bucket):
    test_content = b"Hello, Cloud!"
    test_filename = "test_file.txt"
    
    # Upload
    cloud_bucket.upload_item(test_filename, test_content)
    
    # Download
    downloaded_content = cloud_bucket.read_file(test_filename)
    assert downloaded_content == test_content
    
    # Cleanup
    cloud_bucket.delete_file(test_filename)

def test_generate_url(cloud_bucket):
    test_filename = "test_file.txt"
    test_content = b"Hello, Cloud!"
    
    # Upload a file to generate URL
    cloud_bucket.upload_item(test_filename, test_content)
    
    # Generate URL
    url = cloud_bucket.generate_url(test_filename, expiration=3600)
    assert isinstance(url, str)
    assert "https://" in url
    
    # Cleanup
    cloud_bucket.delete_file(test_filename)

def test_list_folder(cloud_bucket):
    test_folder = "test_folder/"
    test_files = ["file1.txt", "file2.txt"]
    
    for file in test_files:
        cloud_bucket.upload_item(test_folder + file, b"Dummy content")
    
    files_in_bucket = cloud_bucket.list_folder(test_folder)
    assert set(files_in_bucket) == set([test_folder + f for f in test_files])
    
    # Cleanup
    cloud_bucket.delete_folder(test_folder)

def test_download_file(cloud_bucket):
    test_filename = "test.mp4"

    # Upload a file to download
    cloud_bucket.upload_item(test_filename, b"Fake video content")

    # Download the file
    cloud_bucket.download_file(test_filename, test_filename)

    # Verify the file was downloaded
    assert os.path.exists(test_filename)

    # Remove the downloaded file
    os.remove(test_filename)

    # Cleanup
    cloud_bucket.delete_file(test_filename)
