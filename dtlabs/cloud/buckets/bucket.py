"""
This module defines the Bucket class, which abstracts storage operations 
for AWS S3 and OCI Object Storage.
"""

from typing import Dict
from ._bucket_context import BucketContext
from ._s3_bucket import S3Bucket
from ._oci_bucket import OCIBucket


class Bucket(BucketContext):
    """
    A class to manage cloud storage operations for AWS S3 and OCI Object Storage.

    Attributes:
        bucket (str): The name of the storage bucket.
        provider (str): The cloud provider ('AWS' or 'OCI').
        service (Union[S3Bucket, OCIBucket]): The underlying bucket service.
    """

    def __init__(self, bucket: str, provider: str, **kwargs):
        """Initializes the storage service based on the specified provider (AWS or OCI)."""
        self.bucket = bucket
        self.provider = provider
        self.service = self._initialize_service(bucket, provider, **kwargs)
        super().__init__(self.service)

    def _initialize_service(self, bucket: str, provider: str, **kwargs):
        """Initializes the appropriate bucket service based on the provider."""
        if provider == "OCI":
            return self._initialize_oci(bucket, **kwargs)
        if provider == "AWS":
            return self._initialize_aws(bucket, **kwargs)
        raise ValueError(
            f"Provider '{provider}' not supported. Use 'AWS' or 'OCI'.")

    def _initialize_oci(self, bucket: str, **kwargs):
        required_keys = ["user_ocid", "tenancy_ocid",
                         "region", "fingerprint", "private_key"]
        missing_keys = [key for key in required_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(
                f"Missing required parameters for OCI: {missing_keys}")

        config: Dict[str, str] = {
            "user": kwargs["user_ocid"],
            "tenancy": kwargs["tenancy_ocid"],
            "region": kwargs["region"],
            "fingerprint": kwargs["fingerprint"],
            "key_content": kwargs["private_key"],
        }
        return OCIBucket(bucket=bucket, config=config)

    def _initialize_aws(self, bucket: str, **kwargs):
        required_keys = ["aws_access_key_id",
                         "aws_secret_access_key", "region"]
        missing_keys = [key for key in required_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(
                f"Missing required parameters for AWS: {missing_keys}")

        config: Dict[str, str] = {
            "aws_access_key_id": kwargs["aws_access_key_id"],
            "aws_secret_access_key": kwargs["aws_secret_access_key"],
            "region": kwargs["region"],
        }
        return S3Bucket(bucket=bucket, config=config)

    def __repr__(self):
        return f"Bucket(provider={self.provider}, bucket={self.bucket})"

    def get_provider_info(self):
        """
        Returns a dictionary with information about the bucket and provider.

        Returns:
            dict: A dictionary containing the bucket name and provider.
        """
        return {"bucket": self.bucket, "provider": self.provider}
