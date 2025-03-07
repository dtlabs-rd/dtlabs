import os
from ._bucket_context import BucketContext
from ._s3_bucket import S3Bucket
from ._oci_bucket import OCIBucket

class Bucket(BucketContext):
    def __init__(self, bucket: str, provider: str, **kwargs):
        """Initializes the storage service based on the specified provider (AWS or OCI)."""
        self.bucket = bucket
        self.provider = provider
        
        if provider == "OCI":
            required_keys = ["user_ocid", "tenancy_ocid", "region", "fingerprint", "key_path"]
            missing_keys = [key for key in required_keys if key not in kwargs]
            if missing_keys:
                raise ValueError(f"Missing required parameters for OCI: {missing_keys}")

            # Read key content from the provided path
            if not os.path.exists(kwargs["key_path"]):
                raise FileNotFoundError(f"Key file not found at: {kwargs['key_path']}")

            with open(kwargs["key_path"], "r") as key_file:
                key_content = key_file.read()

            service = OCIBucket(
                bucket=bucket,
                user_ocid=kwargs["user_ocid"],
                tenancy_ocid=kwargs["tenancy_ocid"],
                region=kwargs["region"],
                fingerprint=kwargs["fingerprint"],
                key_content=key_content,
            )

        elif provider == "AWS":
            required_keys = ["aws_access_key_id", "aws_secret_access_key", "region"]
            missing_keys = [key for key in required_keys if key not in kwargs]
            if missing_keys:
                raise ValueError(f"Missing required parameters for AWS: {missing_keys}")

            service = S3Bucket(
                bucket=bucket,
                aws_access_key_id=kwargs["aws_access_key_id"],
                aws_secret_access_key=kwargs["aws_secret_access_key"],
                region=kwargs["region"],
            )
        else:
            raise ValueError(f"Provider '{provider}' not supported. Use 'AWS' or 'OCI'.")

        super().__init__(service)
