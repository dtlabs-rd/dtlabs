"""
This module provides an implementation of BucketService for Amazon S3.
"""

import io
from typing import Union, Dict
import boto3
from ._base import BucketService


class S3Bucket(BucketService):
    """
    Implementation of BucketService for AWS S3.
    """

    def __init__(self, bucket: str, config: Dict[str, str]):
        """
        Initializes the S3 bucket service.

        :param bucket: The S3 bucket name.
        :param config: Dictionary containing:
            - aws_access_key_id
            - aws_secret_access_key
            - region
        """
        super().__init__(bucket)

        self.config = config
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.config["aws_access_key_id"],
            aws_secret_access_key=self.config["aws_secret_access_key"],
            region_name=self.config["region"],
        )

    def upload_item(self, target_path: str, item: Union[bytes, io.BytesIO]):
        if isinstance(item, bytes):
            item = io.BytesIO(item)
        return self.client.put_object(Bucket=self.bucket, Key=target_path, Body=item)

    def upload_file(self, source_path: str, target_path: str):
        return self.client.upload_file(Filename=source_path, Bucket=self.bucket, Key=target_path)

    def delete_file(self, target_path: str):
        return self.client.delete_object(Bucket=self.bucket, Key=target_path)

    def read_file(self, target: str):
        response = self.client.get_object(Bucket=self.bucket, Key=target)
        return response["Body"].read()

    def list_folder(self, folder_path: str):
        response = self.client.list_objects_v2(
            Bucket=self.bucket, Prefix=folder_path)
        return [content["Key"] for content in response.get("Contents", [])]

    def generate_url(self, target: str, expiration=3600):
        return self.client.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": target}, ExpiresIn=expiration
        )

    def download_file(self, target_path: str, local_path: str):
        return self.client.download_file(self.bucket, target_path, local_path)

    def delete_folder(self, folder_path: str):
        objects_to_delete = self.list_folder(folder_path)
        if objects_to_delete:
            self.client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": obj}
                                    for obj in objects_to_delete]},
            )
