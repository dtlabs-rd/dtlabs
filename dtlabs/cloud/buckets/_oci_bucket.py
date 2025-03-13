"""
This module provides an implementation of BucketService for Oracle Cloud Infrastructure (OCI).
"""


import io
import datetime
from typing import Union, Dict
import oci
from ._base import BucketService


class OCIBucket(BucketService):
    """
    Provides methods for interacting with OCI Object Storage,
    such as uploading, downloading, and deleting files.
    """

    def __init__(self, bucket: str, config: Dict[str, str], signer=None):
        """
        Initializes the OCI bucket service.

        :param bucket: The bucket name.
        :param config: Dictionary containing:
            - user
            - tenancy
            - region
            - fingerprint
            - key_content (private key)
        :param signer: Optional signer for Instance Principals authentication.
        """
        super().__init__(bucket)
        self.bucket = bucket
        self.config = config
        self.client = None
        self.namespace = None

        try:
            if signer:
                print("Using Instance Principals authentication.")
                self.client = oci.object_storage.ObjectStorageClient(
                    {}, signer=signer)
            else:
                print("Using API Key authentication.")
                print(self.config)
                self.client = oci.object_storage.ObjectStorageClient(
                    self.config)

            self.namespace = self.client.get_namespace().data
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize OCI Object Storage client: {e}") from e

    def upload_item(self, target_path: str, item: Union[bytes, io.BytesIO]):
        """Uploads a file (bytes or stream) to the bucket."""
        if isinstance(item, bytes):
            item = io.BytesIO(item)
        try:
            return self.client.put_object(self.namespace, self.bucket, target_path, item)
        except Exception as e:
            raise RuntimeError(
                f"Upload failed for {target_path}: {e}") from e

    def upload_file(self, source_path: str, target_path: str):
        """Uploads a local file to the bucket."""
        try:
            with open(source_path, "rb", encoding="utf-8") as f:  # Specify encoding
                return self.client.put_object(self.namespace, self.bucket, target_path, f)
        except Exception as e:
            raise RuntimeError(f"File upload failed: {e}") from e

    def delete_file(self, target_path: str):
        """Deletes a file from the bucket."""
        try:
            return self.client.delete_object(self.namespace, self.bucket, target_path)
        except Exception as e:
            raise RuntimeError(
                f"Delete failed for {target_path}: {e}") from e

    def read_file(self, target: str):
        """Reads a file from the bucket and returns its content as bytes."""
        try:
            response = self.client.get_object(
                self.namespace, self.bucket, target)
            return response.data.content
        except Exception as e:
            raise RuntimeError(f"Failed to read file {target}: {e}") from e

    def list_folder(self, folder_path: str):
        """Lists all objects in a folder (prefix match)."""
        try:
            response = self.client.list_objects(
                self.namespace, self.bucket, prefix=folder_path)
            return [obj.name for obj in response.data.objects] if response.data.objects else []
        except Exception as e:
            raise RuntimeError(
                f"Failed to list folder {folder_path}: {e}") from e

    def generate_url(self, target: str, expiration=3600):
        """Generates a pre-authenticated URL to access a file."""
        try:
            expiration_time = datetime.datetime.utcnow(
            ) + datetime.timedelta(seconds=expiration)
            par_details = oci.object_storage.models.CreatePreauthenticatedRequestDetails(
                name=f"par_{target}",
                access_type="ObjectRead",
                time_expires=expiration_time,
                object_name=target,
            )
            response = self.client.create_preauthenticated_request(
                namespace_name=self.namespace,
                bucket_name=self.bucket,
                create_preauthenticated_request_details=par_details,
            )

            base_endpoint = f"https://objectstorage.{self.config['region']}.oraclecloud.com"

            base_url = f"{base_endpoint}/n/{self.namespace}/b/{self.bucket}/o"
            return f"{base_url}{response.data.access_uri}"
        except Exception as e:
            raise RuntimeError(
                f"Failed to generate URL for {target}: {e}") from e

    def download_file(self, target_path: str, local_path: str):
        """Downloads a file from OCI Object Storage and saves it locally."""
        try:
            response = self.client.get_object(
                self.namespace, self.bucket, target_path)
            with open(local_path, "wb") as file:
                for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):
                    file.write(chunk)
            return response
        except Exception as e:
            raise RuntimeError(
                f"Download failed for {target_path}: {e}") from e

    def delete_folder(self, folder_path: str):
        """Deletes all objects in a given folder (prefix match)."""
        try:
            objects_to_delete = self.list_folder(folder_path)
            if objects_to_delete:
                for obj_name in objects_to_delete:
                    self.client.delete_object(
                        self.namespace, self.bucket, obj_name)
        except Exception as e:
            raise RuntimeError(
                f"Failed to delete folder {folder_path}: {e}") from e
