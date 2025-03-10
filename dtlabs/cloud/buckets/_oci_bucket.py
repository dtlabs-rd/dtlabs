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

    def __init__(self, bucket: str, config: Dict[str, str]):
        """
        Initializes the OCI bucket service.

        :param bucket: The bucket name.
        :param config: Dictionary containing:
            - user
            - tenancy
            - region
            - fingerprint
            - key_content
        """
        super().__init__(bucket)

        self.config = config
        print("CONFIG", config)
        self.client = oci.object_storage.ObjectStorageClient(self.config)
        self.namespace = self.client.get_namespace().data

    def upload_item(self, target_path: str, item: Union[bytes, io.BytesIO]):
        if isinstance(item, bytes):
            item = io.BytesIO(item)
        return self.client.put_object(self.namespace, self.bucket, target_path, item)

    def upload_file(self, source_path: str, target_path: str):
        with open(source_path, "rb") as f:
            return self.client.put_object(self.namespace, self.bucket, target_path, f)

    def delete_file(self, target_path: str):
        return self.client.delete_object(self.namespace, self.bucket, target_path)

    def read_file(self, target: str):
        response = self.client.get_object(self.namespace, self.bucket, target)
        return response.data.content

    def list_folder(self, folder_path: str):
        response = self.client.list_objects(
            self.namespace, self.bucket, prefix=folder_path)
        return [obj.name for obj in response.data.objects] if response.data.objects else []

    def generate_url(self, target: str, expiration=3600):
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

        base_url = f"https://objectstorage.{self.config['region']}.oraclecloud.com"
        return f"{base_url}{response.data.access_uri}"

    def download_file(self, target_path: str, local_path: str):
        response = self.client.get_object(
            self.namespace, self.bucket, target_path)
        with open(local_path, "wb") as file:
            for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):
                file.write(chunk)
        return response

    def delete_folder(self, folder_path: str):
        objects_to_delete = self.list_folder(folder_path)
        if objects_to_delete:
            for obj_name in objects_to_delete:
                self.client.delete_object(
                    self.namespace, self.bucket, obj_name)
