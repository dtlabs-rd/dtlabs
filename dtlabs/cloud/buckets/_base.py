"""
This module defines the base functionality for cloud storage bucket services.
It provides an abstract interface that specific providers (AWS, OCI, etc.) will implement.
"""

from abc import ABC, abstractmethod
import io
from typing import Union


class BucketService(ABC):
    """
    Abstract class that defines the interface for cloud storage bucket operations.

    Abstract methods:
        - upload_item: Uploads an item (bytes or stream) to the bucket.
        - upload_file: Uploads a local file to the bucket.
        - delete_file: Deletes a file from the bucket.
        - read_file: Reads the content of a file from the bucket.
        - list_folder: Lists files within a specific folder in the bucket.
        - generate_url: Generates a signed URL to access a file in the bucket.
        - download_file: Downloads a file from the bucket to the local system.
        - delete_folder: Deletes a folder and all its contents from the bucket.
    """

    def __init__(self, bucket: str):
        """
        Initializes the storage service with the specified bucket name.

        :param bucket: The name of the bucket.
        """
        self.bucket = bucket

    @abstractmethod
    def upload_item(self, target_path: str, item: Union[bytes, io.BytesIO]):
        """
        Uploads an item (binary content or stream) to the specified path in the bucket.

        :param target_path: The target path in the bucket.
        :param item: The content to be uploaded, as bytes or a BytesIO stream.
        """

    @abstractmethod
    def upload_file(self, source_path: str, target_path: str):
        """
        Uploads a local file to the specified path in the bucket.

        :param source_path: The local file path.
        :param target_path: The target path in the bucket.
        """

    @abstractmethod
    def delete_file(self, target_path: str):
        """
        Deletes a file from the specified path in the bucket.

        :param target_path: The path of the file to delete.
        """

    @abstractmethod
    def read_file(self, target: str):
        """
        Reads the content of a file from the bucket.

        :param target: The path of the file in the bucket.
        :return: The file content as bytes.
        """

    @abstractmethod
    def list_folder(self, folder_path: str):
        """
        Lists the files inside a specific folder in the bucket.

        :param folder_path: The folder path in the bucket.
        :return: A list of file paths.
        """

    @abstractmethod
    def generate_url(self, target: str, expiration=3600):
        """
        Generates a signed URL for accessing a file in the bucket.

        :param target: The file path in the bucket.
        :param expiration: URL expiration time in seconds (default: 3600).
        :return: A signed URL as a string.
        """

    @abstractmethod
    def download_file(self, target_path: str, local_path: str):
        """
        Downloads a file from the bucket to the local system.

        :param target_path: The file path in the bucket.
        :param local_path: The destination path on the local system.
        """

    @abstractmethod
    def delete_folder(self, folder_path: str):
        """
        Deletes a folder and all its contents from the bucket.

        :param folder_path: The folder path to delete.
        """
