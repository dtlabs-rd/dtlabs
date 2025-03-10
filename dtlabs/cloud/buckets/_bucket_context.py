"""
This module provides a context wrapper for bucket services, allowing seamless access
to underlying storage service methods.
"""

from ._base import BucketService


class BucketContext:
    """
    A wrapper class that delegates method calls to the underlying bucket service.
    This enables seamless interaction with different storage backends (AWS, OCI, etc.).
    """

    def __init__(self, bucket_service: BucketService):
        """
        Initializes the context with a specific bucket service.

        :param bucket_service: An instance of a class implementing BucketService.
        """
        self.bucket_service = bucket_service

    def __getattr__(self, name):
        """
        Delegates attribute/method access to the underlying bucket service.

        :param name: The attribute or method name.
        :return: The corresponding attribute or method from the bucket service.
        """
        return getattr(self.bucket_service, name)

    def get_bucket_name(self) -> str:
        """
        Returns the name of the bucket associated with the service.

        :return: The bucket name as a string.
        """
        return self.bucket_service.bucket
