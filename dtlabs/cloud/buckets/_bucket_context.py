from ._base import BucketService

class BucketContext:
    def __init__(self, bucket_service: BucketService):
        self.bucket_service = bucket_service

    def __getattr__(self, name):
        return getattr(self.bucket_service, name)
