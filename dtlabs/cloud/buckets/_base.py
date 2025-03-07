from abc import ABC, abstractmethod
import io
from typing import Union

class BucketService(ABC):
    def __init__(self, bucket: str):
        self.bucket = bucket

    @abstractmethod
    def upload_item(self, target_path: str, item: Union[bytes, io.BytesIO]):
        pass

    @abstractmethod
    def upload_file(self, source_path: str, target_path: str):
        pass

    @abstractmethod
    def delete_file(self, target_path: str):
        pass

    @abstractmethod
    def read_file(self, target: str):
        pass

    @abstractmethod
    def list_folder(self, folder_path: str):
        pass

    @abstractmethod
    def generate_url(self, target: str, expiration=3600):
        pass

    @abstractmethod
    def download_file(self, target_path: str, local_path: str):
        pass
    
    @abstractmethod
    def delete_folder(self, folder_path: str):
        pass
