import os
import pytest
from dotenv import load_dotenv
from dtlabs.cloud.buckets.bucket import Bucket

# Carregar variáveis de ambiente
load_dotenv()

BUCKET_NAME = "aios-lib"

# Carregar credenciais AWS
AWS_CREDENTIALS = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region": os.getenv("AWS_DEFAULT_REGION"),
}

# Carregar credenciais OCI
OCI_CREDENTIALS = {
    "tenancy_ocid": os.getenv("OCI_TENANCY_OCID"),
    "user_ocid": os.getenv("OCI_USER_OCID"),
    "fingerprint": os.getenv("OCI_FINGERPRINT"),
    "region": os.getenv("OCI_REGION"),
    "key_path": os.getenv("OCI_PRIVATE_KEY_PATH"),
}

# Verificar se as credenciais estão carregadas corretamente
AWS_READY = all(AWS_CREDENTIALS.values())
OCI_READY = all(OCI_CREDENTIALS.values())

if not AWS_READY and not OCI_READY:
    pytest.skip("Nenhuma credencial válida encontrada para AWS ou OCI",
                allow_module_level=True)


@pytest.fixture(params=["AWS", "OCI"])
def cloud_bucket(request):
    """Fixture para inicializar o bucket de armazenamento na nuvem."""
    if request.param == "AWS" and AWS_READY:
        return Bucket(bucket=BUCKET_NAME, provider="AWS", **AWS_CREDENTIALS)
    elif request.param == "OCI" and OCI_READY:
        return Bucket(bucket=BUCKET_NAME, provider="OCI", **OCI_CREDENTIALS)
    else:
        pytest.skip(f"Credenciais não disponíveis para {request.param}")


def test_bucket_initialization(cloud_bucket):
    """Testa se o bucket é inicializado corretamente."""
    assert cloud_bucket.bucket == BUCKET_NAME


def test_upload_download_file(cloud_bucket):
    """Testa o upload, download e exclusão de um arquivo."""
    test_filename = "test_file.txt"
    test_content = b"Hello, Cloud!"

    cloud_bucket.upload_item(test_filename, test_content)
    downloaded_content = cloud_bucket.read_file(test_filename)

    assert downloaded_content == test_content

    # Cleanup
    cloud_bucket.delete_file(test_filename)


def test_generate_url(cloud_bucket):
    """Testa a geração de URL para um arquivo armazenado."""
    test_filename = "test_file.txt"
    test_content = b"Hello, Cloud!"

    cloud_bucket.upload_item(test_filename, test_content)
    url = cloud_bucket.generate_url(test_filename, expiration=3600)

    assert isinstance(url, str) and url.startswith("https://")

    # Cleanup
    cloud_bucket.delete_file(test_filename)


def test_list_folder(cloud_bucket):
    """Testa a listagem de arquivos dentro de uma pasta."""
    test_folder = "test_folder/"
    test_files = ["file1.txt", "file2.txt"]

    for file in test_files:
        cloud_bucket.upload_item(f"{test_folder}{file}", b"Dummy content")

    files_in_bucket = cloud_bucket.list_folder(test_folder)
    assert set(files_in_bucket) == {
        f"{test_folder}{file}" for file in test_files}

    # Cleanup
    cloud_bucket.delete_folder(test_folder)


def test_download_file(cloud_bucket):
    """Testa o download de um arquivo e verifica sua existência."""
    test_filename = "test.mp4"
    test_content = b"Fake video content"

    cloud_bucket.upload_item(test_filename, test_content)
    cloud_bucket.download_file(test_filename, test_filename)

    assert os.path.exists(test_filename)

    # Cleanup
    os.remove(test_filename)
    cloud_bucket.delete_file(test_filename)
