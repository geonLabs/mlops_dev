from datetime import timedelta
from minio import Minio
from minio.error import S3Error
from app.core.config import settings

def get_minio_client() -> Minio:
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )

def ensure_bucket_exists() -> None:
    client = get_minio_client()
    if not client.bucket_exists(settings.minio_bucket):
        client.make_bucket(settings.minio_bucket)

class MinioUploadError(RuntimeError):
    pass

def put_object(*, object_name: str, data, length: int, content_type: str) -> None:
    client = get_minio_client()
    try:
        client.put_object(
            settings.minio_bucket,
            object_name,
            data=data,
            length=length,
            content_type=content_type,
        )
    except S3Error as e:
        raise MinioUploadError(str(e)) from e

def presign_put_object(*, object_name: str, expires_seconds: int = 3600) -> str:
    client = get_minio_client()
    try:
        return client.presigned_put_object(
            settings.minio_bucket,
            object_name,
            expires=timedelta(seconds=expires_seconds),
        )
    except S3Error as e:
        raise MinioUploadError(str(e)) from e
