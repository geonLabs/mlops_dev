import os
from dataclasses import dataclass, field

@dataclass(frozen=True)
class Settings:
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "111.111.111.216:9500")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "geonws")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "geonws1234")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    minio_bucket: str = os.getenv("MINIO_BUCKET", "roadsign-uploads")

    # 프론트가 별도 origin일 때 CORS 허용
    cors_allow_origins: list[str] = field(
        default_factory=lambda: os.getenv(
            "CORS_ALLOW_ORIGINS",
            "http://localhost:5173,http://localhost:5500,http://111.111.111.216:5173",
        ).split(",")
    )

settings = Settings()
