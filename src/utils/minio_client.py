import os
from minio import Minio


class MinioClient:
    """Lightweight MinIO client for bucket and file operations."""

    def __init__(self, config: dict):
        """Initialize MinIO client using global config and environment variables."""
        global_cfg = config["global"]
        self.bucket = global_cfg["bucket_name"].lower()
        self.path = global_cfg.get("bucket_path", "").lower()
        self.logger = None  # Prefect logger can be set externally

        endpoint = os.getenv("MINIO_ENDPOINT", f"minio:{os.getenv('MINIO_PORT', '9000')}")
        access_key = os.getenv("MINIO_ROOT_USER")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD")

        if not all([endpoint, access_key, secret_key]):
            raise EnvironmentError("Missing one or more required MinIO environment variables.")

        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )

    # ------------------------------------------------------------------
    # Bucket management
    # ------------------------------------------------------------------

    def ensure_bucket(self):
        """Create the bucket if it doesn't exist."""
        if not self.bucket:
            raise ValueError("Missing 'bucket_name' in config['global'].")

        if not self.client.bucket_exists(self.bucket):
            if self.logger:
                self.logger.info(f"Creating bucket '{self.bucket}'...")
            self.client.make_bucket(self.bucket)
        elif self.logger:
            self.logger.debug(f"Bucket '{self.bucket}' already exists.")

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    def upload(self, local_path: str, object_name: str | None = None):
        """Upload a local file to MinIO."""
        object_name = object_name or os.path.join(self.path, os.path.basename(local_path))
        if self.logger:
            self.logger.info(f"Uploading {local_path} → s3://{self.bucket}/{object_name}")
        self.client.fput_object(self.bucket, object_name, local_path)

    def download(self, object_name: str, local_path: str):
        """Download a file from MinIO."""
        if self.logger:
            self.logger.info(f"Downloading s3://{self.bucket}/{object_name} → {local_path}")
        self.client.fget_object(self.bucket, object_name, local_path)

    def list_objects(self, prefix: str | None = None) -> list[str]:
        """List all objects under a prefix (recursive)."""
        prefix = prefix.lower() if prefix else self.path
        objects = [
            obj.object_name
            for obj in self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
        ]
        if self.logger:
            self.logger.info(f"Found {len(objects)} object(s) under '{prefix}'")
        return objects
