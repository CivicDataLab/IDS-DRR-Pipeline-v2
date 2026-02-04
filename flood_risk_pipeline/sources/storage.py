"""
Storage resources for data lake and file management.

Supports:
- S3 (AWS)
- GCS (Google Cloud Storage)
- Local filesystem (for development)
"""

import dagster as dg
from dagster import ConfigurableResource
from typing import Optional
from pathlib import Path


class CloudStorageResource(ConfigurableResource):
    """Cloud storage resource for S3 or GCS."""

    provider: str = "local"  # can be 's3' or any other storage option
    bucket: Optional[str] = None
    prefix: str = "flood_risk_pipeline"

    # AWS S3 settings
    aws_region: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    # GCS settings
    gcs_project: Optional[str] = None
    gcs_credentials_path: Optional[str] = None

    # Local settings
    local_base_path: str = "./data"

    def get_client(self):
        """Get storage client based on provider."""
        if self.provider == "s3":
            # TODO: Initialize boto3 client
            # import boto3
            # return boto3.client('s3', region_name=self.aws_region)
            pass
        elif self.provider == "gcs":
            # TODO: Initialize GCS client
            # from google.cloud import storage
            # return storage.Client(project=self.gcs_project)
            pass
        return None

    def upload_file(self, local_path: str, remote_key: str):
        """Upload file to cloud storage."""
        if self.provider == "local":
            dest = Path(self.local_base_path) / self.prefix / remote_key
            dest.parent.mkdir(parents=True, exist_ok=True)
            # Copy file
            import shutil
            shutil.copy(local_path, dest)
            return str(dest)
        # TODO: Implement cloud upload
        return None

    def download_file(self, remote_key: str, local_path: str):
        """Download file from cloud storage."""
        # TODO: Implement cloud download
        pass

    def list_files(self, prefix: str) -> list[str]:
        """List files under a prefix."""
        if self.provider == "local":
            path = Path(self.local_base_path) / self.prefix / prefix
            if path.exists():
                return [str(p) for p in path.rglob("*") if p.is_file()]
        return []


class StagingStorageResource(ConfigurableResource):
    """Staging area for intermediate data."""

    base_path: str = "./staging"

    def get_daily_path(self, date: str) -> Path:
        """Get staging path for daily data."""
        path = Path(self.base_path) / "daily" / date
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_weekly_path(self, week: str) -> Path:
        """Get staging path for weekly data."""
        path = Path(self.base_path) / "weekly" / week
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_monthly_path(self, state: str, month: str) -> Path:
        """Get staging path for monthly state data."""
        path = Path(self.base_path) / "monthly" / state / month
        path.mkdir(parents=True, exist_ok=True)
        return path


# Export configured resources
storage_resources = {
    "cloud_storage": CloudStorageResource(
        provider="local",
        local_base_path="./data",
    ),  
    "staging": StagingStorageResource(
        base_path="./staging",
    ),
}
