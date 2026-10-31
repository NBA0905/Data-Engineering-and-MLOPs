"""small helper so every part of the project talks to rustfs the same way."""

import os
from urllib.parse import urlparse

import boto3
from pyarrow import fs

# defaults
ENDPOINT = os.environ.get("RUSTFS_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("RUSTFS_ACCESS_KEY", "rustfsadmin")
SECRET_KEY = os.environ.get("RUSTFS_SECRET_KEY", "rustfsadmin")


def get_client():
    """gives a boto3 s3 client that points at rustfs instead of amazon."""
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",  # rustfs ignores this but boto3 wants one
    )


def get_filesystem() -> fs.S3FileSystem:
    """the same rustfs as a pyarrow filesystem, so pyarrow and pandas can read and write parquet there."""
    url = urlparse(ENDPOINT)
    return fs.S3FileSystem(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        endpoint_override=url.netloc,
        scheme=url.scheme,
        region="us-east-1",
    )


def resolve(path: str) -> tuple[fs.FileSystem | None, str]:
    """splits "s3://bucket/key" into (rustfs filesystem, "bucket/key"); a local path stays local (None)."""
    if path.startswith("s3://"):
        return get_filesystem(), path.removeprefix("s3://")
    return None, path
