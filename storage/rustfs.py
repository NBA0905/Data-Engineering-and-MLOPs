"""small helper so every part of the project talks to rustfs the same way."""

import os

import boto3

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