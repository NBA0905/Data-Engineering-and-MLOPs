"""creates the buckets we need in rustfs, safe to run more than once.

run with: uv run python -m storage.create_buckets
"""

from storage.rustfs import get_client

# bronze = raw data, silver = cleaned, gold = ready for the model, mlflow = model files
BUCKETS = ["bronze", "silver", "gold", "mlflow"]


def main():
    s3 = get_client()
    existing = [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]

    for bucket in BUCKETS:
        if bucket in existing:
            print(f"{bucket}: already there")
        else:
            s3.create_bucket(Bucket=bucket)
            print(f"{bucket}: created")


if __name__ == "__main__":
    main()