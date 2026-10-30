"""quick check that rustfs and mlflow both work, and that mlflow saves its files in rustfs.

run with: uv run python -m storage.check_setup
"""

import mlflow

from storage.rustfs import get_client

MLFLOW_URL = "http://localhost:5000"


def check_rustfs():
    s3 = get_client()
    buckets = [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]
    print("rustfs buckets:", buckets)


def check_mlflow():
    mlflow.set_tracking_uri(MLFLOW_URL)
    mlflow.set_experiment("setup-check")

    with mlflow.start_run(run_name="hello") as run:
        mlflow.log_param("just_a_test", True)
        mlflow.log_metric("answer", 42)
        # a tiny text file as artifact, this one should end up in the mlflow bucket
        mlflow.log_text("if you can read this, mlflow artifacts land in rustfs", "hello.txt")

    print("mlflow run logged:", run.info.run_id)


def check_artifact_in_rustfs():
    s3 = get_client()
    response = s3.list_objects_v2(Bucket="mlflow")
    files = [item["Key"] for item in response.get("Contents", [])]
    print("files in the mlflow bucket:", files)


if __name__ == "__main__":
    check_rustfs()
    check_mlflow()
    check_artifact_in_rustfs()