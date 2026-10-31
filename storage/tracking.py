"""connect to the mlflow server the same way from every script.

the server tells clients they can up- and download artifacts straight from rustfs, using
links to http://rustfs:9000. that name only exists inside the docker network, so from
windows every download first fails and retries. these two settings make the client go
through the mlflow server instead, which works from both sides.
"""

import os

import mlflow

TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")


def connect(uri: str = TRACKING_URI) -> None:
    os.environ.setdefault("MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD", "false")
    os.environ.setdefault("MLFLOW_ENABLE_PROXY_MULTIPART_UPLOAD", "false")
    mlflow.set_tracking_uri(uri)
