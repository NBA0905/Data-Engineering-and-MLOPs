# mlflow server plus what it needs to talk to postgres and rustfs (s3)
# same mlflow version as in uv.lock so the server and our code match
FROM python:3.12-slim
RUN pip install --no-cache-dir mlflow==3.17.2 psycopg2-binary boto3