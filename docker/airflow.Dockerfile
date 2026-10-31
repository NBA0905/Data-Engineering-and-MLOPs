# airflow from the course, plus the packages our pipeline tasks need
FROM apache/airflow:3.2.0-python3.12

# the constraints file is airflow's own list of versions that work together,
# with it pip won't upgrade anything that would break airflow itself
RUN pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.12.txt \
    apache-airflow==3.2.0 \
    mlflow-skinny==3.17.2 \
    scikit-learn \
    chess \
    zstandard \
    confluent-kafka \
    boto3 \
    pyspark-client==4.1.1