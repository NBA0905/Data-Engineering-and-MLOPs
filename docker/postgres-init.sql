-- extra databases next to the airflow one
-- note: this only runs the very first time the postgres volume is created
CREATE DATABASE mlflow;
CREATE DATABASE chess;