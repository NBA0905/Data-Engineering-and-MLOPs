"""loads data for the dashboard from rustfs and mlflow, cached so the pages stay fast."""

import os

import mlflow
import pandas as pd
import psycopg2
import streamlit as st
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from features import BLUNDER_THRESHOLD
from storage.rustfs import get_filesystem
from storage.tracking import connect

GOLD_FEATURES = "gold/features"
# only the columns the charts need, parquet then only reads those from rustfs
COLUMNS = ["game_id", "speed", "wp_drop", "clock", "mover_elo", "plies_out_of_book"]

# same names as in training/train_model.py
EXPERIMENT = "blunder-model"
MODEL_NAME = "blunder"


@st.cache_data(ttl=600)  # keep it for 10 minutes, gold only changes when the dag runs
def load_features():
    """the gold feature table from the spark job, with the blunder label added."""
    df = pd.read_parquet(GOLD_FEATURES, columns=COLUMNS, filesystem=get_filesystem())
    df["blunder"] = df["wp_drop"] >= BLUNDER_THRESHOLD
    return df


@st.cache_data(ttl=60)
def training_runs():
    """every training run in mlflow, oldest first (evaluate runs have no test_pr_auc, so they drop out)."""
    connect()
    runs = mlflow.search_runs(experiment_names=[EXPERIMENT], order_by=["start_time ASC"])
    if runs.empty or "metrics.test_pr_auc" not in runs:
        return pd.DataFrame()
    # only runs that finished: a crashed run (like the one without skops) can still have metrics
    finished = (runs["status"] == "FINISHED") & runs["metrics.test_pr_auc"].notna()
    return runs[finished].reset_index(drop=True)


@st.cache_data(ttl=60)
def champion():
    """the version that has the champion alias, or None if there is no champion yet."""
    connect()
    try:
        version = MlflowClient().get_model_version_by_alias(MODEL_NAME, "champion")
    except MlflowException:
        return None
    return {"version": int(version.version), "run_id": version.run_id}


# ---- live predictions, written to postgres by the live consumer ----

CHESS_DB_URL = os.environ.get("CHESS_DB_URL", "postgresql://airflow:airflow@localhost:5432/chess")


def current_game(channel):
    """every prediction of the game that is on this channel right now, oldest first.

    not cached on purpose: this is live data, the page asks again every few seconds.
    """
    try:
        conn = psycopg2.connect(CHESS_DB_URL)
    except psycopg2.Error:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            # the newest prediction on this channel tells us which game is on now
            cur.execute("SELECT game_id FROM predictions WHERE channel = %s ORDER BY id DESC LIMIT 1", (channel,))
            row = cur.fetchone()
            if row is None:
                return pd.DataFrame()
            cur.execute("SELECT * FROM predictions WHERE game_id = %s ORDER BY id", (row[0],))
            columns = [column.name for column in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
    except psycopg2.Error:
        return pd.DataFrame()  # for example the table does not exist yet
    finally:
        conn.close()