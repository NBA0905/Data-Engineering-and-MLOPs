"""model page: how good is the model, and does retraining help? everything comes from mlflow."""

import streamlit as st

from dashboard.lake import champion, training_runs

st.title("The model")
st.caption("Every DAG run trains a new model (the challenger). It only becomes the champion, the model "
           "the live consumer uses, when it scores better than the current champion on the newest games.")

runs = training_runs()
best = champion()

if runs.empty:
    st.info("No training runs in MLflow yet. Trigger the train_blunder_model DAG in Airflow.")
    st.stop()

# the champion's own training run, for the numbers at the top
latest = runs.iloc[-1]
champ_run = runs[runs["run_id"] == best["run_id"]].iloc[0] if best else latest

version, pr_auc, clock_only, base_rate = st.columns(4)
version.metric("Champion", f"v{best['version']}" if best else "none yet")
pr_auc.metric("PR-AUC", f"{champ_run['metrics.test_pr_auc']:.3f}")
clock_only.metric("Clock-only baseline", f"{champ_run['metrics.test_clock_only_pr_auc']:.3f}")
base_rate.metric("Guessing (base rate)", f"{champ_run['metrics.test_base_rate']:.3f}")
st.caption("PR-AUC rewards ranking real blunders above normal moves. Accuracy would be useless here: "
           "always saying \"no blunder\" is already 93% accurate.")

st.subheader("Every retrain")
history = runs[["metrics.test_pr_auc", "metrics.test_clock_only_pr_auc", "metrics.test_base_rate"]].copy()
history.columns = ["model", "clock only", "base rate"]
history.index = range(1, len(history) + 1)  # retrain 1, 2, 3, ... reads easier than timestamps
st.line_chart(history, x_label="retrain", y_label="PR-AUC")

table = runs[["start_time", "tags.mlflow.runName", "params.train_moves", "metrics.test_pr_auc", "metrics.lift_over_clock"]].copy()
table.columns = ["started", "run", "training moves", "PR-AUC", "x better than clock only"]
table["champion"] = runs["run_id"] == (best["run_id"] if best else None)
st.dataframe(table.iloc[::-1], hide_index=True)  # newest first