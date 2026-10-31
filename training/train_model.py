"""Train the blunder model, log it to MLflow and register it as the `challenger`.

    uv run python -m training.train_model

Same setup as the notebook: bots and UltraBullet dropped, the newest parts held out as the
test set, `HistGradientBoostingClassifier` without class weights on `features.FEATURES`.
The new version gets the `challenger` alias; `training.evaluate_model` then decides whether
it replaces the `champion`, which is what the live consumer loads.
"""

import argparse
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score

from features import BLUNDER_THRESHOLD, FEATURES, is_scored, model_input
from storage.tracking import TRACKING_URI, connect

MODEL_NAME = "blunder"
CHAMPION = "champion"
CHALLENGER = "challenger"
EXPERIMENT = "blunder-model"
FEATURES_PATH = Path("data/parquet/features")
TEST_PARTS = 6
PARAMS = dict(max_iter=500, early_stopping=True, random_state=0)
# MLflow saves sklearn models with skops, which only loads types on an allow list. These are
# the HistGradientBoosting internals; the file is one we write ourselves.
SKOPS_TRUSTED = [
    "functools.partial",
    "sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor",
    "sklearn.utils.validation.check_array",
]


def load(path: Path) -> pd.DataFrame:
    """Scored moves with the label, one row per move."""
    df = pd.read_parquet(path, columns=["game_id", "part", "speed", "white_title", "black_title", "wp_drop", *FEATURES])
    keep = np.array([is_scored(w, b, s) for w, b, s in zip(df.white_title, df.black_title, df.speed)])
    df = df[keep].reset_index(drop=True)
    df["blunder"] = df.wp_drop >= BLUNDER_THRESHOLD
    return df


def split(df: pd.DataFrame, test_parts: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Train on the older parts, test on the newest `test_parts` (parts are in archive order)."""
    first_test = df.part.max() + 1 - test_parts
    return df[df.part < first_test], df[df.part >= first_test]


def metrics(y: pd.Series, p: np.ndarray) -> dict:
    return {"pr_auc": average_precision_score(y, p), "roc_auc": roc_auc_score(y, p),
            "brier": brier_score_loss(y, p), "mean_p": float(p.mean())}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, default=FEATURES_PATH)
    parser.add_argument("--test-parts", type=int, default=TEST_PARTS, help="newest parts held out as test set")
    parser.add_argument("--tracking-uri", default=TRACKING_URI)
    args = parser.parse_args()

    train, test = split(load(args.features), args.test_parts)
    X_train, X_test = model_input(train), model_input(test)
    print(f"train {len(train):,} moves, test {len(test):,} moves ({test.game_id.nunique():,} games)")

    connect(args.tracking_uri)
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run() as run:
        clock_model = HistGradientBoostingClassifier(**PARAMS).fit(train[["clock"]], train.blunder)
        model = HistGradientBoostingClassifier(**PARAMS).fit(X_train, train.blunder)
        p = model.predict_proba(X_test)[:, 1]
        test_metrics = metrics(test.blunder, p)
        clock_pr_auc = average_precision_score(test.blunder, clock_model.predict_proba(test[["clock"]])[:, 1])

        mlflow.log_params({**PARAMS, "class_weight": None, "blunder_threshold": BLUNDER_THRESHOLD,
                           "n_features": len(FEATURES), "test_parts": args.test_parts,
                           "train_moves": len(train), "test_moves": len(test), "n_iter": model.n_iter_})
        mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})
        mlflow.log_metrics({"test_base_rate": test.blunder.mean(), "test_clock_only_pr_auc": clock_pr_auc,
                            "lift_over_clock": test_metrics["pr_auc"] / clock_pr_auc})
        # The feature order is the training/serving contract: the consumer can check it here.
        mlflow.set_tag("features", ",".join(FEATURES))

        sample = X_test.head(5)
        info = mlflow.sklearn.log_model(model, name="model", registered_model_name=MODEL_NAME,
                                        signature=infer_signature(sample, model.predict_proba(sample)),
                                        input_example=sample, skops_trusted_types=SKOPS_TRUSTED)

    version = info.registered_model_version
    MlflowClient(args.tracking_uri).set_registered_model_alias(MODEL_NAME, CHALLENGER, version)
    print(f"run {run.info.run_id}: PR-AUC {test_metrics['pr_auc']:.3f} "
          f"(clock only {clock_pr_auc:.3f}), registered {MODEL_NAME} v{version} as {CHALLENGER}")


if __name__ == "__main__":
    main()
