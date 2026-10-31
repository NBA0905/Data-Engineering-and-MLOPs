"""Compare the `challenger` with the `champion` on the same newest games and promote the better one.

    uv run python -m training.evaluate_model

Both versions are scored on the test parts `training.train_model` held out, so their
PR-AUCs are comparable (each run's own test metrics are not, once the data has moved on).
The challenger becomes champion when there is no champion yet or when its PR-AUC is higher.
Either way the `challenger` alias is removed, and the comparison is logged as its own run.
"""

import argparse
from pathlib import Path

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from features import model_input
from storage.tracking import TRACKING_URI, connect
from training.train_model import (CHALLENGER, CHAMPION, EXPERIMENT, FEATURES_PATH, MODEL_NAME, TEST_PARTS,
                                  load, metrics, split)


def version_for(client: MlflowClient, alias: str):
    """The model version behind an alias, or None if the alias is not set."""
    try:
        return client.get_model_version_by_alias(MODEL_NAME, alias)
    except MlflowException:
        return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, default=FEATURES_PATH)
    parser.add_argument("--test-parts", type=int, default=TEST_PARTS, help="newest parts to compare on")
    parser.add_argument("--tracking-uri", default=TRACKING_URI)
    args = parser.parse_args()

    connect(args.tracking_uri)
    client = MlflowClient(args.tracking_uri)
    challenger, champion = version_for(client, CHALLENGER), version_for(client, CHAMPION)
    if challenger is None:
        raise SystemExit(f"no {CHALLENGER} to evaluate: run training.train_model first")

    _, test = split(load(args.features), args.test_parts)
    X_test = model_input(test)
    scores = {}
    for alias, version in [(CHALLENGER, challenger), (CHAMPION, champion)]:
        if version is not None:
            model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version.version}")
            scores[alias] = metrics(test.blunder, model.predict_proba(X_test)[:, 1])

    promote = champion is None or scores[CHALLENGER]["pr_auc"] > scores[CHAMPION]["pr_auc"]

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name=f"evaluate v{challenger.version}"):
        mlflow.log_params({"challenger_version": challenger.version,
                           "champion_version": champion.version if champion else None,
                           "test_parts": args.test_parts, "test_moves": len(test)})
        for alias, values in scores.items():
            mlflow.log_metrics({f"{alias}_{k}": v for k, v in values.items()})
        mlflow.set_tag("promoted", promote)

    if promote:
        client.set_registered_model_alias(MODEL_NAME, CHAMPION, challenger.version)
    client.delete_registered_model_alias(MODEL_NAME, CHALLENGER)

    summary = ", ".join(f"{alias} v{v.version} PR-AUC {scores[alias]['pr_auc']:.4f}"
                        for alias, v in [(CHALLENGER, challenger), (CHAMPION, champion)] if v is not None)
    print(f"{summary} -> {CHAMPION} is now v{challenger.version if promote else champion.version}")


if __name__ == "__main__":
    main()
