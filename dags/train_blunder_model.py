"""retrains the blunder model on the next batch of archive games, every 6 hours.

replay the next batch into redpanda -> land it in bronze -> build the features in gold -> train
a challenger -> evaluate it against the champion. each task runs one of our scripts, the same
command you can run by hand with `uv run python -m ...`, so the dag only decides the order.
needs the opening book in gold, so run the opening_book dag once first.

the first run sends the backfill (20k games), every run after that the next batch (5k games).
trigger by hand with a smaller `games` for the demo.
"""

from airflow.sdk import Param, dag, task
from pendulum import datetime

# the project folder is mounted here, so the archive in data/raw/ is too
PROJECT = "/opt/airflow/project"
ARCHIVE = "data/raw/lichess_db_standard_rated_2026-09.part.pgn.zst"
FEATURES = "s3://gold/features"


@dag(
    dag_id="train_blunder_model",
    description="replay archive batch -> bronze -> gold features -> train -> evaluate",
    start_date=datetime(2026, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    params={
        "games": Param(5_000, type="integer", minimum=1, description="annotated games to replay per run"),
        "backfill": Param(20_000, type="integer", minimum=1, description="annotated games on the very first run"),
        "test_parts": Param(3, type="integer", minimum=1, description="newest parts (2,000 games each) held out as test set"),
        "train_parts": Param(27, type="integer", minimum=1, description="parts before the test set to train on"),
    },
    tags=["chess", "batch", "training"],
)
def train_blunder_model():

    @task.bash(cwd=PROJECT)
    def replay_archive(**context):
        """sends the next batch of annotated games to games.archive and moves the checkpoint on."""
        p = context["params"]
        return f"python -m ingest.replay_archive {ARCHIVE} --games {p['games']} --backfill {p['backfill']}"

    @task.bash(cwd=PROJECT)
    def land_in_bronze():
        """writes everything new on games.archive to bronze/archive/."""
        return "python -m ingest.land_archive"

    @task.bash(cwd=PROJECT)
    def build_features():
        """parses the new bronze games into silver/moves/ and gold/features/. the spark job replaces this later."""
        return "python -m ingest.build_gold_features"

    @task.bash(cwd=PROJECT)
    def train_model(**context):
        """trains on the newest games in gold and registers the model as challenger."""
        p = context["params"]
        return (f"python -m training.train_model --features {FEATURES} "
                f"--test-parts {p['test_parts']} --train-parts {p['train_parts']}")

    @task.bash(cwd=PROJECT)
    def evaluate_model(**context):
        """scores challenger and champion on the same test parts and promotes the better one."""
        return f"python -m training.evaluate_model --features {FEATURES} --test-parts {context['params']['test_parts']}"

    replay_archive() >> land_in_bronze() >> build_features() >> train_model() >> evaluate_model()


train_blunder_model()
