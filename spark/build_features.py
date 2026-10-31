"""spark job: cleans the parsed moves (silver) and builds the model features (gold).

run with: uv run python -m spark.build_features

reads bronze/moves/ (one row per move, the board features are already there because they
need python-chess, see ingest/parse_bronze.py) and the opening book from gold/openings/.

writes:
- silver/moves/: bots and ultrabullet removed, every game only once
- gold/features/: the opening book joined on fen_key, plus the features that depend on the
  earlier moves of the game, done with window functions. these give the same values as
  features/game.py (GameState), which the live consumer uses. tests/test_spark_features.py
  checks that.
"""

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from features import EXCLUDED_SPEEDS, EXCLUDED_TITLES, FEATURES
from features.game import MATERIAL_LOOKBACK, RECENT_MOVES

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")

BRONZE_MOVES = "s3a://bronze/moves"
OPENINGS = "s3a://gold/openings/openings.parquet"
SILVER_MOVES = "s3a://silver/moves"
GOLD_FEATURES = "s3a://gold/features"

PART_SIZE = 2000  # games per part, training splits train and test by part

# what train_model.py reads from gold, plus a few columns that are handy to look at
GOLD_COLUMNS = ["game_id", "ply", "batch", "part", "speed", "white_title", "black_title",
                "wp_drop", "lichess_nag", *FEATURES]


def clean(moves):
    """silver: only the games we train on, every game once."""
    # bots and ultrabullet out, the same rule as features.is_scored.
    # coalesce because most players have no title (null), and null.isin() is not false but null
    scored = moves.filter(
        ~F.coalesce(F.col("white_title"), F.lit("")).isin(list(EXCLUDED_TITLES))
        & ~F.coalesce(F.col("black_title"), F.lit("")).isin(list(EXCLUDED_TITLES))
        & ~F.col("speed").isin(list(EXCLUDED_SPEEDS))
    )

    # a game that was sent twice (kafka delivers at least once) keeps only its first copy
    first_copy = scored.groupBy("game_id").agg(F.min("seq").alias("first_seq"))
    once = (
        scored.join(first_copy, "game_id")
        .filter(F.col("seq") == F.col("first_seq"))
        .drop("first_seq")
    )
    return once.dropDuplicates(["game_id", "ply"])


def add_opening_book(moves, openings):
    """combining step: joins the opening book on the normalised fen key."""
    book = openings.select("fen_key", F.col("eco_family").alias("book_eco_family"))
    return (
        moves.join(book, "fen_key", "left")
        .withColumn("in_book", F.col("book_eco_family").isNotNull())
    )


def add_game_features(moves):
    """features that depend on earlier positions in the same game, like GameState does."""
    # all positions of one game in order, from the first one up to the current one
    game = Window.partitionBy("game_id").orderBy("pos")
    so_far = game.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    is_white = F.col("mover") == "w"

    moves = (
        moves
        # positions counted like python-chess board.ply(): the start is 0, our ply 2 is position 1
        .withColumn("pos", F.col("ply") - 1)
        .withColumn("sign", F.when(is_white, 1).otherwise(-1))
        # material from white's point of view, so both sides can be compared in one column
        .withColumn("material_white", F.col("material_balance") * F.col("sign"))

        # opening book: the last position so far that was in the book (the start counts as book)
        .withColumn("last_book_pos", F.coalesce(F.max(F.when(F.col("in_book"), F.col("pos"))).over(so_far), F.lit(0)))
        .withColumn("plies_out_of_book", F.col("pos") - F.col("last_book_pos"))
        .withColumn("eco_family", F.last(F.when(F.col("in_book"), F.col("book_eco_family")), ignorenulls=True).over(so_far))

        # best material balance so far for each side (never below 0, the start is level)
        .withColumn("best_white", F.greatest(F.lit(0), F.max("material_white").over(so_far)))
        .withColumn("best_black", F.greatest(F.lit(0), F.max(-F.col("material_white")).over(so_far)))
        .withColumn("material_drawdown", F.when(is_white, F.col("material_white") - F.col("best_white"))
                                          .otherwise(-F.col("material_white") - F.col("best_black")))
    )

    # material 4 positions ago: look it up with a join on (game, position + 4), 0 if that
    # position is not there (the start, or a move without eval)
    lookback = moves.select(
        "game_id",
        (F.col("pos") + MATERIAL_LOOKBACK).alias("pos"),
        F.col("material_white").alias("material_before"),
    )
    moves = (
        moves.join(lookback, ["game_id", "pos"], "left")
        .fillna(0, subset=["material_before"])
        .withColumn("material_change", F.col("sign") * (F.col("material_white") - F.col("material_before")))
    )

    return add_time_features(moves)


def add_time_features(moves):
    """time spent on the mover's previous move, and the average of their last 3 moves."""
    # white's first move is never a row (there is no eval before it), but white's clock at
    # ply 3 tells how long it took. ingest/build_features.py adds it the same way.
    first_ply = moves.groupBy("game_id").agg(F.min("ply").alias("first_ply"))
    first_white_move = (
        moves.filter(F.col("ply") == 3)
        .join(first_ply.filter(F.col("first_ply") == 2), "game_id")
        .select(
            "game_id",
            F.lit("w").alias("mover"),
            F.lit(0).cast("long").alias("pos"),
            F.greatest(F.lit(0.0), F.col("tc_base") - F.col("clock_before") + F.col("tc_inc")).alias("time_spent"),
        )
    )
    times = moves.select("game_id", "mover", "pos", "time_spent").unionByName(first_white_move)

    # only this player's own moves, in order
    own_moves = Window.partitionBy("game_id", "mover").orderBy("pos")
    times = (
        times
        .withColumn("prev_time_spent", F.lag("time_spent").over(own_moves))
        .withColumn("recent_time_avg", F.avg("time_spent").over(own_moves.rowsBetween(-RECENT_MOVES, -1)))
        .select("game_id", "pos", "prev_time_spent", "recent_time_avg")
    )
    # the extra first-move rows have pos 0, which no real row has, so they drop out here
    return moves.join(times, ["game_id", "pos"], "left")


def build_features(silver, openings):
    """gold: one row per move with every model feature."""
    features = add_game_features(add_opening_book(silver, openings))
    return (
        features
        .withColumn("clock", F.col("clock_before"))
        .withColumn("rating_gap", F.col("mover_elo") - F.col("opp_elo"))  # null if a rating is missing
        .withColumn("part", F.floor(F.col("seq") / PART_SIZE).cast("long"))
        .select(*GOLD_COLUMNS)
    )


def main():
    spark = SparkSession.builder.remote(SPARK_REMOTE).appName("build-features").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    moves = spark.read.parquet(BRONZE_MOVES)
    openings = spark.read.parquet(OPENINGS)

    clean(moves).write.mode("overwrite").parquet(SILVER_MOVES)
    silver = spark.read.parquet(SILVER_MOVES)
    print(f"silver: {silver.count():,} moves from {silver.select('game_id').distinct().count():,} games")

    build_features(silver, openings).write.mode("overwrite").parquet(GOLD_FEATURES)
    gold = spark.read.parquet(GOLD_FEATURES)
    print(f"gold: {gold.count():,} feature rows, parts {gold.agg(F.min('part')).first()[0]} to {gold.agg(F.max('part')).first()[0]}")

    spark.stop()


if __name__ == "__main__":
    main()