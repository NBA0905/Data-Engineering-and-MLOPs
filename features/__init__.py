"""Shared feature module: the contract between training (batch lane) and serving (live lane).

Everything here uses only what is known before the move is played: no engine evals,
no clock after the move.
"""

import chess
import pandas as pd

from features.board import board_features
from features.fen import fen_key
from features.game import GameState
from features.openings import book_features

# A move is a blunder when it drops the mover's win probability by at least this much.
BLUNDER_THRESHOLD = 0.15

# Games we neither train on nor score: bots play differently and UltraBullet is never on TV.
EXCLUDED_TITLES = {"BOT"}
EXCLUDED_SPEEDS = {"ultraBullet"}

# Model input columns, in order. `eco_family` is categorical (A-E or None).
FEATURES = [
    "mover_elo",
    "rating_gap",
    "tc_base",
    "tc_inc",
    "clock",
    "opp_clock",
    "prev_time_spent",
    "move_number",
    "material_balance",
    "piece_count",
    "in_check",
    "legal_moves",
    "in_book",
    "plies_out_of_book",
    "eco_family",
    # Phase 2b: threats on the board (engine-free proxies for "the right move is hard to find")
    "attacked_pieces",
    "en_prise_pieces",
    "en_prise_value",
    "opp_en_prise_value",
    "pinned_pieces",
    "king_zone_attacked",
    "captures_available",
    "checks_available",
    # Phase 2b: how the game has gone so far (engine-free proxies for earlier blunders)
    "material_change",
    "material_drawdown",
    "recent_time_avg",
]
PHASE_2B = FEATURES[FEATURES.index("attacked_pieces"):]


def is_scored(white_title: str | None, black_title: str | None, speed: str) -> bool:
    """Whether a game is used for training and scored live."""
    return (
        white_title not in EXCLUDED_TITLES
        and black_title not in EXCLUDED_TITLES
        and speed not in EXCLUDED_SPEEDS
    )


def model_input(rows: pd.DataFrame | list[dict]) -> pd.DataFrame:
    """The `FEATURES` columns in the dtypes the model was trained on.

    Booleans become 0/1 and `eco_family` a categorical over A-E, which
    HistGradientBoosting treats as categorical. Training and the live consumer both call this.
    """
    df = pd.DataFrame(rows)[FEATURES].copy()
    df[["in_check", "in_book"]] = df[["in_check", "in_book"]].astype(int)
    df["eco_family"] = pd.Categorical(df["eco_family"], categories=list("ABCDE"))
    return df


def compute_features(
    board: chess.Board, clocks: dict, meta: dict, state: GameState, book: dict[str, dict]
) -> dict:
    """Features for the move about to be played in `board`.

    clocks: {"mover": seconds, "opp": seconds} before the move.
    meta: {"mover_elo", "opp_elo", "tc_base", "tc_inc"}.
    Updates `state` with the current position; after the move, the caller must call
    `state.record_move(mover, time_spent)`.
    """
    ply = board.ply()
    board_f = board_features(board)
    book_f = book_features(fen_key(board.fen()), book)
    material_white = board_f["material_balance"] * (1 if board.turn == chess.WHITE else -1)
    state.observe_position(ply, book_f["in_book"], book_f["eco_family"], material_white)
    mover_elo, opp_elo = meta["mover_elo"], meta["opp_elo"]
    features = {
        "mover_elo": mover_elo,
        "rating_gap": None if mover_elo is None or opp_elo is None else mover_elo - opp_elo,
        "tc_base": meta["tc_base"],
        "tc_inc": meta["tc_inc"],
        "clock": clocks["mover"],
        "opp_clock": clocks["opp"],
        **board_f,
        "in_book": book_f["in_book"],
        **state.sequential_features(ply, board.turn),
    }
    return {name: features[name] for name in FEATURES}
