"""Reading and parsing the Lichess monthly archive.

Two passes, as in the build plan: `iter_game_texts` streams raw game text out of the
.pgn.zst file and `has_evals` is a cheap text scan; only games that pass are given to
`parse_game`, which does the slow python-chess parse and returns one row per move.
"""

import io
import math
from collections.abc import Iterator
from pathlib import Path

import chess
import chess.pgn
import zstandard

from features.clock import speed
from features.fen import fen_key

# Centipawn clamp. Mate scores are mapped to the clamp so they do not distort the label.
CP_CLAMP = 1000

# Lichess NAGs on annotated moves: ?! inaccuracy, ? mistake, ?? blunder.
LICHESS_NAGS = {chess.pgn.NAG_DUBIOUS_MOVE: "?!", chess.pgn.NAG_MISTAKE: "?", chess.pgn.NAG_BLUNDER: "??"}


def win_probability(cp: float) -> float:
    """Win probability for the side the centipawn score is from, as used by Lichess."""
    return 1 / (1 + math.exp(-0.00368208 * cp))


def iter_game_texts(path: Path) -> Iterator[str]:
    """Yield the raw PGN text of each game in a .pgn.zst file.

    The file may be cut off (we download only the first 3 GB): decoding stops at the
    first error and the incomplete last game is dropped.
    """
    buf: list[str] = []
    with path.open("rb") as fh:
        reader = zstandard.ZstdDecompressor().stream_reader(fh)
        lines = io.TextIOWrapper(reader, encoding="utf-8")
        try:
            for line in lines:
                if line.startswith("[Event ") and buf:
                    yield "".join(buf)
                    buf = []
                buf.append(line)
        except (zstandard.ZstdError, UnicodeDecodeError):
            return
    if buf:
        yield "".join(buf)


def has_evals(text: str) -> bool:
    return "[%eval" in text


def _elo(value: str | None) -> int | None:
    return int(value) if value and value.isdigit() else None


def _time_control(value: str | None) -> tuple[int, int] | None:
    if not value or "+" not in value:
        return None  # "-" is correspondence, no clock
    base, inc = value.split("+")
    return int(base), int(inc)


def _cp(node: chess.pgn.ChildNode, mover: chess.Color) -> float | None:
    """Engine score after `node`, from the mover's perspective, clamped."""
    score = node.eval()
    if score is None:
        if node.board().is_checkmate():
            return CP_CLAMP  # the mover just delivered mate
        return None
    cp = score.pov(mover).score(mate_score=100 * CP_CLAMP)
    return max(-CP_CLAMP, min(CP_CLAMP, cp))


def parse_game(text: str) -> list[dict]:
    """Parse one annotated game into one row per move.

    The first move is skipped: Lichess does not annotate the starting position, so
    there is no eval before it. Moves without an eval or clock are skipped too.
    """
    game = chess.pgn.read_game(io.StringIO(text))
    if game is None:
        return []
    h = game.headers
    tc = _time_control(h.get("TimeControl"))
    if tc is None:
        return []
    base, inc = tc

    game_meta = {
        "game_id": h.get("Site", "").rsplit("/", 1)[-1],
        "event": h.get("Event"),
        "utc_date": h.get("UTCDate"),
        "white_elo": _elo(h.get("WhiteElo")),
        "black_elo": _elo(h.get("BlackElo")),
        "white_title": h.get("WhiteTitle"),
        "black_title": h.get("BlackTitle"),
        "tc_base": base,
        "tc_inc": inc,
        "speed": speed(base, inc),
        "result": h.get("Result"),
        "termination": h.get("Termination"),
        "eco": h.get("ECO"),
        "opening": h.get("Opening"),
    }

    rows = []
    clocks = {chess.WHITE: float(base), chess.BLACK: float(base)}
    prev_cp_white: float | None = None  # eval after the previous ply, from white's view

    for ply, node in enumerate(game.mainline(), start=1):
        board_before = node.parent.board()
        mover = board_before.turn
        clock_after = node.clock()
        cp_after = _cp(node, mover)

        if ply > 1 and prev_cp_white is not None and cp_after is not None and clock_after is not None:
            cp_before = prev_cp_white if mover == chess.WHITE else -prev_cp_white
            wp_before = win_probability(cp_before)
            wp_after = win_probability(cp_after)
            clock_before = clocks[mover]
            mover_elo = game_meta["white_elo"] if mover == chess.WHITE else game_meta["black_elo"]
            opp_elo = game_meta["black_elo"] if mover == chess.WHITE else game_meta["white_elo"]
            fen_before = board_before.fen()
            rows.append(
                {
                    **game_meta,
                    "ply": ply,
                    "move_number": board_before.fullmove_number,
                    "mover": "w" if mover == chess.WHITE else "b",
                    "mover_elo": mover_elo,
                    "opp_elo": opp_elo,
                    "fen_before": fen_before,
                    "fen_key": fen_key(fen_before),
                    "move_uci": node.move.uci(),
                    "clock_before": clock_before,
                    "clock_after": clock_after,
                    "opp_clock": clocks[not mover],
                    "time_spent": max(0.0, clock_before - clock_after + inc),
                    "cp_before": float(cp_before),
                    "cp_after": float(cp_after),
                    "wp_before": wp_before,
                    "wp_after": wp_after,
                    "wp_drop": wp_before - wp_after,
                    "lichess_nag": next((LICHESS_NAGS[n] for n in node.nags if n in LICHESS_NAGS), None),
                }
            )

        if clock_after is not None:
            clocks[mover] = clock_after
        prev_cp_white = None if cp_after is None else (cp_after if mover == chess.WHITE else -cp_after)

    return rows
