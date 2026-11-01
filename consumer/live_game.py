"""Per-game state of the live lane: turns the TV feed messages of one game into feature rows.

The feed sends the position after every move (FEN, both clocks), so each message gives the
features of the move the player to move is about to make, with the same `compute_features`
the batch lane uses. The time spent on the previous move comes from the clocks of two
consecutive positions, with the same formula as the parser.

A game joined midway is warmed up from the moves and clocks so far (the producer's start
message). Lichess delays that export of a running game by a few moves, so the history can
end before the live position: the board is always taken from the FEN of a move message,
never from replaying. The start message's own FEN is not used, its move counters are reset.
"""

import chess

from features import GameState, compute_features, is_scored
from features.board import PIECE_VALUES
from features.clock import speed, time_spent
from features.fen import fen_key
from features.game import MATERIAL_LOOKBACK

COLORS = {chess.WHITE: "white", chess.BLACK: "black"}


def _material_white(board: chess.Board) -> int:
    return sum(
        value * (len(board.pieces(piece, chess.WHITE)) - len(board.pieces(piece, chess.BLACK)))
        for piece, value in PIECE_VALUES.items()
    )


class LiveGame:
    def __init__(self, meta: dict):
        """meta: the game fields the producer stamps on every message."""
        self.meta = meta
        self.state = GameState()
        self.last_ply: int | None = None  # newest position seen, its clocks are in `clocks`
        self.clocks: dict[chess.Color, float | None] = {chess.WHITE: None, chess.BLACK: None}
        self.opening: dict | None = None  # book entry of the last book position reached
        self.scored_ply = -1  # newest position already scored, to skip repeats after a reconnect

    @property
    def speed(self) -> str | None:
        base, inc = self.meta.get("tc_base"), self.meta.get("tc_inc")
        return speed(base, inc) if base is not None and inc is not None else self.meta.get("channel")

    def is_scored(self) -> bool:
        """Standard chess only, no bots, no UltraBullet: the games the model was trained on."""
        if self.meta.get("variant") not in (None, "standard"):
            return False
        return is_scored(self.meta.get("white_title"), self.meta.get("black_title"), self.speed)

    def _meta(self, mover: chess.Color) -> dict:
        return {
            "mover_elo": self.meta.get(f"{COLORS[mover]}_rating"),
            "opp_elo": self.meta.get(f"{COLORS[not mover]}_rating"),
            "tc_base": self.meta.get("tc_base"),
            "tc_inc": self.meta.get("tc_inc"),
        }

    def _observe(self, board: chess.Board, clocks: dict, book: dict[str, dict]) -> dict:
        """Features of `board`, which also moves the per-game state on to this position."""
        mover = board.turn
        entry = book.get(fen_key(board.fen()))
        if entry is not None:
            self.opening = entry
        return compute_features(board, {"mover": clocks[mover], "opp": clocks[not mover]},
                                self._meta(mover), self.state, book)

    def replay(self, moves: list[str], clocks: list[float], book: dict[str, dict]) -> None:
        """Warms the state up with the moves played before we joined (SAN, from the start).

        `clocks` holds the mover's clock after each move, like `[%clk]` in the archive. The
        starting position is not observed, as in the batch lane, whose rows start at ply 1.
        """
        board = chess.Board()
        inc = self.meta.get("tc_inc") or 0
        self.clocks = {chess.WHITE: self.meta.get("tc_base"), chess.BLACK: self.meta.get("tc_base")}
        for i, san in enumerate(moves):
            if board.ply() > 0:
                self._observe(board, self.clocks, book)
            mover = board.turn
            try:
                board.push_san(san)
            except ValueError:
                break  # should not happen, but a broken history only costs the history features
            clock_after = clocks[i] if i < len(clocks) else None
            if self.clocks[mover] is not None and clock_after is not None:
                self.state.record_move(mover, time_spent(self.clocks[mover], clock_after, inc))
            self.clocks[mover] = clock_after
            self.last_ply = board.ply()
        if board.ply() > 0:
            self._observe(board, self.clocks, book)  # the last position we know, often the live one

    def position(self, fen: str, white_clock: float | None, black_clock: float | None,
                 book: dict[str, dict]) -> dict | None:
        """Features of the move about to be played in `fen`, or None if it was scored already."""
        board = chess.Board(fen)
        ply = board.ply()
        if ply <= self.scored_ply:
            return None
        clocks = {chess.WHITE: white_clock, chess.BLACK: black_clock}

        just_moved = not board.turn
        if self.last_ply == ply - 1 and self.clocks[just_moved] is not None and clocks[just_moved] is not None:
            spent = time_spent(self.clocks[just_moved], clocks[just_moved], self.meta.get("tc_inc") or 0)
            self.state.record_move(just_moved, spent)
        elif self.last_ply is None or self.last_ply < ply - 1:
            self._fill_gap(board, ply)

        features = self._observe(board, clocks, book)
        self.last_ply, self.clocks, self.scored_ply = ply, clocks, ply
        return features

    def _fill_gap(self, board: chess.Board, ply: int) -> None:
        """Material for the plies we never saw, so material_change compares with something.

        Without it a missing ply counts as material 0. We assume nothing changed since the
        last position we saw, or, with no history at all, since the position in front of us.
        """
        seen = [p for p in self.state.material if p < ply] if self.last_ply is not None else []
        material = self.state.material[max(seen)] if seen else _material_white(board)
        for p in range(max(1, ply - MATERIAL_LOOKBACK), ply):
            self.state.material.setdefault(p, material)
