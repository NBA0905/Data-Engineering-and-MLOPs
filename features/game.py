"""Per-game state for the sequential features, which depend on earlier plies.

The live consumer keeps one GameState per game id and starts a fresh one on a game
switch. The Spark job computes the same features with window functions; the parity
test checks that both give identical values.

Plies are counted as positions: the starting position is ply 0, the position after
white's first move is ply 1 (python-chess `board.ply()`).
"""

from dataclasses import dataclass, field

import chess


@dataclass
class GameState:
    last_book_ply: int = 0  # the starting position counts as book
    last_eco_family: str | None = None  # ECO letter of the last book position reached
    prev_time_spent: dict[chess.Color, float | None] = field(
        default_factory=lambda: {chess.WHITE: None, chess.BLACK: None}
    )

    def observe_position(self, ply: int, in_book: bool, eco_family: str | None) -> None:
        """Call for every position, before its features are read."""
        if in_book:
            self.last_book_ply = ply
            self.last_eco_family = eco_family

    def record_move(self, mover: chess.Color, time_spent: float) -> None:
        """Call after every move, with the seconds the mover spent on it."""
        self.prev_time_spent[mover] = time_spent

    def sequential_features(self, ply: int, mover: chess.Color) -> dict:
        return {
            "plies_out_of_book": ply - self.last_book_ply,  # 0 while in book
            "eco_family": self.last_eco_family,
            "prev_time_spent": self.prev_time_spent[mover],  # None before the mover's first move
        }
