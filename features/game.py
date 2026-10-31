"""Per-game state for the sequential features, which depend on earlier plies.

The live consumer keeps one GameState per game id and starts a fresh one on a game
switch. The Spark job computes the same features with window functions; the parity
test checks that both give identical values.

Plies are counted as positions: the starting position is ply 0, the position after
white's first move is ply 1 (python-chess `board.ply()`).
"""

from collections import deque
from dataclasses import dataclass, field

import chess

MATERIAL_LOOKBACK = 4  # plies: the mover's last two moves and the opponent's replies
RECENT_MOVES = 3  # the mover's own moves averaged in recent_time_avg


@dataclass
class GameState:
    last_book_ply: int = 0  # the starting position counts as book
    last_eco_family: str | None = None  # ECO letter of the last book position reached
    material: dict[int, int] = field(default_factory=lambda: {0: 0})  # ply -> material, white's view
    best_material: dict[chess.Color, int] = field(
        default_factory=lambda: {chess.WHITE: 0, chess.BLACK: 0}  # best balance so far, each side's view
    )
    recent_times: dict[chess.Color, deque] = field(
        default_factory=lambda: {c: deque(maxlen=RECENT_MOVES) for c in chess.COLORS}
    )

    def observe_position(self, ply: int, in_book: bool, eco_family: str | None, material_white: int) -> None:
        """Call for every position, before its features are read."""
        if in_book:
            self.last_book_ply = ply
            self.last_eco_family = eco_family
        self.material[ply] = material_white
        self.best_material[chess.WHITE] = max(self.best_material[chess.WHITE], material_white)
        self.best_material[chess.BLACK] = max(self.best_material[chess.BLACK], -material_white)

    def record_move(self, mover: chess.Color, time_spent: float) -> None:
        """Call after every move, with the seconds the mover spent on it."""
        self.recent_times[mover].append(time_spent)

    def sequential_features(self, ply: int, mover: chess.Color) -> dict:
        sign = 1 if mover == chess.WHITE else -1
        material = sign * self.material[ply]
        before = sign * self.material.get(max(0, ply - MATERIAL_LOOKBACK), 0)
        times = self.recent_times[mover]
        return {
            "plies_out_of_book": ply - self.last_book_ply,  # 0 while in book
            "eco_family": self.last_eco_family,
            "prev_time_spent": times[-1] if times else None,  # None before the mover's first move
            "recent_time_avg": sum(times) / len(times) if times else None,
            "material_change": material - before,  # negative: lost material over the last 4 plies
            "material_drawdown": material - self.best_material[mover],  # 0 at the best point of the game
        }
