"""Per-position board features, shared by the batch lane (parser) and the live lane (consumer).

These need python-chess, so the batch lane computes them in the parser, not in Spark.
"""

import chess

PIECE_VALUES = {chess.PAWN: 1, chess.KNIGHT: 3, chess.BISHOP: 3, chess.ROOK: 5, chess.QUEEN: 9}


def board_features(board: chess.Board) -> dict:
    """Features of the position before the move, from the mover's perspective."""
    mover = board.turn
    material = sum(
        value * (len(board.pieces(piece, mover)) - len(board.pieces(piece, not mover)))
        for piece, value in PIECE_VALUES.items()
    )
    return {
        "material_balance": material,
        "piece_count": len(board.piece_map()),  # kings included
        "in_check": board.is_check(),
        "legal_moves": board.legal_moves.count(),
        "move_number": board.fullmove_number,
    }
