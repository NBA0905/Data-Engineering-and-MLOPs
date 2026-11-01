"""Per-position board features, shared by the batch lane (parser) and the live lane (consumer).

These need python-chess, so the batch lane computes them in the parser, not in Spark.
No engine: the threat features approximate "is there something to answer here" from
attacks and defenders only.
"""

import chess

PIECE_VALUES = {chess.PAWN: 1, chess.KNIGHT: 3, chess.BISHOP: 3, chess.ROOK: 5, chess.QUEEN: 9}
KING_AS_ATTACKER = 100  # a king can only take undefended pieces, so it never counts as cheaper


def _attacker_value(board: chess.Board, square: chess.Square) -> int:
    piece = board.piece_type_at(square)
    return KING_AS_ATTACKER if piece == chess.KING else PIECE_VALUES[piece]


def _threats(board: chess.Board, color: chess.Color) -> tuple[int, list[int], int]:
    """For `color`'s pieces (kings excluded): number attacked, values of those en prise
    (attacked and undefended, or attacked by a cheaper piece), and number pinned."""
    attacked = pinned = 0
    en_prise = []
    for square in chess.SquareSet(board.occupied_co[color] & ~board.kings):
        pinned += board.is_pinned(color, square)
        attackers = board.attackers(not color, square)
        if not attackers:
            continue
        attacked += 1
        value = PIECE_VALUES[board.piece_type_at(square)]
        cheapest = min(_attacker_value(board, a) for a in attackers)
        if cheapest < value or not board.attackers(color, square):
            en_prise.append(value)
    return attacked, en_prise, pinned


def board_features(board: chess.Board) -> dict:
    """Features of the position before the move, from the mover's perspective."""
    mover = board.turn
    material = sum(
        value * (len(board.pieces(piece, mover)) - len(board.pieces(piece, not mover)))
        for piece, value in PIECE_VALUES.items()
    )
    attacked, en_prise, pinned = _threats(board, mover)
    _, opp_en_prise, _ = _threats(board, not mover)
    king = board.king(mover)
    king_zone = chess.SquareSet(chess.BB_KING_ATTACKS[king] | chess.BB_SQUARES[king]) if king is not None else []
    legal = list(board.legal_moves)
    return {
        "material_balance": material,
        "piece_count": len(board.piece_map()),  # kings included
        "in_check": board.is_check(),
        "legal_moves": len(legal),
        "move_number": board.fullmove_number,
        "attacked_pieces": attacked,
        "en_prise_pieces": len(en_prise),
        "en_prise_value": max(en_prise, default=0),  # the most valuable piece in danger
        "opp_en_prise_value": max(opp_en_prise, default=0),  # what the mover could win
        "pinned_pieces": pinned,
        "king_zone_attacked": sum(board.is_attacked_by(not mover, s) for s in king_zone),
        "captures_available": sum(board.is_capture(m) for m in legal),
        "checks_available": sum(board.gives_check(m) for m in legal),
    }
