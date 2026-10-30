"""FEN helpers shared by the batch lane (Spark) and the live lane (consumer)."""


def fen_key(fen: str) -> str:
    """Normalised position key: piece placement, side to move and castling rights.

    Drops the en passant square and the move counters, which differ when the same
    position is reached by a different move order. Used to join on eco.json.
    """
    return " ".join(fen.split(" ")[:3])
