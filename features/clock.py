"""Time control helpers shared by the batch lane and the live lane."""


def time_spent(clock_before: float, clock_after: float, inc: int) -> float:
    """Seconds the mover spent on a move. The increment is added back after the move."""
    return max(0.0, clock_before - clock_after + inc)


def speed(base: int, inc: int) -> str:
    """Lichess speed category from the time control, using the estimated game duration."""
    estimate = base + 40 * inc
    if estimate < 30:
        return "ultraBullet"
    if estimate < 180:
        return "bullet"
    if estimate < 480:
        return "blitz"
    if estimate < 1500:
        return "rapid"
    return "classical"
