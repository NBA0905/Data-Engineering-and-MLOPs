"""Time control helpers shared by the batch lane and the live lane."""


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
