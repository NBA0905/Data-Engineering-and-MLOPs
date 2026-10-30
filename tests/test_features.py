import chess

from features import FEATURES, GameState, compute_features, is_scored
from features.board import board_features
from features.clock import time_spent
from features.fen import fen_key


def board_after(*sans: str) -> chess.Board:
    board = chess.Board()
    for san in sans:
        board.push_san(san)
    return board


# A tiny stand-in for the opening book: 1. e4 and 1. e4 e5 are in book.
BOOK = {
    fen_key(board_after("e4").fen()): {"eco_family": "B"},
    fen_key(board_after("e4", "e5").fen()): {"eco_family": "C"},
}
META = {"mover_elo": 1500, "opp_elo": 1600, "tc_base": 180, "tc_inc": 2}


def test_board_features_starting_position():
    assert board_features(chess.Board()) == {
        "material_balance": 0,
        "piece_count": 32,
        "in_check": False,
        "legal_moves": 20,
        "move_number": 1,
        "attacked_pieces": 0,
        "en_prise_pieces": 0,
        "en_prise_value": 0,
        "opp_en_prise_value": 0,
        "pinned_pieces": 0,
        "king_zone_attacked": 0,
        "captures_available": 0,
        "checks_available": 0,
    }


def test_en_prise_means_undefended_or_attacked_by_a_cheaper_piece():
    undefended = board_features(chess.Board("4k3/8/3p4/4N3/8/8/8/4K3 w - - 0 1"))
    defended = board_features(chess.Board("4k3/8/3p4/4N3/3P4/8/8/4K3 w - - 0 1"))  # pawn still cheaper
    for f in (undefended, defended):
        assert (f["attacked_pieces"], f["en_prise_pieces"], f["en_prise_value"]) == (1, 1, 3)


def test_pin_and_king_zone():
    # Knight on e2 pinned by the rook on e7; attacked by a dearer piece and defended, so not en prise.
    f = board_features(chess.Board("4k3/4r3/8/8/8/8/4N3/4K3 w - - 0 1"))
    assert f["pinned_pieces"] == 1
    assert f["attacked_pieces"] == 1
    assert f["en_prise_pieces"] == 0
    assert f["king_zone_attacked"] == 1  # the rook reaches e2


def test_threats_both_ways_after_1_e4_d5():
    f = board_features(board_after("e4", "d5"))
    assert f["en_prise_value"] == 1  # e4 is attacked and undefended
    assert f["opp_en_prise_value"] == 0  # d5 is defended by the queen
    assert f["captures_available"] == 1  # exd5
    assert f["checks_available"] == 1  # Bb5+


def test_material_is_from_the_movers_view():
    board = board_after("e4", "d5", "exd5")  # black to move, a pawn down
    assert board_features(board)["material_balance"] == -1
    assert board_features(board)["piece_count"] == 31


def test_check_and_legal_moves():
    f = board_features(board_after("e4", "f5", "Qh5+"))  # only g6 parries the check
    assert f["in_check"]
    assert f["legal_moves"] == 1


def test_time_spent_adds_increment_and_is_never_negative():
    assert time_spent(180, 175, 2) == 7
    assert time_spent(10, 12, 0) == 0


def test_plies_out_of_book_and_eco_family_carry_forward():
    state = GameState()
    board = chess.Board()
    seen = []
    for san in ["e4", "e5", "Nf3", "Nc6", "Bb5"]:
        board.push_san(san)
        f = compute_features(board, {"mover": 180, "opp": 180}, META, state, BOOK)
        seen.append((f["in_book"], f["plies_out_of_book"], f["eco_family"]))
    assert seen == [
        (True, 0, "B"),  # after 1. e4
        (True, 0, "C"),  # after 1... e5
        (False, 1, "C"),
        (False, 2, "C"),
        (False, 3, "C"),
    ]


def test_starting_position_counts_as_book():
    f = compute_features(board_after("d4"), {"mover": 180, "opp": 180}, META, GameState(), BOOK)
    assert not f["in_book"]
    assert f["plies_out_of_book"] == 1
    assert f["eco_family"] is None


def test_prev_time_spent_is_per_colour():
    state = GameState()
    clocks = {"mover": 180, "opp": 180}
    assert compute_features(chess.Board(), clocks, META, state, BOOK)["prev_time_spent"] is None
    state.record_move(chess.WHITE, 3.0)
    black_to_move = board_after("e4")
    assert compute_features(black_to_move, clocks, META, state, BOOK)["prev_time_spent"] is None
    state.record_move(chess.BLACK, 5.0)
    white_to_move = board_after("e4", "e5")
    assert compute_features(white_to_move, clocks, META, state, BOOK)["prev_time_spent"] == 3.0


def test_material_change_and_drawdown():
    state = GameState()
    board = chess.Board()
    clocks = {"mover": 180, "opp": 180}
    seen = {}
    for san in ["e4", "d5", "exd5", "Qxd5"]:
        board.push_san(san)
        f = compute_features(board, clocks, META, state, BOOK)
        seen[san] = (f["material_change"], f["material_drawdown"])
    assert seen["exd5"] == (-1, -1)  # black to move, a pawn down
    assert seen["Qxd5"] == (0, -1)  # white back to even, but was a pawn up a ply ago


def test_recent_time_avg_covers_the_last_three_own_moves():
    state = GameState()
    for t in [1.0, 2.0, 3.0, 10.0]:
        state.record_move(chess.WHITE, t)
    state.record_move(chess.BLACK, 50.0)
    f = compute_features(chess.Board(), {"mover": 180, "opp": 180}, META, state, BOOK)
    assert f["prev_time_spent"] == 10.0
    assert f["recent_time_avg"] == 5.0


def test_compute_features_follows_the_contract():
    f = compute_features(chess.Board(), {"mover": 170, "opp": 175}, META, GameState(), BOOK)
    assert list(f) == FEATURES
    assert f["rating_gap"] == -100
    assert f["clock"] == 170
    assert f["opp_clock"] == 175


def test_missing_rating_gives_missing_gap():
    meta = {**META, "opp_elo": None}
    f = compute_features(chess.Board(), {"mover": 180, "opp": 180}, meta, GameState(), BOOK)
    assert f["rating_gap"] is None


def test_build_features_recovers_the_first_move_time():
    from ingest.archive import parse_game
    from ingest.build_features import game_features
    from tests.test_archive import GAME

    rows = game_features(parse_game(GAME))
    by_ply = {r["ply"]: r for r in rows}
    assert by_ply[2]["prev_time_spent"] is None  # black has not moved yet
    assert by_ply[3]["prev_time_spent"] == 2  # 1. e4 left 3:00 on a 180+2 clock
    assert by_ply[4]["prev_time_spent"] == 1  # 1... e5 went from 3:00 to 3:01 with the +2


def test_is_scored_excludes_bots_and_ultrabullet():
    assert is_scored(None, None, "blitz")
    assert is_scored("GM", None, "rapid")
    assert not is_scored(None, "BOT", "blitz")
    assert not is_scored(None, None, "ultraBullet")
