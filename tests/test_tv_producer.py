from ingest import tv_producer


def fake_game_info(game_id):
    # pretend lichess answered, so the test doesn't need the internet
    return {"tc_base": 600, "tc_inc": 0, "moves": "e4 e5"}


def featured(game_id):
    return {"t": "featured", "d": {"id": game_id, "fen": "start", "players": [
        {"color": "white", "user": {"name": "alice"}, "rating": 2000},
        {"color": "black", "user": {"name": "bob"}, "rating": 1900},
    ]}}


def move():
    return {"t": "fen", "d": {"fen": "somefen", "lm": "e2e4", "wc": 590, "bc": 600}}


def test_move_before_any_game_is_skipped():
    tv_producer.games.clear()
    assert tv_producer.to_message("rapid", move()) is None


def test_moves_get_the_current_game_stamped_on(monkeypatch):
    monkeypatch.setattr(tv_producer, "fetch_game_info", fake_game_info)
    tv_producer.games.clear()

    start = tv_producer.to_message("rapid", featured("game1"))
    assert start["type"] == "start"
    assert start["moves_so_far"] == "e4 e5"

    message = tv_producer.to_message("rapid", move())
    assert message["game_id"] == "game1"
    assert message["white_rating"] == 2000
    assert message["tc_base"] == 600


def test_new_game_replaces_the_old_one(monkeypatch):
    monkeypatch.setattr(tv_producer, "fetch_game_info", fake_game_info)
    tv_producer.games.clear()

    tv_producer.to_message("rapid", featured("game1"))
    tv_producer.to_message("rapid", featured("game2"))
    assert tv_producer.to_message("rapid", move())["game_id"] == "game2"