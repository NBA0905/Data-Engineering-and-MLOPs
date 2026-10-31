from ingest import tv_producer


def fake_game_info(game_id):
    # pretend lichess answered, so the test doesn't need the internet
    return {"tc_base": 600, "tc_inc": 0, "variant": "standard", "moves": "e4 e5", "clocks": [600, 598]}


def featured(game_id):
    return {"t": "featured", "d": {"id": game_id, "fen": "start", "players": [
        {"color": "white", "user": {"name": "alice", "title": "GM"}, "rating": 2000, "seconds": 595},
        {"color": "black", "user": {"name": "bob"}, "rating": 1900, "seconds": 590},
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


def test_start_carries_titles_and_history(monkeypatch):
    monkeypatch.setattr(tv_producer, "fetch_game_info", fake_game_info)
    tv_producer.games.clear()

    start = tv_producer.to_message("rapid", featured("game1"))
    assert (start["white_title"], start["black_title"]) == ("GM", None)
    assert start["clocks_so_far"] == [600, 598]
    assert start["variant"] == "standard"

    message = tv_producer.to_message("rapid", move())
    assert message["white_title"] == "GM"  # titles are stamped on every move, clocks come from the move
    assert (message["white_clock"], message["black_clock"]) == (590, 600)
    assert "clocks_so_far" not in message


def test_fetch_game_info_drops_the_extra_clock_and_converts_centiseconds(monkeypatch):
    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"clock": {"initial": 600, "increment": 0}, "variant": "standard",
                    "moves": "d4 Nf6 Nf3", "clocks": [60003, 60003, 59771, 59499]}

    monkeypatch.setattr(tv_producer.requests, "get", lambda *args, **kwargs: Response())
    info = tv_producer.fetch_game_info("game1")
    assert info["clocks"] == [600, 600, 598]
    assert (info["tc_base"], info["tc_inc"], info["variant"]) == (600, 0, "standard")
