"""
reads moves from lichess and sends it to redpanda
run with: uv run python -m ingest.tv_producer rapid blitz
"""

import argparse
import json
import os
import threading
import time

import requests
from confluent_kafka import Producer

FEED_URL = "https://lichess.org/api/tv/{channel}/feed"
GAME_URL = "https://lichess.org/game/export/{game_id}"
TOPIC = "tv.moves"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")

producer = Producer({"bootstrap.servers": BOOTSTRAP})

# the game that is currently on each channel, set when a "featured" event comes in
games = {}


def read_feed(channel):
    """gives back every event of one tv channel, reconnects if the connection drops."""
    url = FEED_URL.format(channel=channel)
    while True:
        try:
            with requests.get(url, stream=True, timeout=60) as response:
                for line in response.iter_lines():
                    if line:  # lichess sends empty lines to keep the connection alive
                        yield json.loads(line)
        except requests.exceptions.RequestException as error:
            print(f"[{channel}] connection lost ({error}), reconnecting in 5 s")
            time.sleep(5)

def fetch_game_info(game_id):
    """asks lichess once per game for the time control and the moves played so far."""
    url = GAME_URL.format(game_id=game_id)
    try:
        response = requests.get(url, params={"clocks": "true"}, headers={"Accept": "application/json"}, timeout=10)
        response.raise_for_status()
        info = response.json()
    except requests.exceptions.RequestException as error:
        # not the end of the world, the game just gets sent without a time control
        print(f"could not fetch game {game_id} ({error})")
        return {"tc_base": None, "tc_inc": None, "variant": None, "moves": "", "clocks": []}

    clock = info.get("clock", {})
    moves = info.get("moves", "")
    return {
        "tc_base": clock.get("initial"),  # starting time in seconds
        "tc_inc": clock.get("increment"),  # seconds added per move
        "variant": info.get("variant"),  # "standard", "chess960", ... the model only knows standard
        "moves": moves,  # moves so far, like "e4 e5 Nf3"
        # mover's clock after each of those moves, in whole seconds like the feed. lichess sends
        # centiseconds and one extra value at the end, which we drop
        "clocks": [round(c / 100) for c in info.get("clocks", [])[: len(moves.split())]],
    }


def to_message(channel, event):
    """turns one feed event into a message for kafka, or None if there's nothing to send."""
    data = event["d"]

    if event["t"] == "featured":
        # new game on this channel, remember who is playing and the time control
        game = {"channel": channel, "game_id": data["id"]}
        for player in data["players"]:
            color = player["color"]  # "white" or "black"
            game[color + "_name"] = player.get("user", {}).get("name")
            game[color + "_title"] = player.get("user", {}).get("title")  # "BOT" games are not scored
            game[color + "_rating"] = player.get("rating")

        info = fetch_game_info(data["id"])
        game["tc_base"] = info["tc_base"]
        game["tc_inc"] = info["tc_inc"]
        game["variant"] = info["variant"]
        games[channel] = game

        # the moves so far only go in the start message, the consumer needs them once.
        # lichess delays the export of a running game, so they can end a few moves before `fen`.
        # the featured event's fen has its move counters reset ("0 1") and its `seconds` are
        # not the real clocks, so the consumer only trusts the fen and clocks of move messages
        return {
            **game,
            "type": "start",
            "fen": data["fen"],
            "moves_so_far": info["moves"],
            "clocks_so_far": info["clocks"],
            "received_at": time.time(),
        }

    if event["t"] == "fen":
        # a move was played, but we skip it if we don't know the game yet
        if channel not in games:
            return None
        return {
            **games[channel],
            "type": "move",
            "fen": data["fen"],
            "last_move": data["lm"],
            "white_clock": data["wc"],
            "black_clock": data["bc"],
            "received_at": time.time(),
        }

    return None


def on_delivery(error, message):
    """called by the kafka client for every message once redpanda has (not) stored it."""
    if error is not None:
        print(f"could not send message for game {message.key().decode()} ({error})")


def run_channel(channel):
    """reads one channel forever and sends each message to the topic."""
    for event in read_feed(channel):
        message = to_message(channel, event)
        if message is None:
            continue
        # game id as key so all moves of one game land in the same partition
        producer.produce(TOPIC, key=message["game_id"], value=json.dumps(message), on_delivery=on_delivery)
        producer.poll(0)
        print(f"[{channel}] {message['type']} {message['game_id']} {message.get('last_move', '')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="lichess tv feed producer")
    parser.add_argument("channels", nargs="+", help="for example: bullet blitz rapid classical")
    args = parser.parse_args()

    # one thread per channel so they all run at the same time
    for channel in args.channels:
        threading.Thread(target=run_channel, args=(channel,), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # make sure nothing is left in the buffer before we quit
        print("stopping, sending remaining messages...")
        producer.flush(10)