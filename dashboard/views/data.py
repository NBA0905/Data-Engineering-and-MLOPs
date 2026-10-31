"""data page: where do blunders happen? straight from the gold feature table the spark job builds."""

import pandas as pd
import streamlit as st

from dashboard.lake import load_features

st.title("Where do blunders happen?")
st.caption("From `gold/features` in RustFS: the table the model is trained on, built by the Spark job. "
           "A blunder is a move that drops the player's winning chance by 15 percentage points or more.")

df = load_features()

games, moves, rate = st.columns(3)
games.metric("Games", f"{df['game_id'].nunique():,}")
moves.metric("Moves", f"{len(df):,}")
rate.metric("Blunder rate", f"{df['blunder'].mean():.1%}")


def blunder_rate(groups):
    """blunder rate in % per group, ready for a bar chart."""
    return (df.groupby(groups, observed=True)["blunder"].mean() * 100).rename("blunder rate (%)")


left, right = st.columns(2)
with left:
    st.subheader("Time left on the clock")
    clock = pd.cut(df["clock"], [0, 10, 30, 60, 180, 600, float("inf")], right=False,
                   labels=["<10s", "10-30s", "30-60s", "1-3m", "3-10m", "10m+"])
    st.bar_chart(blunder_rate(clock), sort=False)  # sort=False keeps the bands in our order
    st.caption("Less time, more blunders.")

with right:
    st.subheader("Moves since leaving the opening book")
    book = pd.cut(df["plies_out_of_book"], [0, 1, 5, 10, 20, 40, float("inf")], right=False,
                  labels=["in book", "1-4", "5-9", "10-19", "20-39", "40+"])
    st.bar_chart(blunder_rate(book), sort=False)
    st.caption("Inside known opening theory players almost never blunder. This comes from joining eco.json.")

left, right = st.columns(2)
with left:
    st.subheader("Speed")
    speed = blunder_rate(df["speed"]).reindex(["bullet", "blitz", "rapid", "classical"]).dropna()
    st.bar_chart(speed, sort=False)

with right:
    st.subheader("Rating of the player to move")
    rating = pd.cut(df["mover_elo"], [0, 1200, 1500, 1800, 2100, 2400, float("inf")], right=False,
                    labels=["<1200", "1200-1499", "1500-1799", "1800-2099", "2100-2399", "2400+"])
    st.bar_chart(blunder_rate(rating), sort=False)