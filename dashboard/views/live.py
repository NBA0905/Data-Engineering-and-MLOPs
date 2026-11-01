"""live page: the game on lichess tv right now, with the model's blunder risk for both players."""

import chess
import chess.svg
import streamlit as st

from dashboard.lake import current_game

CHANNELS = ["bullet", "blitz", "rapid", "classical"]
# from the model notebook: 10% of moves score above 0.157, 1% above 0.258
RISKY = 0.157
VERY_RISKY = 0.258

st.title("Live")
channel = st.radio("Channel", CHANNELS, index=CHANNELS.index("rapid"), horizontal=True)


def risk_bar(probability):
    """a vertical bar that fills up with the blunder risk: green, orange above 0.157, red above 0.258."""
    if probability is None:
        return '<div style="height:320px"></div>'
    color = "#d94841" if probability >= VERY_RISKY else "#f0a030" if probability >= RISKY else "#3fa66b"
    filled = min(probability / 0.4, 1) * 100  # a full bar is 40%, risks above that are very rare
    return f"""
        <div style="height:320px;width:56px;margin:auto;border:2px solid #777;border-radius:8px;position:relative;overflow:hidden">
          <div style="position:absolute;bottom:0;width:100%;height:{filled:.0f}%;background:{color}"></div>
        </div>
        <p style="text-align:center;font-size:1.6rem;font-weight:bold;margin:0.3rem 0 0">{probability:.0%}</p>
    """


def player_column(moves, color, to_move):
    """name, rating, clock and blunder risk bar for one player, all centred above each other."""
    now = moves.iloc[-1]
    own = moves[moves["mover"] == color]
    # the risk of this player's latest move, or of the move they're thinking about now
    probability = float(own["blunder_prob"].iloc[-1]) if not own.empty else None

    clock = f"{int(now[color + '_clock'] // 60)}:{int(now[color + '_clock'] % 60):02d}"
    st.markdown(f"""
        <p style="text-align:center;margin:0"><b>{'♔' if color == 'white' else '♚'} {now[color + '_name']}</b> ({now[color + '_rating']})</p>
        <p style="text-align:center;opacity:0.7;margin:0 0 0.8rem">clock {clock}{' · <b>to move</b>' if to_move else ''}</p>
        {risk_bar(probability)}
    """, unsafe_allow_html=True)


@st.fragment(run_every="1s")  # only this part reruns every 3 seconds, not the whole page
def live_board():
    moves = current_game(channel)
    if moves.empty:
        st.info(f"No predictions for {channel} yet. Is the live consumer running?")
        return

    now = moves.iloc[-1]
    board = chess.Board(now["fen"])
    last_move = chess.Move.from_uci(now["last_move"]) if now["last_move"] else None

    white, middle, black = st.columns([1, 2, 1], vertical_alignment="center")
    with white:
        player_column(moves, "white", board.turn == chess.WHITE)
    with middle:
        st.image(chess.svg.board(board, lastmove=last_move), width="stretch")  # fills its column, so it stays centred
    with black:
        player_column(moves, "black", board.turn == chess.BLACK)

    opening = now["opening"] if now["in_book"] else "out of book"
    st.caption(f"game {now['game_id']}  ·  move {board.fullmove_number}  ·  {opening}  ·  model v{now['model_version']}")

    st.subheader("Blunder risk per move")
    history = moves.pivot_table(index="ply", columns="mover", values="blunder_prob")
    # each ply only has a value for the player to move, so carry the last value forward,
    # otherwise every line has a gap at every other point and nothing gets drawn
    st.line_chart(history.ffill(), x_label="ply", y_label="blunder risk")


live_board()