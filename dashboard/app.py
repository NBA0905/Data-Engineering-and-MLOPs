"""streamlit dashboard: insight in the data, the model and (later) the live predictions.

run with: uv run streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

# streamlit only puts the dashboard folder on the python path. add the repo root too, so
# the pages can import our own packages (features, storage, dashboard), same trick as the notebook
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st  # noqa: E402

st.set_page_config(page_title="Chess blunder predictor", page_icon="♟️", layout="wide")

pages = [
    st.Page("views/live.py", title="Live"),
    st.Page("views/data.py", title="Data"),
    st.Page("views/model.py", title="Model"),
]
st.navigation(pages).run()