# the live lane: tv producer, live scorer and bronze lander, all from the same image.
# the dependencies come from uv.lock, so the consumer loads the model with exactly the
# scikit-learn it was trained with. the code itself is mounted at /app by docker compose
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.21 /uv /bin/uv

# the venv lives outside /app, so the windows .venv in the mounted folder is never used
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
