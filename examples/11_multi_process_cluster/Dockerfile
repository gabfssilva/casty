FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY . /app
WORKDIR /app

RUN uv sync --no-dev

ENTRYPOINT ["uv", "run", "python", "examples/11_multi_process_cluster/main.py"]
