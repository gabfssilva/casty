FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY . /app
WORKDIR /app

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
RUN uv sync --no-dev
RUN bash examples/14_tls_cluster/gen-certs.sh /app/certs

ENTRYPOINT ["/app/.venv/bin/python3", "examples/14_tls_cluster/main.py"]
