FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY . /app
WORKDIR /app

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
RUN uv sync --project tests/loadtest --extra app --no-dev

EXPOSE 8000 25520

CMD ["uv", "run", "--project", "tests/loadtest", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
