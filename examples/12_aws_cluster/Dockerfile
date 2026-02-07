FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY . /app
WORKDIR /app

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
# Install the example's dependencies (includes casty as path dep)
RUN uv sync --project examples/12_aws_cluster --no-dev

EXPOSE 8000 25520

CMD ["uv", "run", "--project", "examples/12_aws_cluster", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
