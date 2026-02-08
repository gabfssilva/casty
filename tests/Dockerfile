FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY . /app
WORKDIR /app
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
RUN uv sync --no-dev
ENTRYPOINT ["uv", "run", "python"]
