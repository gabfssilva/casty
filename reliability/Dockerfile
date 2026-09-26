# The image of a run on Kubernetes, built from the root of the repository by `python -m reliability`: the casty wheel of
# this checkout, and `reliability/`, which the runner, the nodes and the clients run.

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS wheel

RUN apt-get update \
 && apt-get install --yes --no-install-recommends build-essential curl \
 && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /repo
COPY pyproject.toml README.md LICENSE Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
# The caches keep what cargo fetched and compiled across builds, so a change rebuilds only what it touches.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=cache,target=/repo/target \
    uv build --wheel --out-dir /wheels

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

RUN --mount=from=wheel,source=/wheels,target=/wheels uv pip install --system /wheels/*.whl "lightkube>=1.0.1,<2"

WORKDIR /repo
COPY reliability ./reliability
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python"]
