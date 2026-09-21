# Built from the root of the repository (see `compose.yaml`), because the example installs casty from there.

# casty is a compiled extension, so building the wheel takes a Rust toolchain that running the cluster does not.
# It stays in this stage; the image the thirty-three containers run carries only the wheel.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS wheel

RUN apt-get update \
 && apt-get install --yes --no-install-recommends build-essential curl \
 && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /repo
COPY pyproject.toml README.md Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
RUN uv build --wheel --out-dir /wheels

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# The wheel is abi3, so it installs against the interpreter the image already has.
RUN --mount=from=wheel,source=/wheels,target=/wheels uv pip install --system /wheels/*.whl

WORKDIR /app
COPY examples/09-docker-cluster/*.py ./
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python"]
CMD ["node.py"]
