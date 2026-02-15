"""
Heartbeat resilience test — verifies that cluster nodes remain stable
under realistic network latency (simulating AWS cross-AZ) and that
genuine node failures are properly detected.

Uses testcontainers + tc netem for network condition injection.
"""

from __future__ import annotations

import subprocess
import textwrap
import time
from pathlib import Path

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
IMAGE_NAME = "casty-resilience:latest"

DOCKERFILE = """\
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN apt-get update -qq && apt-get install -qq -y iproute2 > /dev/null 2>&1
COPY . /app
WORKDIR /app
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
RUN uv sync --no-dev
ENTRYPOINT ["uv", "run", "python", "-c"]
"""

NODE_SCRIPT = textwrap.dedent("""\
    import argparse
    import asyncio
    import logging
    import socket

    from casty.config import (
        CastyConfig,
        FailureDetectorConfig,
        GossipConfig,
        HeartbeatConfig,
    )
    from casty.cluster.system import ClusteredActorSystem

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    CLUSTER_CONFIG = CastyConfig(
        heartbeat=HeartbeatConfig(interval=0.5, availability_check_interval=2.0),
        failure_detector=FailureDetectorConfig(threshold=8.0),
        gossip=GossipConfig(interval=1.0),
    )

    async def run_seed(host: str, port: int, bind_host: str, n_workers: int) -> None:
        async with ClusteredActorSystem(
            name="test-cluster", host=host, port=port, bind_host=bind_host,
            node_id="seed",
            config=CLUSTER_CONFIG,
        ) as system:
            total = n_workers + 1
            await system.wait_for(total, timeout=30.0)
            print(f"CLUSTER_READY members={total}", flush=True)

            # Phase 1: stability window — nothing should break
            await asyncio.sleep(30.0)
            print("STABILITY_DONE", flush=True)

            # Phase 2: detection window — wait for external kill
            await asyncio.sleep(30.0)
            print("SEED_DONE", flush=True)

    async def run_worker(
        host: str, port: int, bind_host: str, seed_host: str, seed_port: int,
    ) -> None:
        async with ClusteredActorSystem(
            name="test-cluster", host=host, port=port, bind_host=bind_host,
            node_id=f"worker-{host}-{port}",
            seed_nodes=[(seed_host, seed_port)],
            config=CLUSTER_CONFIG,
        ) as system:
            print("WORKER_UP", flush=True)
            await asyncio.sleep(999)  # stays alive until killed

    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["seed", "worker"])
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--bind-host", default="0.0.0.0")
    parser.add_argument("--seed-host", default=None)
    parser.add_argument("--seed-port", type=int, default=None)
    parser.add_argument("--n-workers", type=int, default=2)
    args = parser.parse_args()
    host = socket.gethostbyname(socket.gethostname()) if args.host == "auto" else args.host
    if args.role == "seed":
        asyncio.run(run_seed(host, args.port, args.bind_host, args.n_workers))
    else:
        asyncio.run(run_worker(host, args.port, args.bind_host, args.seed_host, args.seed_port))
""")

# -- Args --

SEED_ARGS = [
    "seed",
    "--host",
    "seed",
    "--port",
    "25520",
    "--bind-host",
    "0.0.0.0",
    "--n-workers",
    "2",
]
WORKER_ARGS = [
    "worker",
    "--host",
    "auto",
    "--port",
    "25520",
    "--bind-host",
    "0.0.0.0",
    "--seed-host",
    "seed",
    "--seed-port",
    "25520",
]


# -- Fixtures & helpers --


@pytest.fixture(scope="module")
def resilience_image() -> str:
    """Build image with iproute2 for tc netem support."""
    subprocess.run(
        ["docker", "build", "-t", IMAGE_NAME, "-f", "-", "."],
        input=DOCKERFILE.encode(),
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    )
    return IMAGE_NAME


def _node(
    image: str, network: Network, script: str, args: list[str]
) -> DockerContainer:
    return (
        DockerContainer(image)
        .with_network(network)
        .with_kwargs(cap_add=["NET_ADMIN"])
        .with_command([script, *args])
    )


def _get_logs(container: DockerContainer) -> str:
    stdout, stderr = container.get_logs()
    return stdout.decode("utf-8") + stderr.decode("utf-8")


def _wait_for_log(
    container: DockerContainer,
    marker: str,
    *,
    timeout: float = 60.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if marker in _get_logs(container):
            return
        time.sleep(0.5)
    logs = _get_logs(container)
    msg = f"Marker {marker!r} not found after {timeout}s.\nLogs:\n{logs}"
    raise TimeoutError(msg)


def _inject_latency(
    container: DockerContainer,
    *,
    delay: str = "5ms",
    jitter: str = "3ms",
) -> None:
    wrapped = container.get_wrapped_container()
    exit_code, output = wrapped.exec_run(
        f"tc qdisc add dev eth0 root netem delay {delay} {jitter}",
    )
    assert exit_code == 0, f"tc netem failed: {output.decode()}"


def _kill(container: DockerContainer) -> None:
    container.get_wrapped_container().kill()


# -- Tests --


class TestHeartbeatResilience:
    @pytest.mark.parametrize(
        ("delay", "jitter", "scenario"),
        [
            ("5ms", "3ms", "cross-AZ"),
            ("50ms", "20ms", "cross-region"),
        ],
    )
    def test_stable_under_latency_detects_real_failure(
        self,
        resilience_image: str,
        delay: str,
        jitter: str,
        scenario: str,
    ) -> None:
        """Nodes stay UP under network latency; killed node is detected.

        Phase 1 — Stability: cluster forms, latency injected, 30s pass.
                  No 'Node unreachable' must appear (no false positives).
        Phase 2 — Detection: one worker is SIGKILLed (crash, not graceful).
                  Seed must log 'Node unreachable' (true positive).
        """
        with Network() as network:
            seed = _node(resilience_image, network, NODE_SCRIPT, SEED_ARGS)
            seed.with_network_aliases("seed")
            seed.start()

            workers = [
                _node(resilience_image, network, NODE_SCRIPT, WORKER_ARGS)
                for _ in range(2)
            ]
            for w in workers:
                w.start()

            try:
                _wait_for_log(seed, "CLUSTER_READY", timeout=45)

                for container in [seed, *workers]:
                    _inject_latency(container, delay=delay, jitter=jitter)

                # Phase 1: stability — no false positives
                _wait_for_log(seed, "STABILITY_DONE", timeout=45)
                seed_logs = _get_logs(seed)
                assert "Node unreachable" not in seed_logs, (
                    f"FALSE POSITIVE ({scenario}): alive nodes marked unreachable.\n"
                    f"Logs:\n{seed_logs}"
                )

                # Phase 2: crash a worker, verify detection
                _kill(workers[0])
                _wait_for_log(seed, "Node unreachable", timeout=20)

            finally:
                for w in workers:
                    try:
                        w.stop()
                    except Exception:
                        pass
                seed.stop()
