"""
End-to-end Docker cluster tests using testcontainers.

Fully self-contained: the node script is defined as a Python string and
injected at runtime via ``python -c``. The Docker image is generic —
just casty installed, no test code baked in.
"""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
IMAGE_NAME = "casty-test:latest"

# ── Inline Dockerfile (generic — just casty installed) ───────────────────

DOCKERFILE = """\
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY . /app
WORKDIR /app
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
RUN uv sync --no-dev
ENTRYPOINT ["uv", "run", "python", "-c"]
"""

# ── Node script (injected into containers at runtime via python -c) ──────

NODE_SCRIPT = textwrap.dedent("""\
    import argparse
    import asyncio
    import socket
    from dataclasses import dataclass
    from typing import Any

    from casty import ActorRef, Behavior, Behaviors
    from casty.sharding import ClusteredActorSystem, ShardEnvelope

    @dataclass(frozen=True)
    class Increment:
        amount: int

    @dataclass(frozen=True)
    class GetValue:
        reply_to: ActorRef[int]

    type CounterMsg = Increment | GetValue

    def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
        async def setup(_ctx: Any) -> Any:
            value = 0
            async def receive(_ctx: Any, msg: Any) -> Any:
                nonlocal value
                match msg:
                    case Increment(amount=amount):
                        value += amount
                    case GetValue(reply_to=reply_to):
                        reply_to.tell(value)
                return Behaviors.same()
            return Behaviors.receive(receive)
        return Behaviors.setup(setup)

    async def run_seed(host: str, port: int, bind_host: str) -> None:
        async with ClusteredActorSystem(
            name="test-cluster", host=host, port=port, bind_host=bind_host,
        ) as system:
            proxy = system.spawn(
                Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
                "counters",
            )
            await asyncio.sleep(1.0)
            entity = "test-counter"
            for _ in range(10):
                proxy.tell(ShardEnvelope(entity, Increment(amount=1)))
                await asyncio.sleep(0.05)
            await asyncio.sleep(1.0)
            result = await system.ask(
                proxy,
                lambda r: ShardEnvelope(entity, GetValue(reply_to=r)),
                timeout=5.0,
            )
            print(f"SEED_RESULT={result}")
            await asyncio.sleep(10.0)

    async def run_worker(
        host: str, port: int, bind_host: str, seed_host: str, seed_port: int,
    ) -> None:
        async with ClusteredActorSystem(
            name="test-cluster", host=host, port=port, bind_host=bind_host,
            seed_nodes=[(seed_host, seed_port)],
        ) as system:
            proxy = system.spawn(
                Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
                "counters",
            )
            await asyncio.sleep(8.0)
            entity = "test-counter"
            result = await system.ask(
                proxy,
                lambda r: ShardEnvelope(entity, GetValue(reply_to=r)),
                timeout=5.0,
            )
            print(f"WORKER_RESULT={result}")

    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["seed", "worker"])
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--bind-host", default="0.0.0.0")
    parser.add_argument("--seed-host", default=None)
    parser.add_argument("--seed-port", type=int, default=None)
    args = parser.parse_args()
    host = socket.gethostbyname(socket.gethostname()) if args.host == "auto" else args.host
    if args.role == "seed":
        asyncio.run(run_seed(host, args.port, args.bind_host))
    else:
        asyncio.run(run_worker(host, args.port, args.bind_host, args.seed_host, args.seed_port))
""")

# ── Fixtures & helpers ───────────────────────────────────────────────────

SEED_ARGS = ["seed", "--host", "seed", "--port", "25520", "--bind-host", "0.0.0.0"]
WORKER_ARGS = [
    "worker", "--host", "auto", "--port", "25520", "--bind-host", "0.0.0.0",
    "--seed-host", "seed", "--seed-port", "25520",
]


@pytest.fixture(scope="module")
def casty_image() -> str:
    """Build a generic casty image once per test module."""
    subprocess.run(
        ["docker", "build", "-t", IMAGE_NAME, "-f", "-", "."],
        input=DOCKERFILE.encode(),
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    )
    return IMAGE_NAME


def _node(image: str, network: Network, script: str, args: list[str]) -> DockerContainer:
    return DockerContainer(image).with_network(network).with_command([script, *args])


def _wait_for_exit(container: DockerContainer, timeout: float = 60.0) -> int:
    wrapped = container.get_wrapped_container()
    result: dict[str, int] = wrapped.wait(timeout=int(timeout))
    return result["StatusCode"]


def _get_logs(container: DockerContainer) -> str:
    stdout, stderr = container.get_logs()
    return stdout.decode("utf-8") + stderr.decode("utf-8")


# ── Tests ────────────────────────────────────────────────────────────────


class TestContaineredCluster:
    def test_seed_writes_and_reads_counter(self, casty_image: str) -> None:
        """Seed node writes counter=10 and reads it back successfully."""
        with Network() as network:
            seed = _node(casty_image, network, NODE_SCRIPT, SEED_ARGS)
            seed.with_network_aliases("seed")
            with seed:
                exit_code = _wait_for_exit(seed)
                logs = _get_logs(seed)

                assert exit_code == 0, f"Seed exited with {exit_code}:\n{logs}"
                assert "SEED_RESULT=10" in logs

    def test_workers_read_counter_from_seed(self, casty_image: str) -> None:
        """Workers connect to seed and read the counter value over TCP."""
        with Network() as network:
            seed = _node(casty_image, network, NODE_SCRIPT, SEED_ARGS)
            seed.with_network_aliases("seed")
            seed.start()

            workers = [
                _node(casty_image, network, NODE_SCRIPT, WORKER_ARGS)
                for _ in range(2)
            ]
            for w in workers:
                w.start()

            try:
                for w in workers:
                    exit_code = _wait_for_exit(w)
                    logs = _get_logs(w)
                    assert exit_code == 0, f"Worker exited with {exit_code}:\n{logs}"
                    assert "WORKER_RESULT=10" in logs

                seed_exit = _wait_for_exit(seed)
                seed_logs = _get_logs(seed)
                assert seed_exit == 0, f"Seed exited with {seed_exit}:\n{seed_logs}"
                assert "SEED_RESULT=10" in seed_logs
            finally:
                for w in workers:
                    w.stop()
                seed.stop()

    def test_cluster_scales_to_four_workers(self, casty_image: str) -> None:
        """Cluster works with 4 worker nodes (5 total)."""
        with Network() as network:
            seed = _node(casty_image, network, NODE_SCRIPT, SEED_ARGS)
            seed.with_network_aliases("seed")
            seed.start()

            workers = [
                _node(casty_image, network, NODE_SCRIPT, WORKER_ARGS)
                for _ in range(4)
            ]
            for w in workers:
                w.start()

            try:
                for i, w in enumerate(workers):
                    exit_code = _wait_for_exit(w)
                    logs = _get_logs(w)
                    assert exit_code == 0, f"Worker {i} exited with {exit_code}:\n{logs}"
                    assert "WORKER_RESULT=10" in logs

                seed_exit = _wait_for_exit(seed)
                seed_logs = _get_logs(seed)
                assert seed_exit == 0, f"Seed exited with {seed_exit}:\n{seed_logs}"
            finally:
                for w in workers:
                    w.stop()
                seed.stop()
