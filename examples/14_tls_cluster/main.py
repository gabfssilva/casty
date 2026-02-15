"""
TLS Cluster — Sharded Counter
==============================

5-node cluster with mutual TLS. The seed node increments a sharded counter 50
times, then all nodes read and verify the result.

Usage (local):
    bash examples/14_tls_cluster/gen-certs.sh
    uv run python examples/14_tls_cluster/main.py --port 25520 \
        --certfile examples/14_tls_cluster/certs/node-1.pem \
        --cafile examples/14_tls_cluster/certs/ca.pem
    uv run python examples/14_tls_cluster/main.py --port 25521 \
        --seed 127.0.0.1:25520 \
        --certfile examples/14_tls_cluster/certs/node-2.pem \
        --cafile examples/14_tls_cluster/certs/ca.pem

Usage (Docker Compose):
    cd examples/14_tls_cluster
    bash gen-certs.sh
    docker compose up --build
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from casty import ActorRef, Behavior, Behaviors, tls
from casty.config import load_config
from casty.internal.retry import Backoff, retry
from casty.cluster.system import ClusteredActorSystem
from casty.cluster.envelope import ShardEnvelope

log = logging.getLogger(__name__)

RESET = "\033[0m"
DIM = "\033[2m"
CYAN = "\033[36m"
LEVEL_COLORS = {
    logging.DEBUG: "\033[2m",
    logging.INFO: "\033[32m",
    logging.WARNING: "\033[33m",
    logging.ERROR: "\033[31m",
    logging.CRITICAL: "\033[1;31m",
}


class ColorFormatter(logging.Formatter):
    def __init__(self) -> None:
        self.node = socket.gethostname()

    def format(self, record: logging.LogRecord) -> str:
        ts = time.strftime("%H:%M:%S", time.localtime(record.created))
        color = LEVEL_COLORS.get(record.levelno, "")
        level = record.levelname.ljust(7)
        return (
            f"{DIM}{ts}{RESET}  "
            f"{color}{level}{RESET} "
            f"{CYAN}{self.node}{RESET}  "
            f"{DIM}{record.name}{RESET}  "
            f"{record.getMessage()}"
        )


@dataclass(frozen=True)
class Increment:
    amount: int


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetValue


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    def active(value: int = 0) -> Behavior[CounterMsg]:
        async def receive(_ctx: Any, msg: CounterMsg) -> Behavior[CounterMsg]:
            match msg:
                case Increment(amount=amount):
                    return active(value + amount)
                case GetValue(reply_to=reply_to):
                    reply_to.tell(value)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active()


async def run_node(
    host: str,
    port: int,
    bind_host: str,
    seed_nodes: list[tuple[str, int]] | None,
    num_nodes: int,
    tls_config: tls.Config,
) -> None:
    node_id = socket.gethostname()
    config = load_config(Path(__file__).parent / "casty.toml")

    log.info("Starting (TLS enabled)...")

    async with ClusteredActorSystem.from_config(
        config,
        host=host,
        port=port,
        node_id=node_id,
        seed_nodes=seed_nodes,
        bind_host=bind_host,
        tls=tls_config,
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=20),
            "counters",
        )

        await system.wait_for(num_nodes)
        log.info("Cluster ready (%d nodes)", num_nodes)

        entity = "test-counter"
        is_seed = seed_nodes is None

        if is_seed:
            log.info("Incrementing counter 50 times...")
            for _ in range(50):
                proxy.tell(ShardEnvelope(entity, Increment(amount=1)))
                await asyncio.sleep(0.02)

        await system.barrier("writes-done", num_nodes)

        @retry(max_retries=20, delay=0.5, backoff=Backoff.fixed, on=(Exception,))
        async def read_counter() -> int:
            result = await system.ask(
                proxy,
                lambda r: ShardEnvelope(entity, GetValue(reply_to=r)),
                timeout=2.0,
            )
            if result != 50:
                msg = f"Counter not ready: {result}"
                raise ValueError(msg)
            return result

        result = await read_counter()
        log.info("RESULT=%d", result)
        print(f"RESULT={result}")

        await system.barrier("reads-done", num_nodes)

    log.info("Shutdown")


def main() -> None:
    parser = argparse.ArgumentParser(description="Casty TLS Cluster — Sharded Counter")
    parser.add_argument("--port", type=int, default=25520)
    parser.add_argument(
        "--host",
        default=None,
        help="Identity hostname. Use 'auto' for container IP. Default: 127.0.0.1",
    )
    parser.add_argument(
        "--bind-host",
        default=None,
        help="Address to bind TCP listener. Default: same as --host",
    )
    parser.add_argument(
        "--seed",
        default=None,
        help="Seed node address (host:port) to join an existing cluster",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=1,
        help="Wait for N nodes before starting work (default: 1)",
    )
    parser.add_argument(
        "--certfile", required=True, help="Path to node certificate (PEM)"
    )
    parser.add_argument("--cafile", required=True, help="Path to CA certificate (PEM)")
    args = parser.parse_args()

    match args.host:
        case None:
            host = "127.0.0.1"
        case "auto":
            host = socket.gethostbyname(socket.gethostname())
        case h:
            host = h

    bind_host: str = args.bind_host or host

    seed_nodes: list[tuple[str, int]] | None = None
    if args.seed:
        seed_host, seed_port_str = args.seed.rsplit(":", maxsplit=1)
        seed_nodes = [(seed_host, int(seed_port_str))]

    tls_config = tls.Config.from_paths(certfile=args.certfile, cafile=args.cafile)

    asyncio.run(
        run_node(host, args.port, bind_host, seed_nodes, args.nodes, tls_config)
    )


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    main()
