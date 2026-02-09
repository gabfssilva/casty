"""
Distributed Task Queue
======================

All nodes are symmetric workers. Each submits tasks that get sharded across
the cluster, processed on whichever node owns the shard, and results queried
back from any node.

Usage (local):
    uv run python examples/11_multi_process_cluster/main.py --port 25520
    uv run python examples/11_multi_process_cluster/main.py --port 25521 --seed 127.0.0.1:25520

Usage (Docker Compose):
    cd examples/11_multi_process_cluster
    docker compose up --build
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import socket
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from casty import ActorRef, Behavior, Behaviors
from casty.config import load_config
from casty.sharding import ClusteredActorSystem, ShardEnvelope

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


async def retry[T](
    fn: Callable[[], Awaitable[T]],
    *,
    until: Callable[[T], bool] = lambda _: True,
    attempts: int = 30,
    delay: float = 1.0,
) -> T | None:
    for _ in range(attempts):
        await asyncio.sleep(delay)
        try:
            result = await fn()
        except asyncio.TimeoutError:
            continue
        if until(result):
            return result
    return None


@dataclass(frozen=True)
class SubmitTask:
    payload: str
    iterations: int


@dataclass(frozen=True)
class GetResult:
    reply_to: ActorRef[TaskResult]


@dataclass(frozen=True)
class TaskResult:
    task_id: str
    status: str
    result: str | None
    processed_by: str | None = None


@dataclass(frozen=True)
class NodeReport:
    from_node: str
    distribution: dict[str, int]
    total: int


@dataclass(frozen=True)
class GetFinalReport:
    reply_to: ActorRef[FinalReport]


@dataclass(frozen=True)
class FinalReport:
    distribution: dict[str, int]
    total: int
    nodes_reported: int


type EntityMsg = SubmitTask | GetResult | NodeReport | GetFinalReport


def entity_factory(entity_id: str) -> Behavior[EntityMsg]:
    if entity_id == "report":
        return report_collecting(reports={}, total=0)
    return task_pending(entity_id)


def task_pending(entity_id: str) -> Behavior[EntityMsg]:
    async def receive(_ctx: Any, msg: EntityMsg) -> Behavior[EntityMsg]:
        match msg:
            case SubmitTask(payload=payload, iterations=n):
                digest = payload.encode()
                for _ in range(n):
                    digest = hashlib.sha256(digest).digest()
                result = digest.hex()[:16]
                node = socket.gethostname()
                log.info("[task:%s] processed (%d iters) -> %s", entity_id, n, result)
                return task_completed(entity_id, result, node)
            case GetResult(reply_to=reply_to):
                reply_to.tell(TaskResult(entity_id, "pending", None))
                return Behaviors.same()
            case _:
                return Behaviors.same()
    return Behaviors.receive(receive)


def task_completed(entity_id: str, result: str, node: str) -> Behavior[EntityMsg]:
    async def receive(_ctx: Any, msg: EntityMsg) -> Behavior[EntityMsg]:
        match msg:
            case GetResult(reply_to=reply_to):
                reply_to.tell(TaskResult(entity_id, "completed", result, node))
                return Behaviors.same()
            case _:
                return Behaviors.same()
    return Behaviors.receive(receive)


def report_collecting(
    reports: dict[str, dict[str, int]], total: int,
) -> Behavior[EntityMsg]:
    async def receive(_ctx: Any, msg: EntityMsg) -> Behavior[EntityMsg]:
        match msg:
            case NodeReport(from_node=from_node, distribution=dist, total=node_total):
                return report_collecting({**reports, from_node: dist}, total + node_total)
            case GetFinalReport(reply_to=reply_to):
                merged: dict[str, int] = {}
                for dist in reports.values():
                    for node, count in dist.items():
                        merged[node] = merged.get(node, 0) + count
                reply_to.tell(FinalReport(merged, total, len(reports)))
                return Behaviors.same()
            case _:
                return Behaviors.same()
    return Behaviors.receive(receive)


async def run_worker(
    system: ClusteredActorSystem,
    proxy: ActorRef[ShardEnvelope[EntityMsg]],
    worker_id: str,
    num_tasks: int,
    num_nodes: int,
) -> None:
    task_ids = [f"job-{worker_id}-{i}" for i in range(num_tasks)]
    start = time.monotonic()

    log.info("Submitting %d tasks...", num_tasks)
    for tid in task_ids:
        proxy.tell(ShardEnvelope(tid, SubmitTask(payload=f"doc-{tid}", iterations=500)))

    await asyncio.sleep(3.0)

    log.info("Querying results...")
    done = 0
    distribution: dict[str, int] = {}

    for tid in task_ids:
        result = await system.ask_or_none(
            proxy,
            lambda r, eid=tid: ShardEnvelope(eid, GetResult(reply_to=r)),
            timeout=5.0,
        )
        if result is None:
            log.warning("%s: timeout", tid)
            continue

        log.info("%s: %s (on %s)", result.task_id, result.result, result.processed_by)
        match result:
            case TaskResult(status="completed", processed_by=node) if node:
                done += 1
                distribution[node] = distribution.get(node, 0) + 1
            case _:
                pass

    elapsed = time.monotonic() - start
    log.info("%d/%d completed in %.1fs", done, num_tasks, elapsed)

    proxy.tell(ShardEnvelope(
        "report", NodeReport(from_node=worker_id, distribution=distribution, total=done),
    ))

    await system.barrier("reports-sent", num_nodes)

    report = await retry(
        lambda: system.ask(
            proxy,
            lambda r: ShardEnvelope("report", GetFinalReport(reply_to=r)),
            timeout=3.0,
        ),
        until=lambda r: r.nodes_reported >= num_nodes,
    )

    if report:
        dist = ", ".join(f"{n}: {c}" for n, c in sorted(report.distribution.items()))
        log.info("Final Report (%d tasks): %s", report.total, dist)
    else:
        log.warning("Report incomplete (not all nodes reported)")


async def run_node(
    host: str,
    port: int,
    bind_host: str,
    seed_nodes: list[tuple[str, int]] | None,
    num_tasks: int,
    num_nodes: int,
) -> None:
    worker_id = socket.gethostname()
    config = load_config(Path(__file__).parent / "casty.toml")

    log.info("Starting...")

    async with ClusteredActorSystem.from_config(
        config,
        host=host,
        port=port,
        node_id=worker_id,
        seed_nodes=seed_nodes,
        bind_host=bind_host,
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=entity_factory, num_shards=50),
            "tasks",
        )

        await system.wait_for(num_nodes)
        log.info("Cluster ready (%d nodes)", num_nodes)

        await run_worker(system, proxy, worker_id, num_tasks, num_nodes)
        await system.barrier("shutdown", num_nodes)

    log.info("Shutdown")


def main() -> None:
    parser = argparse.ArgumentParser(description="Casty Distributed Task Queue")
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
        "--tasks",
        type=int,
        default=5,
        help="Number of tasks per worker (default: 5)",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=1,
        help="Wait for N nodes before starting work (default: 1)",
    )
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

    asyncio.run(
        run_node(host, args.port, bind_host, seed_nodes, args.tasks, args.nodes)
    )


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    main()
