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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from casty import ActorRef, Behavior, Behaviors
from casty.config import load_config
from casty.cluster_state import MemberStatus
from casty.sharding import ClusteredActorSystem, ShardEnvelope


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Task entity — state machine via behavior recursion
# ---------------------------------------------------------------------------


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
                print(f"  [task:{entity_id}] processed ({n} iters) -> {result}")
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


# ---------------------------------------------------------------------------
# Report entity — aggregates distribution from all nodes
# ---------------------------------------------------------------------------


def report_collecting(
    reports: dict[str, dict[str, int]], total: int,
) -> Behavior[EntityMsg]:
    async def receive(_ctx: Any, msg: EntityMsg) -> Behavior[EntityMsg]:
        match msg:
            case NodeReport(from_node=from_node, distribution=dist, total=node_total):
                new_reports = {**reports, from_node: dist}
                return report_collecting(new_reports, total + node_total)
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


# ---------------------------------------------------------------------------
# Worker — submit tasks, query results, report
# ---------------------------------------------------------------------------


async def run_worker(
    system: ClusteredActorSystem,
    proxy: ActorRef[ShardEnvelope[EntityMsg]],
    worker_id: str,
    num_tasks: int,
    num_nodes: int,
) -> None:
    task_ids = [f"job-{worker_id}-{i}" for i in range(num_tasks)]

    print(f"[{worker_id}] Submitting {num_tasks} tasks...")
    start = time.monotonic()

    for tid in task_ids:
        proxy.tell(
            ShardEnvelope(tid, SubmitTask(payload=f"doc-{tid}", iterations=500))
        )

    await asyncio.sleep(3.0)

    print(f"[{worker_id}] Querying results...")
    done_count = 0
    node_counts: dict[str, int] = {}
    for tid in task_ids:
        try:
            result: TaskResult = await system.ask(
                proxy,
                lambda r, eid=tid: ShardEnvelope(eid, GetResult(reply_to=r)),
                timeout=5.0,
            )
            mark = "[ok]" if result.status == "completed" else "[..]"
            print(f"  {mark} {result.task_id}: {result.result} (on {result.processed_by})")
            if result.status == "completed":
                done_count += 1
                if result.processed_by:
                    node_counts[result.processed_by] = node_counts.get(result.processed_by, 0) + 1
        except asyncio.TimeoutError:
            print(f"  [!!] {tid}: timeout")

    elapsed = time.monotonic() - start
    print(f"[{worker_id}] {done_count}/{num_tasks} completed in {elapsed:.1f}s")

    # Send report to centralized entity
    proxy.tell(ShardEnvelope(
        "report", NodeReport(from_node=worker_id, distribution=node_counts, total=done_count),
    ))

    # Ensure all nodes are still up before querying the consolidated report.
    await system.wait_for(num_nodes, timeout=60.0)

    for _ in range(30):
        await asyncio.sleep(1.0)
        try:
            report: FinalReport = await system.ask(
                proxy,
                lambda r: ShardEnvelope("report", GetFinalReport(reply_to=r)),
                timeout=3.0,
            )
            if report.nodes_reported >= num_nodes:
                dist = ", ".join(
                    f"{node}: {count}" for node, count in sorted(report.distribution.items())
                )
                print(f"[{worker_id}] === Final Report ({report.total} tasks) ===")
                print(f"[{worker_id}]   {dist}")
                return
        except asyncio.TimeoutError:
            pass

    print(f"[{worker_id}] Report incomplete (not all nodes reported)")


# ---------------------------------------------------------------------------
# Node runner
# ---------------------------------------------------------------------------


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

    print(f"[{worker_id}] Starting...")

    async with ClusteredActorSystem.from_config(
        config,
        host=host,
        port=port,
        seed_nodes=seed_nodes,
        bind_host=bind_host,
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=entity_factory, num_shards=50),
            "tasks",
        )

        state = await system.wait_for(num_nodes)
        up = sum(1 for m in state.members if m.status == MemberStatus.up)
        print(f"[{worker_id}] Cluster ready ({up} nodes).")

        await run_worker(system, proxy, worker_id, num_tasks, num_nodes)

    print(f"[{worker_id}] Shutdown.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


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

    if args.host is None:
        host = "127.0.0.1"
    elif args.host == "auto":
        host = socket.gethostbyname(socket.gethostname())
    else:
        host = args.host

    bind_host: str = args.bind_host or host

    seed_nodes: list[tuple[str, int]] | None = None
    if args.seed:
        seed_host, seed_port_str = args.seed.rsplit(":", maxsplit=1)
        seed_nodes = [(seed_host, int(seed_port_str))]

    asyncio.run(
        run_node(host, args.port, bind_host, seed_nodes, args.tasks, args.nodes)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")
    main()
