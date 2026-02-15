"""Cluster performance benchmarks — shard routing throughput and latency."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from casty import ActorRef, Behavior, Behaviors
from casty.cluster.system import ClusteredActorSystem
from casty.cluster.envelope import ShardEnvelope

if TYPE_CHECKING:
    from tests.benchmarks.conftest import BenchmarkResult


# --- Counter entity (same as test_tcp_cluster.py) ---


@dataclass(frozen=True)
class Increment:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetBalance


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    async def setup(_ctx: Any) -> Any:
        value = 0

        async def receive(_ctx: Any, msg: Any) -> Any:
            nonlocal value
            match msg:
                case Increment(amount=amount):
                    value += amount
                case GetBalance(reply_to=reply_to):
                    reply_to.tell(value)
            return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


# --- Benchmarks ---

DURATION_S = 10.0


async def test_single_node_shard_tell_throughput(bench: BenchmarkResult) -> None:
    """Send as many tell(ShardEnvelope) as possible within DURATION_S seconds."""
    async with ClusteredActorSystem(
        name="bench-tell", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        loop = asyncio.get_running_loop()
        sent = 0
        deadline = time.monotonic() + DURATION_S

        def send_from_thread() -> None:
            nonlocal sent
            i = 0
            while time.monotonic() < deadline:
                loop.call_soon_threadsafe(
                    proxy.tell, ShardEnvelope(f"entity-{i % 100}", Increment(amount=1))
                )
                i += 1
            sent = i

        start = time.perf_counter_ns()
        thread = threading.Thread(target=send_from_thread)
        thread.start()

        while thread.is_alive():
            await asyncio.sleep(0.05)
        thread.join()

        bench.total_ns = time.perf_counter_ns() - start
        bench.ops = sent
        bench.label = (
            f"single-node tell throughput ({sent:,} msgs in {DURATION_S:.0f}s)"
        )

        # Drain — give actors time to process remaining messages
        await asyncio.sleep(2.0)

        # Sanity check: total across all 100 entities should equal sent
        total = 0

        for e in range(100):
            total += await system.ask(
                proxy,
                lambda r, eid=e: ShardEnvelope(f"entity-{eid}", GetBalance(reply_to=r)),
                timeout=5.0,
            )
        assert total == sent


async def test_single_node_shard_ask_latency(bench: BenchmarkResult) -> None:
    """Run ask() round-trips for DURATION_S seconds, measure p50/p99 latency."""
    async with ClusteredActorSystem(
        name="bench-ask", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        # Seed one entity
        proxy.tell(ShardEnvelope("latency-target", Increment(amount=1)))
        await asyncio.sleep(0.2)

        deadline = time.monotonic() + DURATION_S
        while time.monotonic() < deadline:
            with bench.measure():
                result = await system.ask(
                    proxy,
                    lambda r: ShardEnvelope("latency-target", GetBalance(reply_to=r)),
                    timeout=5.0,
                )
                assert result == 1

        bench.ops = len(bench.timings_ns)
        bench.total_ns = sum(bench.timings_ns)
        bench.label = (
            f"single-node ask latency ({bench.ops:,} round-trips in {DURATION_S:.0f}s)"
        )


@pytest.mark.parametrize("num_nodes", [3, 7, 15, 30, 50])
async def test_cluster_formation_time(bench: BenchmarkResult, num_nodes: int) -> None:
    """Measure time from startup to gossip convergence for N nodes."""
    nodes: list[ClusteredActorSystem] = []
    try:
        seed = ClusteredActorSystem(
            name="bench-cluster", host="127.0.0.1", port=0, node_id="node-1"
        )
        await seed.__aenter__()
        nodes.append(seed)
        seed_port = seed.self_node.port

        bench.label = f"{num_nodes}-node cluster formation"
        bench.ops = 1

        async def start_node(idx: int) -> ClusteredActorSystem:
            node = ClusteredActorSystem(
                name="bench-cluster",
                host="127.0.0.1",
                port=0,
                node_id=f"node-{idx}",
                seed_nodes=[("127.0.0.1", seed_port)],
                required_quorum=num_nodes,
            )
            await node.__aenter__()
            return node

        start = time.perf_counter_ns()
        others = await asyncio.gather(
            *(start_node(i + 2) for i in range(num_nodes - 1))
        )
        await seed.wait_for(num_nodes, timeout=30.0)
        bench.total_ns = time.perf_counter_ns() - start

        nodes.extend(others)

        for node in nodes:
            state = await node.get_cluster_state()
            assert len(state.members) == num_nodes
    finally:
        await asyncio.gather(*(node.__aexit__(None, None, None) for node in nodes))
