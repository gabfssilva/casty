"""Cluster performance benchmarks — shard routing throughput and latency."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import ActorRef, Behavior, Behaviors
from casty.sharding import ClusteredActorSystem, ShardEnvelope

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

NUM_TELLS = 5_000
NUM_ASKS = 200


async def test_single_node_shard_tell_throughput(bench: BenchmarkResult) -> None:
    """Fire N tell(ShardEnvelope) at a single-node cluster, measure msgs/sec."""
    async with ClusteredActorSystem(
        name="bench-tell", host="127.0.0.1", port=0
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        bench.label = f"single-node tell throughput ({NUM_TELLS} msgs)"
        bench.ops = NUM_TELLS
        start = time.perf_counter_ns()
        for i in range(NUM_TELLS):
            proxy.tell(ShardEnvelope(f"entity-{i % 100}", Increment(amount=1)))
        bench.total_ns = time.perf_counter_ns() - start

        # Drain — give actors time to process
        await asyncio.sleep(1.0)

        # Sanity check: entity-0 should have received NUM_TELLS/100 increments
        result = await system.ask(
            proxy,
            lambda r: ShardEnvelope("entity-0", GetBalance(reply_to=r)),
            timeout=5.0,
        )
        assert result == NUM_TELLS // 100


async def test_single_node_shard_ask_latency(bench: BenchmarkResult) -> None:
    """Measure p50/p99 of ask() round-trips through shard proxy on single node."""
    async with ClusteredActorSystem(
        name="bench-ask", host="127.0.0.1", port=0
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        # Seed one entity
        proxy.tell(ShardEnvelope("latency-target", Increment(amount=1)))
        await asyncio.sleep(0.2)

        bench.label = f"single-node ask latency ({NUM_ASKS} round-trips)"
        for _ in range(NUM_ASKS):
            with bench.measure():
                result = await system.ask(
                    proxy,
                    lambda r: ShardEnvelope(
                        "latency-target", GetBalance(reply_to=r)
                    ),
                    timeout=5.0,
                )
                assert result == 1

        bench.total_ns = sum(bench.timings_ns)


async def test_two_node_cluster_formation_time(bench: BenchmarkResult) -> None:
    """Measure time from second node startup to gossip convergence (2 nodes UP)."""
    async with ClusteredActorSystem(
        name="bench-cluster", host="127.0.0.1", port=0
    ) as system_a:
        port_a = system_a.self_node.port

        bench.label = "two-node cluster formation"
        bench.ops = 1
        start = time.perf_counter_ns()
        async with ClusteredActorSystem(
            name="bench-cluster",
            host="127.0.0.1",
            port=0,
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            await system_a.wait_for(2, timeout=10.0)
            bench.total_ns = time.perf_counter_ns() - start

            # Verify both see each other
            state_a = await system_a.get_cluster_state()
            state_b = await system_b.get_cluster_state()
            assert len(state_a.members) == 2
            assert len(state_b.members) == 2
