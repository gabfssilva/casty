from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty._shard_coordinator_actor import (
    shard_coordinator_actor,
    LeastShardStrategy,
)
from casty._shard_region_actor import shard_region_actor
from casty.cluster_state import NodeAddress
from casty.distributed import Counter
from casty.distributed._counter import counter_entity


async def test_counter_increment_and_get() -> None:
    """Counter starts at 0; increment, increment(5), get -> 6, decrement(2), get -> 4."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="counter-test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({self_node}),
            ),
            "coordinator",
        )

        region_ref = system.spawn(
            shard_region_actor(
                self_node=self_node,
                coordinator=coord_ref,
                entity_factory=counter_entity,
                num_shards=50,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        counter = Counter(
            system=system, region_ref=region_ref, name="my-counter", timeout=2.0
        )

        val = await counter.increment()
        assert val == 1

        val = await counter.increment(5)
        assert val == 6

        val = await counter.get()
        assert val == 6

        val = await counter.decrement(2)
        assert val == 4

        val = await counter.get()
        assert val == 4


async def test_counter_multiple_counters() -> None:
    """Two named counters are independent."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="multi-counter-test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({self_node}),
            ),
            "coordinator",
        )

        region_ref = system.spawn(
            shard_region_actor(
                self_node=self_node,
                coordinator=coord_ref,
                entity_factory=counter_entity,
                num_shards=50,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        counter_a = Counter(
            system=system, region_ref=region_ref, name="counter-a", timeout=2.0
        )
        counter_b = Counter(
            system=system, region_ref=region_ref, name="counter-b", timeout=2.0
        )

        await counter_a.increment(10)
        await counter_b.increment(20)

        val_a = await counter_a.get()
        val_b = await counter_b.get()

        assert val_a == 10
        assert val_b == 20
