from __future__ import annotations

import asyncio

from casty import ActorSystem, InMemoryJournal
from casty.cluster.coordinator import LeastShardStrategy, shard_coordinator_actor
from casty.cluster.region import shard_region_actor
from casty.cluster.state import NodeAddress
from casty.distributed import Counter
from casty.distributed.counter import persistent_counter_entity


async def test_persistent_counter_survives_replay() -> None:
    """Persistent counter recovers state from journal after restart."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)
    journal = InMemoryJournal()

    # First run: increment counter to 8
    async with ActorSystem(name="persist-test-1") as system:
        entity_factory = persistent_counter_entity(journal)
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
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        counter = Counter(
            gateway=system, region_ref=region_ref, name="hits", timeout=5.0
        )

        val = await counter.increment(5)
        assert val == 5

        val = await counter.increment(3)
        assert val == 8

        val = await counter.get()
        assert val == 8

    # Second run with SAME journal: counter should recover to 8 via event replay
    async with ActorSystem(name="persist-test-2") as system:
        entity_factory = persistent_counter_entity(journal)
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
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        counter = Counter(
            gateway=system, region_ref=region_ref, name="hits", timeout=5.0
        )

        # Should recover to 8 from journal events
        val = await counter.get()
        assert val == 8


async def test_persistent_counter_decrement_survives_replay() -> None:
    """Persistent counter handles mixed increment/decrement and recovers."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)
    journal = InMemoryJournal()

    # First run: increment 10, decrement 3 -> 7
    async with ActorSystem(name="persist-dec-1") as system:
        entity_factory = persistent_counter_entity(journal)
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
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        counter = Counter(
            gateway=system, region_ref=region_ref, name="score", timeout=5.0
        )

        val = await counter.increment(10)
        assert val == 10

        val = await counter.decrement(3)
        assert val == 7

    # Second run: should recover to 7
    async with ActorSystem(name="persist-dec-2") as system:
        entity_factory = persistent_counter_entity(journal)
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
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        counter = Counter(
            gateway=system, region_ref=region_ref, name="score", timeout=5.0
        )

        val = await counter.get()
        assert val == 7

        # Continue incrementing after recovery
        val = await counter.increment(3)
        assert val == 10


async def test_persistent_counter_events_in_journal() -> None:
    """Verify that events are actually persisted in the journal."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)
    journal = InMemoryJournal()

    async with ActorSystem(name="persist-journal") as system:
        entity_factory = persistent_counter_entity(journal)
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
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        counter = Counter(
            gateway=system, region_ref=region_ref, name="metrics", timeout=5.0
        )

        await counter.increment(5)
        await counter.decrement(2)
        await asyncio.sleep(0.1)

    # Check journal contents
    events = await journal.load("metrics")
    assert len(events) == 2
    from casty.distributed.counter import Decremented, Incremented

    assert events[0].event == Incremented(5)
    assert events[1].event == Decremented(2)
