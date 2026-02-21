from __future__ import annotations

import asyncio

from casty import ActorSystem, InMemoryJournal
from casty.cluster.coordinator import LeastShardStrategy, shard_coordinator_actor
from casty.cluster.region import shard_region_actor
from casty.cluster.state import NodeAddress
from casty.distributed import Queue
from casty.distributed.queue import persistent_queue_entity


async def test_persistent_queue_multinode_recovery() -> None:
    """Node A enqueues items, shuts down. Node B starts with same journal and dequeues them."""
    node_a = NodeAddress(host="127.0.0.1", port=25510)
    node_b = NodeAddress(host="127.0.0.1", port=25511)
    journal = InMemoryJournal()

    # Node A: enqueue items
    async with ActorSystem(name="node-a") as system_a:
        entity_factory = persistent_queue_entity(journal)
        coord_ref = system_a.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a}),
            ),
            "coordinator",
        )
        region_ref = system_a.spawn(
            shard_region_actor(
                self_node=node_a,
                coordinator=coord_ref,
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        q: Queue[str] = Queue(gateway=system_a, region_ref=region_ref, name="work")
        await q.enqueue("job-1")
        await q.enqueue("job-2")
        await q.enqueue("job-3")
        assert await q.size() == 3

    # Node B: different node, same journal — should recover and dequeue in FIFO order
    async with ActorSystem(name="node-b") as system_b:
        entity_factory = persistent_queue_entity(journal)
        coord_ref = system_b.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_b}),
            ),
            "coordinator",
        )
        region_ref = system_b.spawn(
            shard_region_actor(
                self_node=node_b,
                coordinator=coord_ref,
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        q: Queue[str] = Queue(gateway=system_b, region_ref=region_ref, name="work")
        assert await q.size() == 3
        assert await q.peek() == "job-1"
        assert await q.dequeue() == "job-1"
        assert await q.dequeue() == "job-2"
        assert await q.dequeue() == "job-3"
        assert await q.dequeue() is None


async def test_persistent_queue_partial_dequeue_recovery() -> None:
    """Node A enqueues 3, dequeues 1. Node B should see 2 remaining in order."""
    node_a = NodeAddress(host="127.0.0.1", port=25510)
    node_b = NodeAddress(host="127.0.0.1", port=25511)
    journal = InMemoryJournal()

    # Node A: enqueue 3, dequeue 1
    async with ActorSystem(name="node-a") as system_a:
        entity_factory = persistent_queue_entity(journal)
        coord_ref = system_a.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a}),
            ),
            "coordinator",
        )
        region_ref = system_a.spawn(
            shard_region_actor(
                self_node=node_a,
                coordinator=coord_ref,
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        q: Queue[str] = Queue(gateway=system_a, region_ref=region_ref, name="tasks")
        await q.enqueue("a")
        await q.enqueue("b")
        await q.enqueue("c")
        assert await q.dequeue() == "a"  # dequeue first
        assert await q.size() == 2

    # Node B: should see "b", "c" remaining
    async with ActorSystem(name="node-b") as system_b:
        entity_factory = persistent_queue_entity(journal)
        coord_ref = system_b.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_b}),
            ),
            "coordinator",
        )
        region_ref = system_b.spawn(
            shard_region_actor(
                self_node=node_b,
                coordinator=coord_ref,
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        q: Queue[str] = Queue(gateway=system_b, region_ref=region_ref, name="tasks")
        assert await q.size() == 2
        assert await q.dequeue() == "b"
        assert await q.dequeue() == "c"
        assert await q.dequeue() is None
