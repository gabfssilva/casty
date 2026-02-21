from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty.cluster.coordinator import (
    shard_coordinator_actor,
    LeastShardStrategy,
)
from casty.cluster.region import shard_region_actor
from casty.cluster.state import NodeAddress
from casty.distributed import Queue
from casty.distributed.queue import queue_entity


async def test_queue_enqueue_dequeue() -> None:
    """Empty size=0, dequeue=None, peek=None, enqueue 3 items, size=3,
    peek=first, size still 3, dequeue in FIFO order, dequeue empty=None, size=0."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="queue-test") as system:
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
                entity_factory=queue_entity,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        q: Queue[str] = Queue(
            gateway=system, region_ref=region_ref, name="my-queue", timeout=5.0
        )

        # Empty queue checks
        assert await q.size() == 0
        assert await q.dequeue() is None
        assert await q.peek() is None

        # Enqueue 3 items
        await q.enqueue("a")
        await q.enqueue("b")
        await q.enqueue("c")

        # Size = 3
        assert await q.size() == 3

        # Peek returns first without removing
        assert await q.peek() == "a"
        assert await q.size() == 3

        # Dequeue in FIFO order
        assert await q.dequeue() == "a"
        assert await q.dequeue() == "b"
        assert await q.dequeue() == "c"

        # Empty again
        assert await q.dequeue() is None
        assert await q.size() == 0
