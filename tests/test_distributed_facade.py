from __future__ import annotations

import asyncio

from casty.distributed import Distributed
from casty.sharding import ClusteredActorSystem


async def test_distributed_counter() -> None:
    """Create counter via d.counter('hits'), increment, get."""
    async with ClusteredActorSystem(name="dist-counter", host="127.0.0.1", port=0) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        counter = d.counter("hits", shards=10)
        val = await counter.increment(3)
        assert val == 3

        val = await counter.get()
        assert val == 3


async def test_distributed_map_bracket_syntax() -> None:
    """Create map via d.map[str, str]('users'), put, get."""
    async with ClusteredActorSystem(name="dist-map", host="127.0.0.1", port=0) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        users = d.map[str, str]("users", shards=10)

        old = await users.put("alice", "Alice Smith")
        assert old is None

        val = await users.get("alice")
        assert val == "Alice Smith"


async def test_distributed_set_bracket_syntax() -> None:
    """Create set via d.set[str]('tags'), add, contains."""
    async with ClusteredActorSystem(name="dist-set", host="127.0.0.1", port=0) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        tags = d.set[str]("tags", shards=10)

        added = await tags.add("python")
        assert added is True

        present = await tags.contains("python")
        assert present is True

        absent = await tags.contains("java")
        assert absent is False


async def test_distributed_queue_bracket_syntax() -> None:
    """Create queue via d.queue[str]('jobs'), enqueue, dequeue."""
    async with ClusteredActorSystem(name="dist-queue", host="127.0.0.1", port=0) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        jobs = d.queue[str]("jobs", shards=10)

        await jobs.enqueue("task-1")
        await jobs.enqueue("task-2")

        first = await jobs.dequeue()
        assert first == "task-1"

        second = await jobs.dequeue()
        assert second == "task-2"

        empty = await jobs.dequeue()
        assert empty is None
