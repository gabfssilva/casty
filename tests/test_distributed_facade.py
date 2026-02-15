from __future__ import annotations

import asyncio

from casty.distributed import Distributed
from casty.distributed.lock import Lock
from casty.cluster.system import ClusteredActorSystem


async def test_distributed_counter() -> None:
    """Create counter via d.counter('hits'), increment, get."""
    async with ClusteredActorSystem(
        name="dist-counter", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        counter = d.counter("hits", shards=10)
        val = await counter.increment(3)
        assert val == 3

        val = await counter.get()
        assert val == 3


async def test_distributed_map_bracket_syntax() -> None:
    """Create map via d.map[str, str]('users'), put, get."""
    async with ClusteredActorSystem(
        name="dist-map", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        users = d.map[str, str]("users", shards=10)

        old = await users.put("alice", "Alice Smith")
        assert old is None

        val = await users.get("alice")
        assert val == "Alice Smith"


async def test_distributed_set_bracket_syntax() -> None:
    """Create set via d.set[str]('tags'), add, contains."""
    async with ClusteredActorSystem(
        name="dist-set", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
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
    async with ClusteredActorSystem(
        name="dist-queue", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
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


async def test_distributed_lock_acquire_release() -> None:
    """Acquire then release a distributed lock."""
    async with ClusteredActorSystem(
        name="dist-lock-1", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        lock = d.lock("my-lock", shards=10)
        await lock.acquire()
        released = await lock.release()
        assert released is True


async def test_distributed_lock_try_acquire() -> None:
    """try_acquire returns True when free, False when held."""
    async with ClusteredActorSystem(
        name="dist-lock-2", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        lock1 = d.lock("try-lock", shards=10)
        lock2 = d.lock("try-lock", shards=10)

        got = await lock1.try_acquire()
        assert got is True

        got2 = await lock2.try_acquire()
        assert got2 is False


async def test_distributed_lock_wrong_owner_release() -> None:
    """Releasing a lock held by someone else returns False."""
    async with ClusteredActorSystem(
        name="dist-lock-3", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        lock1 = d.lock("owned-lock", shards=10)
        lock2 = d.lock("owned-lock", shards=10)

        await lock1.acquire()
        released = await lock2.release()
        assert released is False

        released = await lock1.release()
        assert released is True


async def test_distributed_lock_fifo_waiters() -> None:
    """Blocked acquirers are granted in FIFO order."""
    async with ClusteredActorSystem(
        name="dist-lock-4", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        lock1 = d.lock("fifo-lock", shards=10)
        lock2 = d.lock("fifo-lock", shards=10)
        lock3 = d.lock("fifo-lock", shards=10)

        await lock1.acquire()

        order: list[int] = []

        async def waiter(lock: Lock, idx: int) -> None:
            await lock.acquire()
            order.append(idx)

        t2 = asyncio.create_task(waiter(lock2, 2))
        t3 = asyncio.create_task(waiter(lock3, 3))
        await asyncio.sleep(0.1)

        await lock1.release()
        await asyncio.sleep(0.1)

        await lock2.release()
        await asyncio.sleep(0.1)

        await t2
        await t3

        assert order == [2, 3]


async def test_distributed_semaphore_permits() -> None:
    """N permits allow N concurrent holders; N+1th blocks."""
    async with ClusteredActorSystem(
        name="dist-sem-1", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        sem1 = d.semaphore("my-sem", permits=2, shards=10)
        sem2 = d.semaphore("my-sem", permits=2, shards=10)
        sem3 = d.semaphore("my-sem", permits=2, shards=10)

        got1 = await sem1.try_acquire()
        assert got1 is True

        got2 = await sem2.try_acquire()
        assert got2 is True

        got3 = await sem3.try_acquire()
        assert got3 is False

        released = await sem1.release()
        assert released is True

        got3_retry = await sem3.try_acquire()
        assert got3_retry is True


async def test_distributed_semaphore_wrong_owner_release() -> None:
    """Releasing a semaphore not held by this owner returns False."""
    async with ClusteredActorSystem(
        name="dist-sem-2", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        sem1 = d.semaphore("sem-owner", permits=1, shards=10)
        sem2 = d.semaphore("sem-owner", permits=1, shards=10)

        await sem1.acquire()
        released = await sem2.release()
        assert released is False

        released = await sem1.release()
        assert released is True


async def test_distributed_barrier_via_facade() -> None:
    """Barrier through the Distributed facade."""
    async with ClusteredActorSystem(
        name="dist-barrier", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        b = d.barrier("gate")
        await b.arrive(expected=1)


async def test_destroy_stops_entity_and_allows_respawn() -> None:
    """Destroy frees the entity actor; subsequent use re-spawns from scratch."""
    async with ClusteredActorSystem(
        name="dist-destroy", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        d = Distributed(system)
        await asyncio.sleep(0.1)

        counter = d.counter("ephemeral", shards=10)
        await counter.increment(10)
        assert await counter.get() == 10

        assert await counter.destroy() is True
        await asyncio.sleep(0.05)

        assert await counter.get() == 0

        tags = d.set[str]("ephemeral-set", shards=10)
        await tags.add("a")
        assert await tags.contains("a") is True

        assert await tags.destroy() is True
        await asyncio.sleep(0.05)

        assert await tags.contains("a") is False

        jobs = d.queue[str]("ephemeral-queue", shards=10)
        await jobs.enqueue("x")
        assert await jobs.destroy() is True
        await asyncio.sleep(0.05)

        assert await jobs.dequeue() is None

        lock = d.lock("ephemeral-lock", shards=10)
        await lock.acquire()
        assert await lock.destroy() is True
        await asyncio.sleep(0.05)

        got = await lock.try_acquire()
        assert got is True

        sem = d.semaphore("ephemeral-sem", permits=1, shards=10)
        await sem.acquire()
        assert await sem.destroy() is True
        await asyncio.sleep(0.05)

        got = await sem.try_acquire()
        assert got is True

        barrier = d.barrier("ephemeral-barrier")
        assert await barrier.destroy() is True
