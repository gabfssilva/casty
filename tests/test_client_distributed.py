"""Integration tests for distributed collections via ClusterClient."""

from __future__ import annotations

import asyncio

from casty.client.client import ClusterClient
from casty.cluster.system import ClusteredActorSystem


async def test_client_distributed_counter() -> None:
    """ClusterClient.distributed() counter follows the same API as ClusteredActorSystem."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        server_d = system.distributed()
        server_counter = server_d.counter("hits", shards=10)
        await server_counter.increment(5)

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            d = client.distributed()
            counter = d.counter("hits", shards=10)

            val = await counter.get()
            assert val == 5

            val = await counter.increment(3)
            assert val == 8


async def test_client_distributed_map() -> None:
    """ClusterClient.distributed() map follows the same API as ClusteredActorSystem."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        server_d = system.distributed()
        users = server_d.map[str, str]("users", shards=10)
        await users.put("alice", "Alice Smith")

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            d = client.distributed()
            users = d.map[str, str]("users", shards=10)

            val = await users.get("alice")
            assert val == "Alice Smith"


async def test_client_distributed_set() -> None:
    """ClusterClient.distributed() set follows the same API as ClusteredActorSystem."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        server_d = system.distributed()
        tags = server_d.set[str]("tags", shards=10)
        await tags.add("python")

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            d = client.distributed()
            tags = d.set[str]("tags", shards=10)

            assert await tags.contains("python") is True
            assert await tags.contains("java") is False


async def test_client_distributed_queue() -> None:
    """ClusterClient.distributed() queue follows the same API as ClusteredActorSystem."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        server_d = system.distributed()
        jobs = server_d.queue[str]("jobs", shards=10)
        await jobs.enqueue("task-1")
        await jobs.enqueue("task-2")

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            d = client.distributed()
            jobs = d.queue[str]("jobs", shards=10)

            first = await jobs.dequeue()
            assert first == "task-1"


async def test_client_distributed_lock() -> None:
    """ClusterClient.distributed() lock follows the same API as ClusteredActorSystem."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        server_d = system.distributed()
        _ = server_d.lock("my-lock", shards=10)

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            d = client.distributed()
            lock = d.lock("my-lock", shards=10)

            got = await lock.try_acquire()
            assert got is True

            released = await lock.release()
            assert released is True
