"""Distributed Cache with DistributedCache API.

Demonstrates:
- Simple distributed cache using casty.cluster.cache
- Multi-node cluster with DevelopmentCluster
- Automatic sharding via consistent hashing
- Per-key TTL support
- Replication across nodes
- Values are automatically serialized via msgpack

This is the high-level API built on top of Casty's clustering primitives.
For a lower-level example, see 03-sharded-cache.py.

Run with: uv run python examples/distributed/05-distributed-cache.py
"""

import asyncio
from casty.cluster import DevelopmentCluster
from casty.cluster.cache import DistributedCache


async def main():
    print("=== Distributed Cache Example ===\n")

    # Using cluster directly - no need to access specific nodes
    async with DevelopmentCluster(10) as cluster:
        print(f"Started {len(cluster)}-node cluster\n")

        cache = await DistributedCache.create(
            cluster,
            name="my-cache",
        )
        print("Created distributed cache\n")

        # Set some values
        print("--- Setting values ---")
        await cache.set("user:1", {"name": "Alice", "email": "alice@example.com"})
        await cache.set("user:2", {"name": "Bob", "email": "bob@example.com"})
        await cache.set("counter", 42)
        await cache.set("tags", ["python", "actor", "distributed"])
        print("Set 4 keys\n")

        # Get values
        print("--- Getting values ---")
        user1 = await cache.get("user:1")
        print(f"user:1 = {user1}")

        counter = await cache.get("counter")
        print(f"counter = {counter}")

        tags = await cache.get("tags")
        print(f"tags = {tags}")

        missing = await cache.get("nonexistent")
        print(f"nonexistent = {missing}\n")

        # TTL example
        print("--- TTL expiration ---")
        await cache.set("temp", "expires soon", ttl=1.0)

        value = await cache.get("temp")
        print(f"temp (before TTL) = {value}")

        print("Waiting 1.5s...")
        await asyncio.sleep(1.5)

        value = await cache.get("temp")
        print(f"temp (after TTL) = {value}\n")

        # Delete
        print("--- Deleting ---")
        await cache.delete("user:2")
        user2 = await cache.get("user:2")
        print(f"user:2 after delete = {user2}\n")

        # Update
        print("--- Updating ---")
        await cache.set("counter", 100)
        counter = await cache.get("counter")
        print(f"counter after update = {counter}\n")

    print("=== Done ===")


if __name__ == "__main__":
    asyncio.run(main())
