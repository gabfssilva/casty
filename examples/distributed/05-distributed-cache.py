"""Distributed Cache with DistributedCache API.

Demonstrates:
- Simple distributed cache using casty.cluster.cache
- Entry-per-actor pattern (each cache key is a separate actor)
- Multi-node cluster with DevelopmentCluster
- Per-key TTL support with automatic cleanup
- Values are automatically serialized via msgpack

Run with: uv run python examples/distributed/05-distributed-cache.py
"""

import asyncio
from casty.cluster import DevelopmentCluster
from casty.cluster.development import DistributionStrategy
from casty.cluster.cache import DistributedCache


async def main():
    print("=== Distributed Cache Example ===\n")

    async with DevelopmentCluster(3, strategy=DistributionStrategy.ROUND_ROBIN, debug=False) as cluster:
        print(f"Started {len(cluster)}-node cluster\n")

        # Debug: Check membership on each node
        from casty.cluster.messages import GetAliveMembers, GetResponsibleNodes
        print("--- Debug: Membership on each node ---")
        for i, node in enumerate(cluster.nodes):
            members = await node._membership_ref.ask(GetAliveMembers())
            print(f"  node-{i} sees members: {list(members.keys())}")

        # Debug: Check who each node thinks is the leader for cache:user:1
        test_key = "cache_entry/cache:user:1"
        print(f"\n--- Debug: Leader for '{test_key}' ---")
        for i, node in enumerate(cluster.nodes):
            leaders = await node._membership_ref.ask(GetResponsibleNodes(test_key, 3))
            print(f"  node-{i} thinks leader is: {leaders[0] if leaders else 'none'}")
        print()

        cache = DistributedCache(cluster, replicas=3)
        print("Created distributed cache\n")

        # Set some values
        print("--- Setting values ---")
        await cache.set("user:1", {"name": "Alice", "email": "alice@example.com"})
        await cache.set("user:2", {"name": "Bob", "email": "bob@example.com"})
        await cache.set("counter", 42)
        await cache.set("tags", ["python", "actor", "distributed"])
        print("Set 4 keys\n")

        await asyncio.sleep(1)

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

        # Check existence
        print("--- Checking existence ---")
        exists = await cache.exists("user:1")
        print(f"user:1 exists = {exists}")

        exists = await cache.exists("nonexistent")
        print(f"nonexistent exists = {exists}\n")

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
