"""Distributed Cache with Consistent Hashing.

Demonstrates:
- Cache sharded across multiple nodes using consistent hashing
- Each node owns a subset of keys
- Get/Set operations routed to the correct node
- DevelopmentCluster for multi-node setup

Consistent hashing ensures minimal key redistribution when nodes
join or leave the cluster, making it ideal for distributed caches.

Run with: uv run python examples/distributed/03-sharded-cache.py
"""

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Any

from casty import Actor, Context, LocalRef
from casty.cluster import DevelopmentCluster, HashRing


# --- Cache Messages ---

@dataclass
class Get:
    """Get a value from cache."""
    key: str


@dataclass
class Set:
    """Set a value in cache."""
    key: str
    value: Any
    ttl: float | None = None


@dataclass
class Delete:
    """Delete a key from cache."""
    key: str


@dataclass
class GetStats:
    """Get cache statistics."""
    pass


@dataclass
class _ExpireKey:
    """Internal: expire a key after TTL."""
    key: str


# --- Cache Shard Actor ---

class CacheShard(Actor[Get | Set | Delete | GetStats | _ExpireKey]):
    """A single shard of the distributed cache.

    Each shard is responsible for a portion of the key space,
    determined by consistent hashing.
    """

    def __init__(self, shard_id: str):
        self.shard_id = shard_id
        self.data: dict[str, Any] = {}
        self.ttl_tasks: dict[str, str] = {}
        self.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0}

    async def receive(self, msg, ctx: Context):
        match msg:
            case Get(key):
                if key in self.data:
                    self.stats["hits"] += 1
                    await ctx.reply(("found", self.data[key]))
                else:
                    self.stats["misses"] += 1
                    await ctx.reply(("not_found", None))

            case Set(key, value, ttl):
                # Cancel existing TTL if any
                if key in self.ttl_tasks:
                    await ctx.cancel_schedule(self.ttl_tasks[key])

                self.data[key] = value
                self.stats["sets"] += 1

                # Schedule expiration if TTL provided
                if ttl is not None:
                    task_id = await ctx.schedule(ttl, _ExpireKey(key))
                    self.ttl_tasks[key] = task_id

                print(f"  [{self.shard_id}] SET {key}={value}" + (f" (TTL={ttl}s)" if ttl else ""))
                await ctx.reply("ok")

            case Delete(key):
                if key in self.data:
                    del self.data[key]
                    self.stats["deletes"] += 1
                    if key in self.ttl_tasks:
                        await ctx.cancel_schedule(self.ttl_tasks.pop(key))
                    await ctx.reply(True)
                else:
                    await ctx.reply(False)

            case GetStats():
                await ctx.reply({
                    "shard_id": self.shard_id,
                    "keys": len(self.data),
                    **self.stats,
                })

            case _ExpireKey(key):
                if key in self.data:
                    del self.data[key]
                    self.ttl_tasks.pop(key, None)
                    print(f"  [{self.shard_id}] EXPIRED {key}")


# --- Cache Router ---

@dataclass
class RouteGet:
    """Get routed to correct shard."""
    key: str


@dataclass
class RouteSet:
    """Set routed to correct shard."""
    key: str
    value: Any
    ttl: float | None = None


@dataclass
class RouteDelete:
    """Delete routed to correct shard."""
    key: str


@dataclass
class GetAllStats:
    """Get stats from all shards."""
    pass


class CacheRouter(Actor[RouteGet | RouteSet | RouteDelete | GetAllStats]):
    """Routes cache operations to the appropriate shard using consistent hashing.

    The router maintains a hash ring and directs each key to its
    responsible shard based on the key's hash value.
    """

    def __init__(self, shards: dict[str, LocalRef[Any]]):
        self.shards = shards
        self.ring = HashRing(virtual_nodes=100)
        for shard_id in shards.keys():
            self.ring.add_node(shard_id)

    def _get_shard(self, key: str) -> LocalRef[Any]:
        """Get the shard responsible for a key."""
        shard_id = self.ring.get_node(key)
        return self.shards[shard_id]

    async def receive(self, msg, ctx: Context):
        match msg:
            case RouteGet(key):
                shard = self._get_shard(key)
                result = await shard.ask(Get(key))
                await ctx.reply(result)

            case RouteSet(key, value, ttl):
                shard = self._get_shard(key)
                result = await shard.ask(Set(key, value, ttl))
                await ctx.reply(result)

            case RouteDelete(key):
                shard = self._get_shard(key)
                result = await shard.ask(Delete(key))
                await ctx.reply(result)

            case GetAllStats():
                all_stats = []
                for shard in self.shards.values():
                    stats = await shard.ask(GetStats())
                    all_stats.append(stats)
                await ctx.reply(all_stats)


# --- Client Helper Functions ---

async def cache_get(router: LocalRef[Any], key: str) -> tuple[bool, Any]:
    """Get a value from the distributed cache."""
    status, value = await router.ask(RouteGet(key))
    return status == "found", value


async def cache_set(router: LocalRef[Any], key: str, value: Any, ttl: float | None = None) -> bool:
    """Set a value in the distributed cache."""
    result = await router.ask(RouteSet(key, value, ttl))
    return result == "ok"


async def cache_delete(router: LocalRef[Any], key: str) -> bool:
    """Delete a key from the distributed cache."""
    return await router.ask(RouteDelete(key))


async def main():
    print("=== Distributed Cache with Consistent Hashing ===\n")

    async with DevelopmentCluster(3) as (node0, node1, node2):  # type: ignore[misc]
        print(f"Started 3-node cluster")
        await asyncio.sleep(0.5)

        # Create cache shards on each node
        print("\nPhase 1: Creating cache shards")
        print("-" * 50)

        shard0 = await node0.spawn(CacheShard, shard_id="shard-0")
        shard1 = await node1.spawn(CacheShard, shard_id="shard-1")
        shard2 = await node2.spawn(CacheShard, shard_id="shard-2")

        shards = {
            "shard-0": shard0,
            "shard-1": shard1,
            "shard-2": shard2,
        }

        # Create router on node0
        router = await node0.spawn(CacheRouter, shards=shards)

        print("Created 3 shards with consistent hashing router\n")

        print("Phase 2: Setting values")
        print("-" * 50)

        # Set multiple keys - they'll be distributed across shards
        test_data = {
            "user:1001": {"name": "Alice", "email": "alice@example.com"},
            "user:1002": {"name": "Bob", "email": "bob@example.com"},
            "user:1003": {"name": "Charlie", "email": "charlie@example.com"},
            "session:abc123": {"user_id": 1001, "expires": "2024-12-31"},
            "session:def456": {"user_id": 1002, "expires": "2024-12-31"},
            "config:app": {"debug": False, "version": "1.0.0"},
            "counter:visits": 12345,
            "cache:popular": ["item1", "item2", "item3"],
        }

        for key, value in test_data.items():
            await cache_set(router, key, value)

        print("\nPhase 3: Checking key distribution")
        print("-" * 50)

        stats = await router.ask(GetAllStats())
        for shard_stats in stats:
            print(f"  {shard_stats['shard_id']}: {shard_stats['keys']} keys, "
                  f"{shard_stats['sets']} sets, {shard_stats['hits']} hits")

        print("\nPhase 4: Getting values")
        print("-" * 50)

        # Get some values
        found, user = await cache_get(router, "user:1001")
        print(f"user:1001 -> {'FOUND' if found else 'MISS'}: {user}")

        found, session = await cache_get(router, "session:abc123")
        print(f"session:abc123 -> {'FOUND' if found else 'MISS'}: {session}")

        found, missing = await cache_get(router, "nonexistent:key")
        print(f"nonexistent:key -> {'FOUND' if found else 'MISS'}")

        print("\nPhase 5: TTL expiration")
        print("-" * 50)

        # Set a key with TTL
        await cache_set(router, "temp:data", "expires soon", ttl=2.0)

        # Verify it exists
        found, value = await cache_get(router, "temp:data")
        print(f"temp:data (before TTL) -> {'FOUND' if found else 'MISS'}: {value}")

        # Wait for expiration
        print("Waiting 2.5s for TTL expiration...")
        await asyncio.sleep(2.5)

        found, value = await cache_get(router, "temp:data")
        print(f"temp:data (after TTL) -> {'FOUND' if found else 'MISS'}")

        print("\nPhase 6: Demonstrating consistent hashing")
        print("-" * 50)

        # Show which shard handles each key
        ring = HashRing(virtual_nodes=100)
        for shard_id in shards.keys():
            ring.add_node(shard_id)

        print("Key -> Shard mapping:")
        for key in list(test_data.keys())[:5]:
            shard_id = ring.get_node(key)
            print(f"  {key} -> {shard_id}")

        print("\nPhase 7: Final statistics")
        print("-" * 50)

        stats = await router.ask(GetAllStats())
        total_keys = sum(s["keys"] for s in stats)
        total_hits = sum(s["hits"] for s in stats)
        total_misses = sum(s["misses"] for s in stats)

        print(f"\nCluster stats:")
        print(f"  Total keys: {total_keys}")
        print(f"  Total hits: {total_hits}")
        print(f"  Total misses: {total_misses}")
        print(f"  Hit rate: {total_hits / (total_hits + total_misses) * 100:.1f}%")

        for shard_stats in stats:
            print(f"\n  {shard_stats['shard_id']}:")
            print(f"    Keys: {shard_stats['keys']}")
            print(f"    Sets: {shard_stats['sets']}")
            print(f"    Hits: {shard_stats['hits']}")
            print(f"    Misses: {shard_stats['misses']}")

        print("\n=== Summary ===")
        print("Distributed cache features demonstrated:")
        print("  - Consistent hashing for key distribution")
        print("  - Each shard handles a portion of the key space")
        print("  - TTL-based expiration")
        print("  - Statistics collection across shards")
        print("  - Minimal redistribution when topology changes")


if __name__ == "__main__":
    asyncio.run(main())
