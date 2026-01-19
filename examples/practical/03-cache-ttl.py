"""Cache with TTL Example - Time-Based Expiration.

Demonstrates:
- In-memory cache actor with Get/Set operations
- Per-entry TTL (time-to-live) using ctx.schedule()
- Automatic eviction of expired entries
- Cache statistics: hits, misses, evictions

Run with:
    uv run python examples/practical/03-cache-ttl.py
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from casty import Actor, ActorSystem, Context, on


# --- Messages ---

@dataclass
class Set:
    """Set a cache entry with optional TTL."""
    key: str
    value: Any
    ttl: float | None = None  # seconds, None = no expiration


@dataclass
class Get:
    """Get a cache entry by key."""
    key: str


@dataclass
class Delete:
    """Delete a cache entry."""
    key: str


@dataclass
class GetStats:
    """Get cache statistics."""
    pass


@dataclass
class Clear:
    """Clear all cache entries."""
    pass


@dataclass
class _Expire:
    """Internal message to expire an entry."""
    key: str
    set_at: datetime


@dataclass
class CacheStats:
    """Cache statistics."""
    size: int
    hits: int
    misses: int
    evictions: int
    hit_rate: float


@dataclass
class CacheEntry:
    """Internal cache entry with metadata."""
    value: Any
    set_at: datetime = field(default_factory=datetime.now)
    expires_at: datetime | None = None
    ttl_task_id: str | None = None


# --- Cache Actor ---

type CacheMessage = Set | Get | Delete | GetStats | Clear | _Expire


class Cache(Actor[CacheMessage]):
    """In-memory cache with TTL support.

    Features:
    - Get/Set/Delete operations
    - Per-entry TTL with automatic expiration
    - Statistics tracking (hits, misses, evictions)
    - Uses ctx.schedule() for expiration timers
    """

    def __init__(self, cache_name: str = "default", default_ttl: float | None = None):
        self.cache_name = cache_name
        self.default_ttl = default_ttl
        self.entries: dict[str, CacheEntry] = {}
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    @on(Set)
    async def handle_set(self, msg: Set, ctx: Context) -> None:
        key = msg.key
        ttl = msg.ttl if msg.ttl is not None else self.default_ttl

        # Cancel existing expiration timer if present
        if key in self.entries:
            old_entry = self.entries[key]
            if old_entry.ttl_task_id:
                await ctx.cancel_schedule(old_entry.ttl_task_id)

        # Create entry
        now = datetime.now()
        entry = CacheEntry(
            value=msg.value,
            set_at=now,
            expires_at=now + timedelta(seconds=ttl) if ttl else None,
        )

        # Schedule expiration if TTL is set
        if ttl:
            task_id = await ctx.schedule(ttl, _Expire(key, set_at=now))
            entry.ttl_task_id = task_id

        self.entries[key] = entry
        ttl_str = f" (TTL: {ttl}s)" if ttl else ""
        print(f"[Cache:{self.cache_name}] SET {key} = {msg.value}{ttl_str}")

    @on(Get)
    async def handle_get(self, msg: Get, ctx: Context) -> None:
        key = msg.key
        entry = self.entries.get(key)

        if entry is None:
            self.misses += 1
            print(f"[Cache:{self.cache_name}] GET {key} -> MISS")
            await ctx.reply(None)
            return

        # Check if expired (belt-and-suspenders check)
        if entry.expires_at and datetime.now() > entry.expires_at:
            del self.entries[key]
            self.misses += 1
            self.evictions += 1
            print(f"[Cache:{self.cache_name}] GET {key} -> EXPIRED")
            await ctx.reply(None)
            return

        self.hits += 1
        print(f"[Cache:{self.cache_name}] GET {key} -> HIT ({entry.value})")
        await ctx.reply(entry.value)

    @on(Delete)
    async def handle_delete(self, msg: Delete, ctx: Context) -> None:
        key = msg.key
        entry = self.entries.pop(key, None)

        if entry:
            if entry.ttl_task_id:
                await ctx.cancel_schedule(entry.ttl_task_id)
            print(f"[Cache:{self.cache_name}] DELETE {key}")
        else:
            print(f"[Cache:{self.cache_name}] DELETE {key} -> NOT FOUND")

    @on(_Expire)
    async def handle_expire(self, msg: _Expire, ctx: Context) -> None:
        entry = self.entries.get(msg.key)

        # Only expire if the entry exists and wasn't overwritten
        if entry and entry.set_at == msg.set_at:
            del self.entries[msg.key]
            self.evictions += 1
            print(f"[Cache:{self.cache_name}] EXPIRED {msg.key}")

    @on(GetStats)
    async def handle_stats(self, msg: GetStats, ctx: Context) -> None:
        total = self.hits + self.misses
        hit_rate = self.hits / total if total > 0 else 0.0
        stats = CacheStats(
            size=len(self.entries),
            hits=self.hits,
            misses=self.misses,
            evictions=self.evictions,
            hit_rate=hit_rate,
        )
        await ctx.reply(stats)

    @on(Clear)
    async def handle_clear(self, msg: Clear, ctx: Context) -> None:
        # Cancel all expiration timers
        for entry in self.entries.values():
            if entry.ttl_task_id:
                await ctx.cancel_schedule(entry.ttl_task_id)

        count = len(self.entries)
        self.entries.clear()
        print(f"[Cache:{self.cache_name}] CLEARED {count} entries")


# --- Main ---

async def main():
    print("=" * 60)
    print("Casty Cache with TTL Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        # Create cache with 5-second default TTL
        cache = await system.actor(Cache, name="cache-user-cache", cache_name="user-cache", default_ttl=5.0)
        print("Cache 'user-cache' created (default TTL: 5s)")
        print()

        # Set some entries
        print("--- Setting entries ---")
        await cache.send(Set("user:1", {"name": "Alice", "role": "admin"}))
        await cache.send(Set("user:2", {"name": "Bob", "role": "user"}, ttl=1.0))  # 1s TTL
        await cache.send(Set("user:3", {"name": "Charlie", "role": "user"}))  # default TTL
        await asyncio.sleep(0.1)
        print()

        # Get entries (all should be hits)
        print("--- Getting entries (immediate) ---")
        user1 = await cache.ask(Get("user:1"))
        user2 = await cache.ask(Get("user:2"))
        user3 = await cache.ask(Get("user:3"))
        user4 = await cache.ask(Get("user:4"))  # miss
        print()

        # Wait for user:2 to expire (1s TTL)
        print("--- Waiting 1.5s for user:2 to expire ---")
        await asyncio.sleep(1.5)
        print()

        # Get entries again
        print("--- Getting entries (after 1.5s) ---")
        user2_after = await cache.ask(Get("user:2"))  # should be expired
        user1_after = await cache.ask(Get("user:1"))  # should still be there
        print()

        # Update an entry
        print("--- Updating entry ---")
        await cache.send(Set("user:1", {"name": "Alice", "role": "superadmin"}, ttl=2.0))
        await asyncio.sleep(0.1)
        print()

        # Delete an entry
        print("--- Deleting entry ---")
        await cache.send(Delete("user:3"))
        await asyncio.sleep(0.1)
        print()

        # Stats at this point
        print("--- Statistics (mid-session) ---")
        stats: CacheStats = await cache.ask(GetStats())
        print(f"Size:      {stats.size}")
        print(f"Hits:      {stats.hits}")
        print(f"Misses:    {stats.misses}")
        print(f"Evictions: {stats.evictions}")
        print(f"Hit rate:  {stats.hit_rate:.1%}")
        print()

        # Wait for remaining entries to expire
        print("--- Waiting 3s for remaining entries to expire ---")
        await asyncio.sleep(3.0)
        print()

        # Try to get expired entries
        print("--- Getting entries (after expiration) ---")
        user1_final = await cache.ask(Get("user:1"))  # should be expired
        print()

        # Final stats
        print("--- Final Statistics ---")
        stats = await cache.ask(GetStats())
        print(f"Size:      {stats.size}")
        print(f"Hits:      {stats.hits}")
        print(f"Misses:    {stats.misses}")
        print(f"Evictions: {stats.evictions}")
        print(f"Hit rate:  {stats.hit_rate:.1%}")

    print()
    print("=" * 60)
    print("Cache shut down")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
