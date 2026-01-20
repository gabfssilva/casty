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

from casty import actor, ActorSystem, Mailbox


@dataclass
class Set:
    key: str
    value: Any
    ttl: float | None = None


@dataclass
class Get:
    key: str


@dataclass
class Delete:
    key: str


@dataclass
class GetStats:
    pass


@dataclass
class Clear:
    pass


@dataclass
class _Expire:
    key: str
    set_at: datetime


@dataclass
class CacheStats:
    size: int
    hits: int
    misses: int
    evictions: int
    hit_rate: float


@dataclass
class CacheEntry:
    value: Any
    set_at: datetime = field(default_factory=datetime.now)
    expires_at: datetime | None = None


CacheMsg = Set | Get | Delete | GetStats | Clear | _Expire


@actor
async def cache(cache_name: str = "default", default_ttl: float | None = None, *, mailbox: Mailbox[CacheMsg]):
    entries: dict[str, CacheEntry] = {}
    hits = 0
    misses = 0
    evictions = 0

    async for msg, ctx in mailbox:
        match msg:
            case Set(key, value, ttl):
                actual_ttl = ttl if ttl is not None else default_ttl
                now = datetime.now()
                entry = CacheEntry(
                    value=value,
                    set_at=now,
                    expires_at=now + timedelta(seconds=actual_ttl) if actual_ttl else None,
                )

                if actual_ttl:
                    await ctx.schedule(_Expire(key, set_at=now), delay=actual_ttl)

                entries[key] = entry
                ttl_str = f" (TTL: {actual_ttl}s)" if actual_ttl else ""
                print(f"[Cache:{cache_name}] SET {key} = {value}{ttl_str}")

            case Get(key):
                entry = entries.get(key)

                if entry is None:
                    misses += 1
                    print(f"[Cache:{cache_name}] GET {key} -> MISS")
                    await ctx.reply(None)
                    continue

                if entry.expires_at and datetime.now() > entry.expires_at:
                    del entries[key]
                    misses += 1
                    evictions += 1
                    print(f"[Cache:{cache_name}] GET {key} -> EXPIRED")
                    await ctx.reply(None)
                    continue

                hits += 1
                print(f"[Cache:{cache_name}] GET {key} -> HIT ({entry.value})")
                await ctx.reply(entry.value)

            case Delete(key):
                entry = entries.pop(key, None)
                if entry:
                    print(f"[Cache:{cache_name}] DELETE {key}")
                else:
                    print(f"[Cache:{cache_name}] DELETE {key} -> NOT FOUND")

            case _Expire(key, set_at):
                entry = entries.get(key)
                if entry and entry.set_at == set_at:
                    del entries[key]
                    evictions += 1
                    print(f"[Cache:{cache_name}] EXPIRED {key}")

            case GetStats():
                total = hits + misses
                hit_rate = hits / total if total > 0 else 0.0
                stats = CacheStats(
                    size=len(entries),
                    hits=hits,
                    misses=misses,
                    evictions=evictions,
                    hit_rate=hit_rate,
                )
                await ctx.reply(stats)

            case Clear():
                count = len(entries)
                entries.clear()
                print(f"[Cache:{cache_name}] CLEARED {count} entries")


async def main():
    print("=" * 60)
    print("Casty Cache with TTL Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        c = await system.actor(cache(cache_name="user-cache", default_ttl=5.0), name="cache-user-cache")
        print("Cache 'user-cache' created (default TTL: 5s)")
        print()

        print("--- Setting entries ---")
        await c.send(Set("user:1", {"name": "Alice", "role": "admin"}))
        await c.send(Set("user:2", {"name": "Bob", "role": "user"}, ttl=1.0))
        await c.send(Set("user:3", {"name": "Charlie", "role": "user"}))
        await asyncio.sleep(0.1)
        print()

        print("--- Getting entries (immediate) ---")
        await c.ask(Get("user:1"))
        await c.ask(Get("user:2"))
        await c.ask(Get("user:3"))
        await c.ask(Get("user:4"))
        print()

        print("--- Waiting 1.5s for user:2 to expire ---")
        await asyncio.sleep(1.5)
        print()

        print("--- Getting entries (after 1.5s) ---")
        await c.ask(Get("user:2"))
        await c.ask(Get("user:1"))
        print()

        print("--- Updating entry ---")
        await c.send(Set("user:1", {"name": "Alice", "role": "superadmin"}, ttl=2.0))
        await asyncio.sleep(0.1)
        print()

        print("--- Deleting entry ---")
        await c.send(Delete("user:3"))
        await asyncio.sleep(0.1)
        print()

        print("--- Statistics (mid-session) ---")
        stats: CacheStats = await c.ask(GetStats())
        print(f"Size:      {stats.size}")
        print(f"Hits:      {stats.hits}")
        print(f"Misses:    {stats.misses}")
        print(f"Evictions: {stats.evictions}")
        print(f"Hit rate:  {stats.hit_rate:.1%}")
        print()

        print("--- Waiting 3s for remaining entries to expire ---")
        await asyncio.sleep(3.0)
        print()

        print("--- Getting entries (after expiration) ---")
        await c.ask(Get("user:1"))
        print()

        print("--- Final Statistics ---")
        stats = await c.ask(GetStats())
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
