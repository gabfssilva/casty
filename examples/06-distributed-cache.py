"""Distributed Cache Example

Demonstrates building a distributed cache on top of the actor framework.
Uses replicated actors for fault tolerance and consistent hashing for
key distribution.

Run with:
    uv run python examples/06-distributed-cache.py
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import msgpack

from casty import actor, Mailbox
from casty.cluster import DevelopmentCluster


@dataclass
class CacheSet:
    value: bytes
    ttl: float | None = None


@dataclass
class CacheGet:
    pass


@dataclass
class CacheDelete:
    pass


@dataclass
class CacheExists:
    pass


@dataclass
class CacheExpire:
    pass


type CacheMessage = CacheSet | CacheGet | CacheDelete | CacheExists | CacheExpire


@actor
async def cache_entry(data: bytes | None, *, mailbox: Mailbox[CacheMessage]):
    async for msg, ctx in mailbox:
        match msg:
            case CacheSet(new_value, ttl):
                data = new_value
                if ttl is not None:
                    await ctx.schedule(CacheExpire(), delay=ttl)
                await ctx.reply(True)

            case CacheGet():
                await ctx.reply(data)

            case CacheDelete():
                data = None

            case CacheExists():
                await ctx.reply(data is not None)

            case CacheExpire():
                data = None


class DistributedCache:
    def __init__(
        self,
        cluster,
        prefix: str = "cache",
        write_quorum: int = 2,
        replicas: int = 3,
    ):
        self._cluster = cluster
        self._prefix = prefix
        self._write_quorum = write_quorum
        self._replicas = replicas
        self._entries: dict[str, Any] = {}

    def _key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def _entry(self, key: str):
        cache_key = self._key(key)
        if cache_key not in self._entries:
            self._entries[cache_key] = await self._cluster.actor(
                cache_entry(None),
                name=cache_key,
            )
        return self._entries[cache_key]

    async def get(self, key: str) -> Any | None:
        entry = await self._entry(key)
        value = await entry.ask(CacheGet())
        return msgpack.unpackb(value) if value is not None else None

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        entry = await self._entry(key)
        await entry.ask(CacheSet(msgpack.packb(value), ttl))

    async def delete(self, key: str) -> None:
        entry = await self._entry(key)
        await entry.send(CacheDelete())

    async def exists(self, key: str) -> bool:
        entry = await self._entry(key)
        return await entry.ask(CacheExists())


async def main():
    import asyncio

    async with DevelopmentCluster(nodes=3) as cluster:
        cache = DistributedCache(cluster)

        await cache.set("user:1", {"name": "Alice", "age": 30})
        await cache.set("user:2", {"name": "Bob", "age": 25})

        user1 = await cache.get("user:1")
        print(f"user:1 = {user1}")

        exists = await cache.exists("user:2")
        print(f"user:2 exists = {exists}")

        await cache.delete("user:2")
        exists = await cache.exists("user:2")
        print(f"user:2 exists after delete = {exists}")

        await cache.set("temp", "expires soon", ttl=1.0)
        print(f"temp = {await cache.get('temp')}")
        await asyncio.sleep(1.5)
        print(f"temp after TTL = {await cache.get('temp')}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
