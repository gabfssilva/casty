from typing import Any

import msgpack

from .entry import cache_entry, Get, Set, Delete, Exists


class DistributedCache:
    def __init__(self, cluster, prefix: str = "cache"):
        self._cluster = cluster
        self._prefix = prefix

    def _key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def _entry(self, key: str):
        return await self._cluster.actor(
            cache_entry(),
            name=self._key(key),
        )

    async def get(self, key: str) -> Any | None:
        entry = await self._entry(key)
        value = await entry.ask(Get())
        return msgpack.unpackb(value) if value is not None else None

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        entry = await self._entry(key)
        await entry.send(Set(msgpack.packb(value), ttl))

    async def delete(self, key: str) -> None:
        entry = await self._entry(key)
        await entry.send(Delete())

    async def exists(self, key: str) -> bool:
        entry = await self._entry(key)
        return await entry.ask(Exists())
