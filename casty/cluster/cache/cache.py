from typing import Any

import msgpack

from .entry import cache_entry, Get, Set, Delete, Exists


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
                write_quorum=self._write_quorum,
                replicas=self._replicas,
            )
        return self._entries[cache_key]

    async def get(self, key: str) -> Any | None:
        entry = await self._entry(key)
        value = await entry.ask(Get())
        return msgpack.unpackb(value) if value is not None else None

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        entry = await self._entry(key)
        await entry.ask(Set(msgpack.packb(value), ttl))

    async def delete(self, key: str) -> None:
        entry = await self._entry(key)
        await entry.send(Delete())

    async def exists(self, key: str) -> bool:
        entry = await self._entry(key)
        return await entry.ask(Exists())
