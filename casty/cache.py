from __future__ import annotations

from typing import Any

import msgpack

from .core import actor, Mailbox
from .messages import CacheSet, CacheGet, CacheDelete, CacheExists, CacheExpire, CacheMessage


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
                write_quorum=self._write_quorum,
                replicas=self._replicas,
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
