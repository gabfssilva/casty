"""casty.MultiMap: a distributed key -> set-of-values map (spec 06 §2.3). All
values of a key are co-located (shard by key), so `get(key)` is one call. STUB —
bodies to be implemented. Do not touch the prefix, factory registration,
`shard_info` or the `MultiMap` class name."""

from __future__ import annotations

import dataclasses
import typing

from casty.actors.registry import ActorInfo, Consistency, actor
from casty.collections import _sharded
from casty.serde import codec

_PREFIX = "casty.MultiMapShard"


def _shard_class(
    wire: str, replicas: int, write: Consistency | int, read: Consistency | int
) -> None:
    @actor(name=wire, replicas=replicas, write=write, read=read)
    class MultiMapShard:
        entries: dict[bytes, set[bytes]] = dataclasses.field(default_factory=dict)

        async def put(self, key: bytes, value: bytes) -> bool:
            values = self.entries.setdefault(key, set())
            if value in values:
                return False
            values.add(value)
            return True

        async def remove(self, key: bytes, value: bytes) -> bool:
            values = self.entries.get(key)
            if values is None or value not in values:
                return False
            values.discard(value)
            if not values:
                del self.entries[key]
            return True

        async def get(self, key: bytes) -> list[bytes]:
            return list(self.entries.get(key, ()))

        async def remove_key(self, key: bytes) -> int:
            values = self.entries.pop(key, None)
            return len(values) if values is not None else 0

        async def contains(self, key: bytes, value: bytes) -> bool:
            return value in self.entries.get(key, ())

        async def size(self) -> int:
            return sum(len(values) for values in self.entries.values())

        async def clear(self) -> None:
            self.entries.clear()


_sharded.register(_PREFIX, _shard_class)


def shard_info(replicas: int, write: Consistency | int, read: Consistency | int) -> ActorInfo:
    return _sharded.materialize(_PREFIX, replicas, write, read)


class MultiMap[K, V](_sharded.ShardRouter):
    """Typed distributed multimap: each key maps to a set of values."""

    async def put(self, key: K, value: V) -> bool:
        encoded = codec.encode_raw(key)
        return typing.cast(
            bool,
            await self._call(self._shard_of(encoded), "put", [encoded, codec.encode_raw(value)]),
        )

    async def remove(self, key: K, value: V) -> bool:
        encoded = codec.encode_raw(key)
        return typing.cast(
            bool,
            await self._call(
                self._shard_of(encoded), "remove", [encoded, codec.encode_raw(value)]
            ),
        )

    async def get(self, key: K) -> list[V]:
        encoded = codec.encode_raw(key)
        raw = await self._call(self._shard_of(encoded), "get", [encoded])
        return [
            typing.cast(V, codec.decode_any(value))
            for value in typing.cast(list[bytes], raw)
        ]

    async def remove_key(self, key: K) -> int:
        encoded = codec.encode_raw(key)
        return typing.cast(
            int, await self._call(self._shard_of(encoded), "remove_key", [encoded])
        )

    async def contains(self, key: K, value: V) -> bool:
        encoded = codec.encode_raw(key)
        return typing.cast(
            bool,
            await self._call(
                self._shard_of(encoded), "contains", [encoded, codec.encode_raw(value)]
            ),
        )

    async def size(self) -> int:
        counts = await self._fanout("size")
        return sum(typing.cast(int, count) for count in counts)

    async def clear(self) -> None:
        await self._fanout("clear")
