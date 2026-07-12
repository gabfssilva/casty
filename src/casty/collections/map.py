"""casty.Map: a distributed map as sugar over shard actors (spec 06). Each
shard is an ordinary virtual actor — placement, replication, quorums and
fencing all come from specs 03/05 unchanged."""

from __future__ import annotations

import dataclasses
import typing

from casty.actors.registry import ActorInfo, Consistency, actor
from casty.collections import _sharded
from casty.serde import codec

_PREFIX = "casty.MapShard"


def _shard_class(
    wire: str, replicas: int, write: Consistency | int, read: Consistency | int
) -> None:
    @actor(name=wire, replicas=replicas, write=write, read=read)
    class MapShard:
        entries: dict[bytes, bytes] = dataclasses.field(default_factory=dict)

        async def put(self, key: bytes, value: bytes) -> None:
            self.entries[key] = value

        async def get(self, key: bytes) -> bytes | None:
            return self.entries.get(key)

        async def remove(self, key: bytes) -> bool:
            return self.entries.pop(key, None) is not None

        async def contains(self, key: bytes) -> bool:
            return key in self.entries

        async def size(self) -> int:
            return len(self.entries)

        async def clear(self) -> None:
            self.entries.clear()

        async def dump(self) -> dict[bytes, bytes]:
            return dict(self.entries)


_sharded.register(_PREFIX, _shard_class)


def shard_info(replicas: int, write: Consistency | int, read: Consistency | int) -> ActorInfo:
    return _sharded.materialize(_PREFIX, replicas, write, read)


class Map[K, V](_sharded.ShardRouter):
    """Typed distributed map, from `ActorSystem.map`. One shard-actor call per
    key operation; aggregates (`size`, `clear`, `items`) fan out to every
    shard."""

    async def put(self, key: K, value: V) -> None:
        """Store `value` under `key`, replacing any previous value."""
        encoded = codec.encode_raw(key)
        await self._call(self._shard_of(encoded), "put", [encoded, codec.encode_raw(value)])

    async def get(self, key: K) -> V | None:
        """The value under `key`, or None if absent."""
        encoded = codec.encode_raw(key)
        raw = await self._call(self._shard_of(encoded), "get", [encoded])
        if raw is None:
            return None
        return typing.cast(V, codec.decode_any(typing.cast(bytes, raw)))

    async def remove(self, key: K) -> bool:
        """Remove `key`. True if it was present."""
        encoded = codec.encode_raw(key)
        return typing.cast(bool, await self._call(self._shard_of(encoded), "remove", [encoded]))

    async def contains(self, key: K) -> bool:
        """Whether `key` is present."""
        encoded = codec.encode_raw(key)
        return typing.cast(bool, await self._call(self._shard_of(encoded), "contains", [encoded]))

    async def size(self) -> int:
        """Total entries, summed across all shards."""
        counts = await self._fanout("size")
        return sum(typing.cast(int, count) for count in counts)

    async def clear(self) -> None:
        """Remove every entry from every shard."""
        await self._fanout("clear")

    async def items(self) -> list[tuple[K, V]]:
        """Every entry, pulled from every shard. Unordered; O(map size)."""
        dumps = await self._fanout("dump")
        result: list[tuple[K, V]] = []
        for dump in dumps:
            for raw_key, raw_value in typing.cast(dict[bytes, bytes], dump).items():
                result.append(
                    (
                        typing.cast(K, codec.decode_any(raw_key)),
                        typing.cast(V, codec.decode_any(raw_value)),
                    )
                )
        return result
