"""casty.Set: a distributed set as sugar over shard actors (spec 06 §2.2).
The prefix, factory registration, `shard_info` and the `Set` class name are
the contract that system.py / collections.__init__ depend on."""

from __future__ import annotations

import dataclasses
import typing

from casty.actors.registry import ActorInfo, Consistency, actor
from casty.collections import _sharded
from casty.serde import codec

_PREFIX = "casty.SetShard"


def _shard_class(
    wire: str, replicas: int, write: Consistency | int, read: Consistency | int
) -> None:
    @actor(name=wire, replicas=replicas, write=write, read=read)
    class SetShard:
        members: set[bytes] = dataclasses.field(default_factory=set)

        async def add(self, item: bytes) -> bool:
            if item in self.members:
                return False
            self.members.add(item)
            return True

        async def remove(self, item: bytes) -> bool:
            if item not in self.members:
                return False
            self.members.remove(item)
            return True

        async def contains(self, item: bytes) -> bool:
            return item in self.members

        async def size(self) -> int:
            return len(self.members)

        async def clear(self) -> None:
            self.members.clear()

        async def dump(self) -> set[bytes]:
            return set(self.members)


_sharded.register(_PREFIX, _shard_class)


def shard_info(replicas: int, write: Consistency | int, read: Consistency | int) -> ActorInfo:
    return _sharded.materialize(_PREFIX, replicas, write, read)


class Set[T](_sharded.ShardRouter):
    """Typed distributed set, from `ActorSystem.set`. Single-item ops route to
    one shard; aggregates and set algebra fan out to all shards."""

    async def add(self, item: T) -> bool:
        """Add `item`. True if it was not already present."""
        encoded = codec.encode_raw(item)
        return typing.cast(bool, await self._call(self._shard_of(encoded), "add", [encoded]))

    async def remove(self, item: T) -> bool:
        """Remove `item`. True if it was present."""
        encoded = codec.encode_raw(item)
        return typing.cast(bool, await self._call(self._shard_of(encoded), "remove", [encoded]))

    async def contains(self, item: T) -> bool:
        """Whether `item` is present."""
        encoded = codec.encode_raw(item)
        return typing.cast(bool, await self._call(self._shard_of(encoded), "contains", [encoded]))

    async def size(self) -> int:
        """Total items, summed across all shards."""
        counts = await self._fanout("size")
        return sum(typing.cast(int, count) for count in counts)

    async def clear(self) -> None:
        """Remove every item from every shard."""
        await self._fanout("clear")

    async def items(self) -> list[T]:
        """Every item, pulled from every shard. Unordered; O(set size)."""
        dumps = await self._fanout("dump")
        result: list[T] = []
        for dump in dumps:
            for raw_item in typing.cast(set[bytes], dump):
                result.append(typing.cast(T, codec.decode_any(raw_item)))
        return result

    async def union(self, other: Set[T]) -> set[T]:
        """Client-side union: pulls both sets whole."""
        return set(await self.items()) | set(await other.items())

    async def intersection(self, other: Set[T]) -> set[T]:
        """Client-side intersection: pulls both sets whole."""
        return set(await self.items()) & set(await other.items())

    async def difference(self, other: Set[T]) -> set[T]:
        """Client-side difference: pulls both sets whole."""
        return set(await self.items()) - set(await other.items())
