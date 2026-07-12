"""casty.Queue: a distributed FIFO queue (spec 06 §4.2). One owner actor keeps
order; it does not scale within a single queue (all traffic hits one node) —
scale by partitioning into many named queues. STUB — bodies to be implemented.
Do not touch the prefix, factory registration, `shard_info` or the `Queue` class
name. Single-owner: the facade always addresses shard 0."""

from __future__ import annotations

import dataclasses
import typing

from casty.actors.registry import ActorInfo, Consistency, actor
from casty.collections import _sharded
from casty.serde import codec

_PREFIX = "casty.QueueShard"


def _shard_class(
    wire: str, replicas: int, write: Consistency | int, read: Consistency | int
) -> None:
    @actor(name=wire, replicas=replicas, write=write, read=read)
    class QueueShard:
        items: list[bytes] = dataclasses.field(default_factory=list)

        async def offer(self, item: bytes) -> None:
            self.items.append(item)

        async def poll(self) -> bytes | None:
            if not self.items:
                return None
            return self.items.pop(0)

        async def peek(self) -> bytes | None:
            if not self.items:
                return None
            return self.items[0]

        async def size(self) -> int:
            return len(self.items)

        async def drain(self, max_items: int) -> list[bytes]:
            drained = self.items[:max_items]
            del self.items[:max_items]
            return drained

        async def clear(self) -> None:
            self.items.clear()


_sharded.register(_PREFIX, _shard_class)


def shard_info(replicas: int, write: Consistency | int, read: Consistency | int) -> ActorInfo:
    return _sharded.materialize(_PREFIX, replicas, write, read)


class Queue[T](_sharded.ShardRouter):
    """Typed distributed FIFO queue. Single owner (shard 0)."""

    async def offer(self, item: T) -> None:
        await self._call(0, "offer", [codec.encode_raw(item)])

    async def poll(self) -> T | None:
        raw = await self._call(0, "poll", [])
        if raw is None:
            return None
        return typing.cast(T, codec.decode_any(typing.cast(bytes, raw)))

    async def peek(self) -> T | None:
        raw = await self._call(0, "peek", [])
        if raw is None:
            return None
        return typing.cast(T, codec.decode_any(typing.cast(bytes, raw)))

    async def size(self) -> int:
        return typing.cast(int, await self._call(0, "size", []))

    async def drain(self, max_items: int) -> list[T]:
        raws = await self._call(0, "drain", [max_items])
        return [
            typing.cast(T, codec.decode_any(raw))
            for raw in typing.cast(list[bytes], raws)
        ]

    async def clear(self) -> None:
        await self._call(0, "clear", [])
