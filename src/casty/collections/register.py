"""casty.Register: a distributed atomic reference (spec 06 §4.1). A single named
value under one owner actor; compare-and-set is correct for free because the
owner serializes writes (single-writer). Single-owner: the facade always
addresses shard 0."""

from __future__ import annotations

import typing

from casty.actors.registry import ActorInfo, Consistency, actor
from casty.collections import _sharded
from casty.serde import codec

_PREFIX = "casty.RegisterShard"


def _shard_class(
    wire: str, replicas: int, write: Consistency | int, read: Consistency | int
) -> None:
    @actor(name=wire, replicas=replicas, write=write, read=read)
    class RegisterShard:
        value: bytes | None = None

        async def get(self) -> bytes | None:
            return self.value

        async def set(self, value: bytes) -> None:
            self.value = value

        async def compare_and_set(self, expected: bytes | None, new: bytes) -> bool:
            if self.value != expected:
                return False
            self.value = new
            return True

        async def get_and_set(self, value: bytes) -> bytes | None:
            old = self.value
            self.value = value
            return old


_sharded.register(_PREFIX, _shard_class)


def shard_info(replicas: int, write: Consistency | int, read: Consistency | int) -> ActorInfo:
    return _sharded.materialize(_PREFIX, replicas, write, read)


class Register[T](_sharded.ShardRouter):
    """Typed distributed atomic reference, from `ActorSystem.register`. One
    owner actor serializes writes, so the read-modify-write ops are atomic."""

    async def get(self) -> T | None:
        """The current value, or None if never set."""
        raw = await self._call(0, "get", [])
        if raw is None:
            return None
        return typing.cast(T, codec.decode_any(typing.cast(bytes, raw)))

    async def set(self, value: T) -> None:
        """Replace the value unconditionally."""
        await self._call(0, "set", [codec.encode_raw(value)])

    async def compare_and_set(self, expected: T | None, new: T) -> bool:
        """Set to `new` only if the current value equals `expected` (None
        matches never-set). True if the swap happened. Comparison is by
        encoded bytes."""
        encoded_expected = None if expected is None else codec.encode_raw(expected)
        return typing.cast(
            bool,
            await self._call(0, "compare_and_set", [encoded_expected, codec.encode_raw(new)]),
        )

    async def get_and_set(self, value: T) -> T | None:
        """Replace the value, returning the previous one (None if never set)."""
        raw = await self._call(0, "get_and_set", [codec.encode_raw(value)])
        if raw is None:
            return None
        return typing.cast(T, codec.decode_any(typing.cast(bytes, raw)))
