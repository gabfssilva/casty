"""casty.Counter: a striped distributed counter (spec 06 §3.1). A single logical
value split into `stripes` shard actors to avoid a write hotspot: `add` lands on
a rotating stripe, `get` sums the fan-out."""

from __future__ import annotations

import typing

from casty.actors.proxy import Caller
from casty.actors.registry import ActorInfo, Consistency, actor
from casty.collections import _sharded

_PREFIX = "casty.CounterShard"


def _shard_class(
    wire: str, replicas: int, write: Consistency | int, read: Consistency | int
) -> None:
    @actor(name=wire, replicas=replicas, write=write, read=read)
    class CounterShard:
        value: int = 0

        async def add(self, delta: int) -> None:
            self.value += delta

        async def get(self) -> int:
            return self.value

        async def reset(self) -> None:
            self.value = 0


_sharded.register(_PREFIX, _shard_class)


def shard_info(replicas: int, write: Consistency | int, read: Consistency | int) -> ActorInfo:
    return _sharded.materialize(_PREFIX, replicas, write, read)


class Counter(_sharded.ShardRouter):
    """Striped distributed counter, from `ActorSystem.counter`. `add` rotates
    over stripes to spread writes; `get` sums every stripe, so it is not a
    point-in-time snapshot under concurrent writers."""

    def __init__(self, caller: Caller, name: str, info: ActorInfo, stripes: int) -> None:
        super().__init__(caller, name, info, stripes)
        self._next = 0

    async def add(self, delta: int = 1) -> None:
        """Add `delta` (negative to subtract) to one stripe."""
        stripe = self._next % self._shards
        self._next += 1
        await self._call(stripe, "add", [delta])

    async def get(self) -> int:
        """The value, summed across all stripes."""
        counts = await self._fanout("get")
        return sum(typing.cast(int, count) for count in counts)

    async def reset(self) -> None:
        """Zero every stripe."""
        await self._fanout("reset")
