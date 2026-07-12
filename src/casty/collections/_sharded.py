"""Shared skeleton for distributed collections (spec 06). A collection is sugar
over shard actors: a shard class materialized per (replicas, write, read) triple
with a self-describing wire name, plus a client-side router. Placement,
replication, quorum and fencing are inherited unchanged from specs 03/05."""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Callable

from casty.actors.context import context_or_none
from casty.actors.proxy import Caller
from casty.actors.registry import ActorInfo, Consistency, info_by_name

_WIRE_RE = re.compile(r"^(?P<prefix>[\w.]+)\[r(?P<r>\d+),w(?P<w>[a-z0-9]+),rd(?P<rd>[a-z0-9]+)\]$")


def _token(level: Consistency | int) -> str:
    match level:
        case Consistency.ONE:
            return "one"
        case Consistency.MAJORITY:
            return "majority"
        case Consistency.ALL:
            return "all"
        case int() as n:
            return str(n)


def _level(token: str) -> Consistency | int:
    match token:
        case "one":
            return Consistency.ONE
        case "majority":
            return Consistency.MAJORITY
        case "all":
            return Consistency.ALL
        case _:
            return int(token)


def wire_name(prefix: str, replicas: int, write: Consistency | int, read: Consistency | int) -> str:
    return f"{prefix}[r{replicas},w{_token(write)},rd{_token(read)}]"


# A factory materializes the @casty.actor shard class for one triple, under the
# given wire name. It does not return; the class registers itself on definition.
ShardFactory = Callable[[str, int, Consistency | int, Consistency | int], None]

_factories: dict[str, ShardFactory] = {}


def register(prefix: str, factory: ShardFactory) -> None:
    """Called at import time by each collection module to register its shard
    class factory under a wire-name prefix."""
    _factories[prefix] = factory


def materialize(
    prefix: str, replicas: int, write: Consistency | int, read: Consistency | int
) -> ActorInfo:
    wire = wire_name(prefix, replicas, write, read)
    info = info_by_name(wire)
    if info is not None:
        return info
    _factories[prefix](wire, replicas, write, read)
    materialized = info_by_name(wire)
    assert materialized is not None
    return materialized


def ensure(wire: str) -> ActorInfo | None:
    """Materialize any registered collection's shard class from its wire name.
    Lets a node serve collections it never called the factory for."""
    match = _WIRE_RE.match(wire)
    if match is None:
        return None
    prefix = match.group("prefix")
    if prefix not in _factories:
        return None
    return materialize(
        prefix, int(match.group("r")), _level(match.group("w")), _level(match.group("rd"))
    )


class ShardRouter:
    """Client side of a collection. Routes single-item ops to `hash(item) %
    shards` and fans out aggregates to every shard. Single-owner collections
    use `shards=1` and address shard 0."""

    def __init__(self, caller: Caller, name: str, info: ActorInfo, shards: int) -> None:
        self._caller = caller
        self._name = name
        self._info = info
        self._shards = shards

    def _shard_of(self, encoded_item: bytes) -> int:
        digest = hashlib.blake2b(encoded_item, digest_size=8).digest()
        return int.from_bytes(digest, "big") % self._shards

    async def _call(self, shard: int, method_name: str, args: list[object]) -> object:
        method = self._info.methods[method_name]
        ctx = context_or_none()
        chain = ctx.chain if ctx is not None else []
        return await self._caller._call_actor(
            self._info, f"{self._name}:{shard}", method, args, chain
        )

    async def _fanout(self, method_name: str, args: list[object] | None = None) -> list[object]:
        return list(
            await asyncio.gather(
                *(self._call(i, method_name, args or []) for i in range(self._shards))
            )
        )
