"""The actor types the runs put under traffic: the ledgers of the chaos run, and the counters of the performance run.

`reliability.deploy` is the next version of `ledger`, which a rolling deploy of the chaos run brings in.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import assert_never

from casty import Askable, Context, NodeId, actor


@dataclass(frozen=True)
class Ledger:
    entries: tuple[int, ...] = ()


@dataclass(frozen=True)
class Append(Askable[bool]):
    entry: int


@dataclass(frozen=True)
class Listing:
    entries: tuple[int, ...]
    node: NodeId


@dataclass(frozen=True)
class Entries(Askable[Listing]):
    pass


type LedgerMsg = Append | Entries


async def _entries(ctx: Context[Ledger, LedgerMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Append(entry, reply_to=reply_to):
                await ctx.state.set(Ledger((*ctx.state.value.entries, entry)))
                reply_to.tell(True)
            case Entries(reply_to=reply_to):
                reply_to.tell(Listing(ctx.state.value.entries, ctx.system.node))
            case _:
                assert_never(msg)


# A type is named after its body, so two types take two bodies, even when they do the same.
@actor(initial=Ledger())
async def ledger(ctx: Context[Ledger, LedgerMsg]) -> None:
    await _entries(ctx)


@actor(initial=Ledger(), durable="write")
async def durable_ledger(ctx: Context[Ledger, LedgerMsg]) -> None:
    """A ledger its store keeps too: an entry it confirmed outlives every replica of its key, and every node."""
    await _entries(ctx)


@dataclass(frozen=True)
class Stamp(Askable[NodeId]):
    """Append `entry`, and answer with the node that applied it."""

    entry: int


type StampMsg = Stamp | Entries


@actor(initial=Ledger(), pinned=True)
async def stamped(ctx: Context[Ledger, StampMsg]) -> None:
    """A ledger whose keys each run on the node their ref names, one copy that goes with that node.

    Each append answers with the node that applied it, so a check knows which incarnation must still hold it.
    """
    async for msg in ctx.inbox:
        match msg:
            case Stamp(entry, reply_to=reply_to):
                await ctx.state.set(Ledger((*ctx.state.value.entries, entry)))
                reply_to.tell(ctx.system.node)
            case Entries(reply_to=reply_to):
                reply_to.tell(Listing(ctx.state.value.entries, ctx.system.node))
            case _:
                assert_never(msg)


@dataclass(frozen=True)
class Read(Askable[int]):
    payload: bytes


@dataclass(frozen=True)
class Increment(Askable[int]):
    payload: bytes


type CounterMsg = Read | Increment


async def _count(ctx: Context[int, CounterMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Read(reply_to=reply_to):
                reply_to.tell(ctx.state.value)
            case Increment(reply_to=reply_to):
                await ctx.state.set(ctx.state.value + 1)
                reply_to.tell(ctx.state.value)
            case _:
                assert_never(msg)


@actor(initial=0, replicas=3, write="majority")
async def counter(ctx: Context[int, CounterMsg]) -> None:
    await _count(ctx)


@actor(initial=0, replicas=3, write="majority", durable="write")
async def durable_counter(ctx: Context[int, CounterMsg]) -> None:
    """A counter its store keeps too: every increment is written to the database before it is confirmed."""
    await _count(ctx)
