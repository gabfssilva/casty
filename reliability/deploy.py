"""The next version of `reliability.actors`: `ledger` with a field added.

A rolling deploy is what happens when nodes running this definition replace nodes running `reliability.actors`, one at
a time. The names on the wire are the same, because a value is tagged by the qualified name of its class, and that is
what lets the two versions read each other's state: a field the other side does not know is ignored, and one it does
not send takes its default.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import assert_never

from casty import Askable, Context, NodeId, actor


@dataclass(frozen=True)
class Ledger:
    entries: tuple[int, ...] = ()
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Append(Askable[bool]):
    entry: int
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Listing:
    entries: tuple[int, ...]
    node: NodeId
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Entries(Askable[Listing]):
    pass


type LedgerMsg = Append | Entries


async def _ledger(ctx: Context[Ledger, LedgerMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Append(entry, tags, reply_to=reply_to):
                await ctx.state.set(Ledger((*ctx.state.value.entries, entry), (*ctx.state.value.tags, *tags)))
                reply_to.tell(True)
            case Entries(reply_to=reply_to):
                reply_to.tell(Listing(ctx.state.value.entries, ctx.system.node, ctx.state.value.tags))
            case _:
                assert_never(msg)


# A type is named after where its body lives, and the next version of a body lives where the last one did. One image
# holds both versions, so the body says here what a deploy would make true by itself.
_ledger.__module__, _ledger.__qualname__ = "reliability.actors", "ledger"
ledger = actor(initial=Ledger())(_ledger)
