"""The next version of the application: `ledger` with a field added, and a type only this version hosts.

A rolling deploy is what happens when nodes running these definitions replace nodes running `tests.app`, one at a
time. The names on the wire are the same, because a value is tagged by the qualified name of its class, and that is
what lets the two versions read each other's state: a field the other side does not know is ignored, and one it does
not send takes its default.
"""

from dataclasses import dataclass
from typing import assert_never

from casty import Context, NodeId, Ref, actor


@dataclass(frozen=True)
class Ledger:
    entries: tuple[int, ...] = ()
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Append:
    reply_to: Ref[bool]
    entry: int
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Listing:
    entries: tuple[int, ...]
    node: NodeId
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Entries:
    reply_to: Ref[Listing]


type LedgerMsg = Append | Entries


async def _ledger(ctx: Context[Ledger, LedgerMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Append(reply_to, entry, tags):
                await ctx.state.set(Ledger((*ctx.state.value.entries, entry), (*ctx.state.value.tags, *tags)))
                reply_to.tell(True)
            case Entries(reply_to):
                reply_to.tell(Listing(ctx.state.value.entries, ctx.system.node, ctx.state.value.tags))
            case _:
                assert_never(msg)


# A type is named after where its body lives, and the next version of a body lives where the last one did. One process
# cannot hold two modules under one path, so the body says here what a deploy would make true by itself.
_ledger.__module__, _ledger.__qualname__ = "tests.app", "ledger"
ledger = actor(initial=Ledger())(_ledger)


@dataclass(frozen=True)
class Audit:
    checked: int = 0


@dataclass(frozen=True)
class Check:
    reply_to: Ref[NodeId]


@actor(initial=Audit())
async def audit(ctx: Context[Audit, Check]) -> None:
    """A type only this version brings in."""
    async for msg in ctx.inbox:
        await ctx.state.set(Audit(ctx.state.value.checked + 1))
        msg.reply_to.tell(ctx.system.node)
