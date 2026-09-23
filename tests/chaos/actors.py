"""Actor types that only the chaos suite puts under traffic."""

from __future__ import annotations

from dataclasses import dataclass
from typing import assert_never

from casty import Context, NodeId, Ref, actor
from tests.app import Entries, Ledger, Listing


@dataclass(frozen=True)
class Stamp:
    """Append `entry`, and answer with the node that applied it."""

    reply_to: Ref[NodeId]
    entry: int


type StampMsg = Stamp | Entries


@actor(initial=Ledger(), pinned=True)
async def stamped(ctx: Context[Ledger, StampMsg]) -> None:
    """A ledger whose keys each run on the node their ref names, one copy that goes with that node.

    Each append answers with the node that applied it, so a check knows which incarnation must still hold it.
    """
    async for msg in ctx.inbox:
        match msg:
            case Stamp(reply_to, entry):
                await ctx.state.set(Ledger((*ctx.state.value.entries, entry)))
                reply_to.tell(ctx.system.node)
            case Entries(reply_to):
                reply_to.tell(Listing(ctx.state.value.entries, ctx.system.node))
            case _:
                assert_never(msg)
