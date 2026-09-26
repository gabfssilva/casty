"""The actor type, imported by the nodes that host it and by the client that drives it."""

from dataclasses import dataclass
from typing import assert_never

from casty import Askable, Context, NodeId, actor


@dataclass(frozen=True)
class Hit(Askable[int]):
    pass


@dataclass(frozen=True)
class Seen:
    hits: int
    node: NodeId


@dataclass(frozen=True)
class Locate(Askable[Seen]):
    pass


type PageMsg = Hit | Locate


@actor(initial=0)
async def page(ctx: Context[int, PageMsg]) -> None:
    """Hit counter of one page."""
    async for msg in ctx.inbox:
        match msg:
            case Hit(reply_to=reply_to):
                await ctx.state.set(ctx.state.value + 1)
                reply_to.tell(ctx.state.value)
            case Locate(reply_to=reply_to):
                reply_to.tell(Seen(ctx.state.value, ctx.system.node))
            case _:
                assert_never(msg)
