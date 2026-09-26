"""The actor types, imported by the nodes that host them and by the client that talks to them."""

from dataclasses import dataclass
from typing import assert_never

from casty import Askable, Context, NodeId, actor


@dataclass(frozen=True)
class Vote(Askable[int]):
    voter: str


@dataclass(frozen=True)
class Tally:
    votes: int
    node: NodeId


@dataclass(frozen=True)
class Count(Askable[Tally]):
    pass


type PollMsg = Vote | Count


# A type is named after where its body lives: `app:poll`. A node that never touched it imports it from there when the
# name arrives, which is why `node.py` does not mention it.
@actor(initial=frozenset[str]())
async def poll(ctx: Context[frozenset[str], PollMsg]) -> None:
    """One poll option: who voted for it. A set, so a voter asking twice counts once."""
    async for msg in ctx.inbox:
        match msg:
            case Vote(voter, reply_to=reply_to):
                await ctx.state.set(ctx.state.value | {voter})
                reply_to.tell(len(ctx.state.value))
            case Count(reply_to=reply_to):
                reply_to.tell(Tally(len(ctx.state.value), ctx.system.node))
            case _:
                assert_never(msg)
