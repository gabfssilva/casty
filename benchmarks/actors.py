from dataclasses import dataclass
from typing import assert_never

from casty import Context, Ref, actor


@dataclass(frozen=True)
class Read:
    reply_to: Ref[int]
    payload: bytes


@dataclass(frozen=True)
class Increment:
    reply_to: Ref[int]
    payload: bytes


@actor(initial=0, replicas=3, write="majority")
async def counter(ctx: Context[int, Read | Increment]) -> None:
    async for message in ctx.inbox:
        match message:
            case Read(reply_to, _):
                reply_to.tell(ctx.state.value)
            case Increment(reply_to, _):
                await ctx.state.set(ctx.state.value + 1)
                reply_to.tell(ctx.state.value)
            case _:
                assert_never(message)
