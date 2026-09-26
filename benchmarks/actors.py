from dataclasses import dataclass
from typing import assert_never

from casty import Askable, Context, actor


@dataclass(frozen=True)
class Read(Askable[int]):
    payload: bytes


@dataclass(frozen=True)
class Increment(Askable[int]):
    payload: bytes


@actor(initial=0, replicas=3, write="majority")
async def counter(ctx: Context[int, Read | Increment]) -> None:
    async for message in ctx.inbox:
        match message:
            case Read(_, reply_to=reply_to):
                reply_to.tell(ctx.state.value)
            case Increment(_, reply_to=reply_to):
                await ctx.state.set(ctx.state.value + 1)
                reply_to.tell(ctx.state.value)
            case _:
                assert_never(message)
