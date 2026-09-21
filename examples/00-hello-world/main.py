"""One actor type, one key, one question. Run with `uv run main.py`.

An actor type is a function over a `Context[State, Message]`. Each key of the type is an entity with its own state and
its own mailbox; `greeter/ana` and `greeter/bia` never share anything.
"""

import asyncio
from dataclasses import dataclass

from casty import ActorSystem, Context, Ref, actor


@dataclass(frozen=True)
class Greet:
    reply_to: Ref[str]
    name: str


@actor(initial=0)
async def greeter(ctx: Context[int, Greet]) -> None:
    """Counts the greetings of its key. `initial=0` lets a key exist from its first message."""
    async for msg in ctx.inbox:
        await ctx.state.set(ctx.state.value + 1)
        msg.reply_to.tell(f"hello, {msg.name}! greeting #{ctx.state.value} from {ctx.key}")


async def main() -> None:
    async with ActorSystem() as system:
        ana = system.ref(greeter, "ana")
        # `ask` builds the message: the first argument of `Greet` is the ref the answer comes back through.
        print(await ana.ask(Greet, "world"))
        print(await ana.ask(Greet, "again"))
        print(await system.ref(greeter, "bia").ask(Greet, "world"))


asyncio.run(main())
