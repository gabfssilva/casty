"""An order as a state machine: each state is an actor of its own, and `become` hands the key from one to the next.

Run with `uv run main.py`. What a pending order does with `Ship` is written once, in `pending`, next to the state
only a pending order has. The key stays `pending/order-1` for whoever holds a ref: what changes is who reads its
mailbox, and that is saved with the state, so it survives the node.
"""

import asyncio
from dataclasses import dataclass
from typing import assert_never

from casty import ActorSystem, Context, Ref, actor


@dataclass(frozen=True)
class Pending:
    items: tuple[str, ...]


@dataclass(frozen=True)
class Paid:
    items: tuple[str, ...]
    amount: int


@dataclass(frozen=True)
class Shipped:
    tracking: str


@dataclass(frozen=True)
class Pay:
    reply_to: Ref[str]
    amount: int


@dataclass(frozen=True)
class Ship:
    reply_to: Ref[str]
    tracking: str


type OrderMsg = Pay | Ship


@actor
async def shipped(ctx: Context[Shipped, OrderMsg]) -> None:
    async for msg in ctx.inbox:
        msg.reply_to.tell(f"refused: already shipped as {ctx.state.value.tracking}")


@actor
async def paid(ctx: Context[Paid, OrderMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Pay(reply_to, _):
                reply_to.tell("refused: already paid")
            case Ship(reply_to, tracking):
                await ctx.become(shipped, Shipped(tracking))
                reply_to.tell(f"shipped {len(ctx.state.value.items)} item(s) as {tracking}")
            case _:
                assert_never(msg)


@actor
async def pending(ctx: Context[Pending, OrderMsg]) -> None:
    """Where an order starts. No default `initial`: the ref that creates it says which items it has."""
    async for msg in ctx.inbox:
        match msg:
            case Pay(reply_to, amount):
                # The state is of the type of `paid`, and the checker holds the two together. The code after this
                # line still runs here; the next message is read by `paid`.
                await ctx.become(paid, Paid(ctx.state.value.items, amount))
                reply_to.tell(f"paid {amount} for {', '.join(ctx.state.value.items)}")
            case Ship(reply_to, _):
                reply_to.tell("refused: not paid yet")
            case _:
                assert_never(msg)


async def main() -> None:
    async with ActorSystem() as system:
        ref = system.ref(pending, "order-1", initial=Pending(("keyboard", "mouse")))
        print(await ref.ask(Ship, "BR123"))
        print(await ref.ask(Pay, 450))
        print(await ref.ask(Pay, 450))
        print(await ref.ask(Ship, "BR123"))
        print(await ref.ask(Ship, "BR999"))


asyncio.run(main())
