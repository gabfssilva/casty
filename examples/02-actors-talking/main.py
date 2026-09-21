"""Actors that talk to each other: a transfer between two accounts, coordinated by a third actor.

Run with `uv run main.py`. A `Ref` is a value: it goes inside messages, and the actor that receives it answers through
it without knowing who is on the other side. Inside a body, `ctx.system` reaches any other entity.
"""

import asyncio
from dataclasses import dataclass
from typing import assert_never

from casty import ActorSystem, Context, Ref, actor


@dataclass(frozen=True)
class Deposit:
    reply_to: Ref[int]
    amount: int


@dataclass(frozen=True)
class Withdraw:
    reply_to: Ref[bool]
    amount: int


@dataclass(frozen=True)
class Balance:
    reply_to: Ref[int]


type AccountMsg = Deposit | Withdraw | Balance


@actor(initial=0)
async def account(ctx: Context[int, AccountMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Deposit(reply_to, amount):
                await ctx.state.set(ctx.state.value + amount)
                reply_to.tell(ctx.state.value)
            case Withdraw(reply_to, amount) if amount <= ctx.state.value:
                await ctx.state.set(ctx.state.value - amount)
                reply_to.tell(True)
            case Withdraw(reply_to, _):
                reply_to.tell(False)
            case Balance(reply_to):
                reply_to.tell(ctx.state.value)
            case _:
                assert_never(msg)


@dataclass(frozen=True)
class Transfer:
    reply_to: Ref[str]
    source: str
    target: str
    amount: int


@actor(initial=0)
async def teller(ctx: Context[int, Transfer]) -> None:
    """Moves money between accounts and counts the transfers it completed.

    One message at a time per key, so two transfers through the same teller never interleave. The accounts are asked
    like any caller would ask them: there is no other way into an entity than its mailbox.
    """
    async for msg in ctx.inbox:
        source = ctx.system.ref(account, msg.source)
        target = ctx.system.ref(account, msg.target)
        if await source.ask(Withdraw, msg.amount):
            await target.ask(Deposit, msg.amount)
            await ctx.state.set(ctx.state.value + 1)
            msg.reply_to.tell(f"moved {msg.amount} from {msg.source} to {msg.target} (transfer #{ctx.state.value})")
        else:
            msg.reply_to.tell(f"{msg.source} cannot cover {msg.amount}")


async def main() -> None:
    async with ActorSystem() as system:
        ana, bia = system.ref(account, "ana"), system.ref(account, "bia")
        await ana.ask(Deposit, 100)
        desk = system.ref(teller, "desk-1")
        print(await desk.ask(Transfer, "ana", "bia", 70))
        print(await desk.ask(Transfer, "ana", "bia", 70))
        print(f"ana={await ana.ask(Balance)} bia={await bia.ask(Balance)}")


asyncio.run(main())
