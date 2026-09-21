"""What a caller sees when things go wrong, and what the entity keeps. Run with `uv run main.py`.

- a body that raises fails the `ask` it was processing with `ActorFailed`, and restarts from the last saved state;
- a type without a default `initial` takes it from the ref, and only the first ref of a key decides its state;
- a bounded mailbox refuses what does not fit with `MailboxFull` instead of growing.
"""

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import assert_never

from casty import ActorFailed, ActorSystem, Backoff, Context, MailboxFull, Ref, actor


@dataclass(frozen=True)
class Divide:
    reply_to: Ref[float]
    by: int


@dataclass(frozen=True)
class Value:
    reply_to: Ref[float]


type CalculatorMsg = Divide | Value


@actor(initial=100.0)
async def calculator(ctx: Context[float, CalculatorMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Divide(reply_to, by):
                await ctx.state.set(ctx.state.value / by)
                reply_to.tell(ctx.state.value)
            case Value(reply_to):
                reply_to.tell(ctx.state.value)
            case _:
                assert_never(msg)


@dataclass(frozen=True)
class Read:
    reply_to: Ref[str]


@actor
async def document(ctx: Context[str, Read]) -> None:
    async for msg in ctx.inbox:
        msg.reply_to.tell(ctx.state.value)


@dataclass(frozen=True)
class Work:
    reply_to: Ref[int]
    job: int


@actor(initial=0, mailbox=2)
async def worker(ctx: Context[int, Work]) -> None:
    async for msg in ctx.inbox:
        await asyncio.sleep(0.2)
        msg.reply_to.tell(msg.job)


async def main() -> None:
    # The restart after a failure waits `first`, then twice that, up to `limit`. Short here so the example is quick.
    backoff = Backoff(first=timedelta(milliseconds=50), limit=timedelta(seconds=1))
    async with ActorSystem(backoff=backoff) as system:
        calc = system.ref(calculator, "calc")
        print("100 / 4 =", await calc.ask(Divide, 4))
        try:
            await calc.ask(Divide, 0)
        except ActorFailed as failed:
            print(f"failed: {failed.error} ({failed.message})")
        print("still there, with the last saved state:", await calc.ask(Value))

        print("content:", await system.ref(document, "readme", initial="# casty").ask(Read))
        print("still:", await system.ref(document, "readme", initial="something else").ask(Read))

        busy = system.ref(worker, "w")
        results = await asyncio.gather(*(busy.ask(Work, job) for job in range(6)), return_exceptions=True)
        done = [result for result in results if isinstance(result, int)]
        refused = [result for result in results if isinstance(result, MailboxFull)]
        print(f"jobs done: {done}, refused by the full mailbox: {len(refused)}")


asyncio.run(main())
