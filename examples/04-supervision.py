"""Supervision: Fault tolerance with automatic restarts.

Demonstrates:
- @supervised decorator for fault tolerance
- Restart strategy with max_retries
- OneForOne scope (only restart the failed actor)
- Actor automatically recovers from crashes

Run with:
    uv run python examples/04-supervision.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, ActorSystem, Mailbox
from casty.state import State
from casty.supervision import supervised, Restart, OneForOne


@dataclass
class DoWork:
    value: int


@dataclass
class Crash:
    pass


@dataclass
class GetStats:
    pass


type WorkerMsg = DoWork | Crash | GetStats


@supervised(strategy=Restart(max_retries=3), scope=OneForOne())
@actor
async def resilient_worker(state: State[int], *, mailbox: Mailbox[WorkerMsg]):
    print("Worker started")

    async for msg, ctx in mailbox:
        match msg:
            case DoWork(value):
                state.value += 1
                result = value * 2
                print(f"Processed {value} -> {result} (total: {state.value})")
                await ctx.reply(result)

            case Crash():
                print("Crashing intentionally!")
                raise RuntimeError("Intentional crash")

            case GetStats():
                await ctx.reply({"processed": state.value})


async def main():
    async with ActorSystem() as system:
        worker = await system.actor(resilient_worker(State(0)), name="worker")

        # Normal work
        print("--- Normal operations ---")
        for i in range(3):
            result = await worker.ask(DoWork(i))
            print(f"  Result: {result}")

        # Crash and recover
        print("\n--- Crash and recovery ---")
        await worker.send(Crash())
        await asyncio.sleep(0.1)

        # Worker should be back up
        print("\n--- After recovery ---")
        result = await worker.ask(DoWork(100))
        print(f"  Result: {result}")

        stats = await worker.ask(GetStats())
        print(f"  Stats: {stats}")


if __name__ == "__main__":
    asyncio.run(main())
