"""
Example 09: Detached async operations

Demonstrates how ctx.detach allows an actor to remain responsive
while a long-running async operation runs in the background.
"""

import asyncio
from dataclasses import dataclass

from casty import Actor, ActorSystem, Context


@dataclass
class SlowResult:
    """Result from the slow operation."""
    value: str


async def slow_operation() -> str:
    print("[slow_operation] Starting (5s)...")
    await asyncio.sleep(5)
    print("[slow_operation] Done!")
    return "result"


@dataclass
class Start:
    block: bool = False


@dataclass
class Ping:
    seq: int


class MyActor(Actor[Start | Ping | SlowResult]):
    async def receive(self, msg: Start | Ping | SlowResult, ctx: Context):
        match msg:
            case Start(block=True):
                # Blocking: actor can't process other messages until done
                result = await slow_operation()
                print(f"[actor] Got result: {result}")

            case Start(block=False):
                # Non-blocking: actor remains responsive
                # Result is transformed into SlowResult message
                ctx.detach(slow_operation()).then(SlowResult)

            case Ping(seq):
                print(f"[actor] Ping #{seq}")

            case SlowResult(value):
                print(f"[actor] Got result: {value}")


async def run(ref, block: bool):
    await ref.send(Start(block=block))

    for i in range(10):
        await asyncio.sleep(0.49)
        await ref.send(Ping(seq=i + 1))

    await asyncio.sleep(0.5)


async def main():
    system = ActorSystem()

    try:
        print("=== BLOCKING ===\n")
        ref = await system.spawn(MyActor)
        await run(ref, block=True)

        print("\n=== DETACHED ===\n")
        ref = await system.spawn(MyActor)
        await run(ref, block=False)
    finally:
        await system.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
