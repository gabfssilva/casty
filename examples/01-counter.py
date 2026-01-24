"""Counter: The basics of Casty.

Demonstrates:
- @actor decorator and Mailbox
- Dataclass messages with pattern matching
- send() (fire-and-forget) vs ask() (request-response)
- AST-transformed state management

Run with:
    uv run python examples/01-counter.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, ActorSystem, Mailbox


@dataclass
class Increment:
    amount: int = 1


@dataclass
class Decrement:
    amount: int = 1


@dataclass
class Get:
    pass


type CounterMsg = Increment | Decrement | Get


@actor
async def counter(count: int, *, mailbox: Mailbox[CounterMsg]):
    async for msg, ctx in mailbox:
        match msg:
            case Increment(amount):
                count += amount
                print(f"Incremented by {amount} -> {count}")

            case Decrement(amount):
                count -= amount
                print(f"Decremented by {amount} -> {count}")

            case Get():
                await ctx.reply(count)


async def main():
    async with ActorSystem() as system:
        ref = await system.actor(counter(0), name="counter")

        # send() - fire and forget
        await ref.send(Increment(10))
        await ref.send(Increment(5))
        await ref.send(Decrement(3))

        # ask() - request-response
        result = await ref.ask(Get())
        print(f"Final count: {result}")


if __name__ == "__main__":
    asyncio.run(main())
