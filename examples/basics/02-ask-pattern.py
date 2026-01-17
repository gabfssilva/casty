"""Ask Pattern: send() vs ask() and timeout handling.

Demonstrates:
- Fire-and-forget with send() - no response expected
- Request-response with ask() - awaits a reply
- Timeout handling with ask(timeout=)
- Catching asyncio.TimeoutError

Run with:
    uv run python examples/basics/02-ask-pattern.py
"""

import asyncio
from dataclasses import dataclass

from casty import Actor, ActorSystem, Context


@dataclass
class Increment:
    """Fire-and-forget: increment the counter."""
    amount: int


@dataclass
class GetCount:
    """Request-response: get current count."""
    pass


@dataclass
class SlowQuery:
    """A query that takes a long time to respond."""
    delay: float


class Counter(Actor[Increment | GetCount | SlowQuery]):
    """Simple counter actor demonstrating send vs ask patterns."""

    def __init__(self):
        self.count = 0

    async def receive(self, msg: Increment | GetCount | SlowQuery, ctx: Context):
        match msg:
            case Increment(amount):
                # Fire-and-forget: no reply needed
                self.count += amount
                print(f"[Counter] Incremented by {amount}, count is now {self.count}")

            case GetCount():
                # Request-response: reply with current count
                print(f"[Counter] Replying with count: {self.count}")
                await ctx.reply(self.count)

            case SlowQuery(delay):
                # Simulate slow processing
                print(f"[Counter] Processing slow query (will take {delay}s)...")
                await asyncio.sleep(delay)
                print(f"[Counter] Slow query complete, replying")
                await ctx.reply(self.count)


async def main():
    print("=" * 60)
    print("Casty Ask Pattern Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        counter = await system.spawn(Counter)

        # --- 1. Fire-and-forget with send() ---
        print("--- 1. Fire-and-forget with send() ---")
        print("Sending increments (no response expected)...")
        await counter.send(Increment(5))
        await counter.send(Increment(10))
        await counter.send(Increment(3))
        # Give time for messages to be processed
        await asyncio.sleep(0.1)
        print()

        # --- 2. Request-response with ask() ---
        print("--- 2. Request-response with ask() ---")
        result = await counter.ask(GetCount())
        print(f"Asked for count, got: {result}")
        print()

        # --- 3. ask() with explicit timeout (success) ---
        print("--- 3. ask() with timeout (success case) ---")
        print("Asking slow query with 2s timeout, query takes 0.5s...")
        result = await counter.ask(SlowQuery(delay=0.5), timeout=2.0)
        print(f"Got response: {result}")
        print()

        # --- 4. ask() with timeout (timeout case) ---
        print("--- 4. ask() with timeout (timeout case) ---")
        print("Asking slow query with 0.2s timeout, query takes 1s...")
        try:
            result = await counter.ask(SlowQuery(delay=1.0), timeout=0.2)
            print(f"Got response: {result}")  # Won't reach here
        except asyncio.TimeoutError:
            print("TimeoutError caught! The query took too long.")
        print()

        # --- 5. Using operators: >> for send, << for ask ---
        print("--- 5. Operator syntax: >> (send) and << (ask) ---")
        await (counter >> Increment(100))  # Same as await counter.send(...)
        await asyncio.sleep(0.1)
        result = await (counter << GetCount())  # Same as await counter.ask(...)
        print(f"After >> Increment(100), << GetCount() returned: {result}")

    print()
    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
