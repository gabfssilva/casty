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

from casty import actor, ActorSystem, Mailbox


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


@dataclass
class SlowQuery:
    delay: float


@actor
async def counter(*, mailbox: Mailbox[Increment | GetCount | SlowQuery]):
    count = 0

    async for msg, ctx in mailbox:
        match msg:
            case Increment(amount):
                count += amount
                print(f"[Counter] Incremented by {amount}, count is now {count}")

            case GetCount():
                print(f"[Counter] Replying with count: {count}")
                await ctx.reply(count)

            case SlowQuery(delay):
                print(f"[Counter] Processing slow query (will take {delay}s)...")
                await asyncio.sleep(delay)
                print(f"[Counter] Slow query complete, replying")
                await ctx.reply(count)


async def main():
    print("=" * 60)
    print("Casty Ask Pattern Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        ref = await system.actor(counter(), name="counter")

        # --- 1. Fire-and-forget with send() ---
        print("--- 1. Fire-and-forget with send() ---")
        print("Sending increments (no response expected)...")
        await ref.send(Increment(5))
        await ref.send(Increment(10))
        await ref.send(Increment(3))
        await asyncio.sleep(0.1)
        print()

        # --- 2. Request-response with ask() ---
        print("--- 2. Request-response with ask() ---")
        result = await ref.ask(GetCount())
        print(f"Asked for count, got: {result}")
        print()

        # --- 3. ask() with explicit timeout (success) ---
        print("--- 3. ask() with timeout (success case) ---")
        print("Asking slow query with 2s timeout, query takes 0.5s...")
        result = await ref.ask(SlowQuery(delay=0.5), timeout=2.0)
        print(f"Got response: {result}")
        print()

        # --- 4. ask() with timeout (timeout case) ---
        print("--- 4. ask() with timeout (timeout case) ---")
        print("Asking slow query with 0.2s timeout, query takes 1s...")
        try:
            result = await ref.ask(SlowQuery(delay=1.0), timeout=0.2)
            print(f"Got response: {result}")
        except asyncio.TimeoutError:
            print("TimeoutError caught! The query took too long.")
        print()

        # --- 5. Using operators: >> for send, << for ask ---
        print("--- 5. Operator syntax: >> (send) and << (ask) ---")
        await (ref >> Increment(100))
        await asyncio.sleep(0.1)
        result = await (ref << GetCount())
        print(f"After >> Increment(100), << GetCount() returned: {result}")

    print()
    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
