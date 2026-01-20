"""Ping-Pong: Two actors exchanging messages.

Demonstrates:
- Creating multiple actors with @actor decorator
- Using ctx.sender to reply back to the message sender
- Bidirectional message flow
- Stopping after N exchanges using a counter

Run with:
    uv run python examples/basics/01-ping-pong.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, ActorSystem, Mailbox, LocalActorRef


@dataclass
class Ping:
    count: int


@dataclass
class Pong:
    count: int


@dataclass
class Start:
    target: LocalActorRef[Ping]
    max_exchanges: int


@actor
async def ping_actor(*, mailbox: Mailbox[Start | Pong]):
    exchanges = 0
    max_exchanges = 0

    async for msg, ctx in mailbox:
        match msg:
            case Start(target, max_ex):
                max_exchanges = max_ex
                print(f"[Ping] Starting ping-pong for {max_exchanges} exchanges")
                await target.send(Ping(count=1), sender=ctx.self_id)

            case Pong(count):
                exchanges = count
                print(f"[Ping] Received Pong #{count}")

                if count < max_exchanges and ctx.sender:
                    await ctx.sender.send(Ping(count=count + 1), sender=ctx.self_id)
                elif count >= max_exchanges:
                    print(f"[Ping] Reached {max_exchanges} exchanges. Done!")


@actor
async def pong_actor(*, mailbox: Mailbox[Ping]):
    async for msg, ctx in mailbox:
        match msg:
            case Ping(count):
                print(f"[Pong] Received Ping #{count}, sending Pong")
                if ctx.sender:
                    await ctx.sender.send(Pong(count=count), sender=ctx.self_id)


async def main():
    print("=" * 50)
    print("Casty Ping-Pong Example")
    print("=" * 50)
    print()

    async with ActorSystem() as system:
        ping = await system.actor(ping_actor(), name="ping")
        pong = await system.actor(pong_actor(), name="pong")

        await ping.send(Start(target=pong, max_exchanges=5))

        await asyncio.sleep(0.5)

    print()
    print("=" * 50)
    print("Done!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
