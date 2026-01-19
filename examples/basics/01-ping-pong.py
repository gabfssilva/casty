"""Ping-Pong: Two actors exchanging messages.

Demonstrates:
- Creating multiple actors
- Using ctx.sender to reply back to the message sender
- Bidirectional message flow
- Stopping after N exchanges using a counter

Run with:
    uv run python examples/basics/01-ping-pong.py
"""

import asyncio
from dataclasses import dataclass

from casty import Actor, ActorSystem, Context, ActorRef


@dataclass
class Ping:
    """Ping message with exchange count."""
    count: int


@dataclass
class Pong:
    """Pong message with exchange count."""
    count: int


@dataclass
class Start:
    """Message to start the ping-pong exchange."""
    target: ActorRef[Ping]
    max_exchanges: int


class PingActor(Actor[Start | Pong]):
    """Actor that initiates and continues the ping-pong exchange."""

    def __init__(self):
        self.exchanges = 0
        self.max_exchanges = 0

    async def receive(self, msg: Start | Pong, ctx: Context):
        match msg:
            case Start(target, max_exchanges):
                self.max_exchanges = max_exchanges
                print(f"[Ping] Starting ping-pong for {max_exchanges} exchanges")
                # Send first Ping, passing self as sender so Pong can reply
                await target.send(Ping(count=1), sender=ctx.self_ref)

            case Pong(count):
                self.exchanges = count
                print(f"[Ping] Received Pong #{count}")

                if count < self.max_exchanges and ctx.sender:
                    # Continue the exchange - reply back to sender
                    await ctx.sender.send(Ping(count=count + 1), sender=ctx.self_ref)
                elif count >= self.max_exchanges:
                    print(f"[Ping] Reached {self.max_exchanges} exchanges. Done!")


class PongActor(Actor[Ping]):
    """Actor that responds to Ping with Pong."""

    async def receive(self, msg: Ping, ctx: Context):
        match msg:
            case Ping(count):
                print(f"[Pong] Received Ping #{count}, sending Pong")
                # Reply back to whoever sent the Ping
                if ctx.sender:
                    await ctx.sender.send(Pong(count=count), sender=ctx.self_ref)


async def main():
    print("=" * 50)
    print("Casty Ping-Pong Example")
    print("=" * 50)
    print()

    async with ActorSystem.local() as system:
        # Create both actors
        ping_actor = await system.actor(PingActor, name="ping")
        pong_actor = await system.actor(PongActor, name="pong")

        # Start the exchange: 5 ping-pong rounds
        await ping_actor.send(Start(target=pong_actor, max_exchanges=5))

        # Give time for all exchanges to complete
        await asyncio.sleep(0.5)

    print()
    print("=" * 50)
    print("Done!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
