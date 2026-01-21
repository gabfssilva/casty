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

from casty import actor, ActorSystem, Mailbox, State


@dataclass
class Ping: pass


@dataclass
class Pong:
    count: int = 0


@actor
async def ping_actor(state: State[int], mailbox: Mailbox[Ping]):
    async for msg, ctx in mailbox:
        state.set(state.value + 1)
        await ctx.reply(Pong(state.value))


async def main():
    async with ActorSystem() as system:
        ping = await system.actor(ping_actor(0), name="ping")

        for _ in range(10):
            pong = await ping.ask(Ping())
            print(pong)


if __name__ == "__main__":
    asyncio.run(main())
