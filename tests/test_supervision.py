import asyncio
import pytest
from dataclasses import dataclass

from casty import ActorSystem, actor, Mailbox
from casty.supervision import supervised, Restart, OneForOne
from casty.state import State


@dataclass
class Crash:
    pass


@dataclass
class Ping:
    pass


@dataclass
class Get:
    pass


@dataclass
class Inc:
    pass


@pytest.mark.asyncio
async def test_supervised_actor_restarts_after_crash():
    """Actor with Restart strategy restarts after exception"""
    crash_count = 0

    @supervised(strategy=Restart(max_retries=3), scope=OneForOne())
    @actor
    async def crasher(*, mailbox: Mailbox[Crash | Ping]):
        nonlocal crash_count
        async for msg, ctx in mailbox:
            match msg:
                case Crash():
                    crash_count += 1
                    raise ValueError("intentional crash")
                case Ping():
                    await ctx.reply("pong")

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(crasher(), name="crasher")

            await ref.send(Crash())
            await asyncio.sleep(0.1)

            result = await ref.ask(Ping())

            assert result == "pong"
            assert crash_count >= 1


@pytest.mark.asyncio
async def test_actor_stops_after_max_retries():
    """Actor stops permanently after exceeding max_retries"""
    crash_count = 0

    @supervised(strategy=Restart(max_retries=2, within=1.0), scope=OneForOne())
    @actor
    async def crasher(*, mailbox: Mailbox[Crash]):
        nonlocal crash_count
        async for msg, ctx in mailbox:
            crash_count += 1
            raise ValueError("crash")

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(crasher(), name="crasher")

            for _ in range(5):
                await ref.send(Crash())
                await asyncio.sleep(0.05)

            await asyncio.sleep(0.2)

            assert crash_count <= 3  # max_retries + 1 initial


@pytest.mark.asyncio
async def test_state_preserved_after_restart():
    """Actor state is preserved across restarts (not reset)"""

    @supervised(strategy=Restart(max_retries=3), scope=OneForOne())
    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Inc | Crash | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Inc():
                    state.set(state.value + 1)
                case Crash():
                    raise ValueError("crash")
                case Get():
                    await ctx.reply(state.value)

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(counter(State(0)), name="counter")

            await ref.send(Inc())
            await ref.send(Inc())
            await ref.send(Inc())

            before_crash = await ref.ask(Get())
            assert before_crash == 3

            await ref.send(Crash())
            await asyncio.sleep(0.1)

            after_crash = await ref.ask(Get())
            assert after_crash == 3  # State preserved across restart
