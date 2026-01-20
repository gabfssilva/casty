# tests/test_supervised.py
import pytest
from dataclasses import dataclass

from casty import actor, Mailbox
from casty.supervision import supervised, Restart, OneForOne, Decision


@dataclass
class Crash:
    pass


@dataclass
class GetCrashCount:
    pass


def test_supervised_decorator_attaches_config():
    @supervised(strategy=Restart(max_retries=5), scope=OneForOne())
    @actor
    async def worker(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass

    behavior = worker()
    assert behavior.supervision is not None
    assert behavior.supervision.strategy.max_retries == 5
    assert behavior.supervision.scope.affects_siblings is False


@dataclass
class Ping:
    pass


@pytest.mark.asyncio
async def test_supervised_actor_restarts_on_error():
    from casty.system import ActorSystem
    import asyncio

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

    async with ActorSystem() as system:
        ref = await system.actor(crasher(), name="crasher")

        # Send crash message
        await ref.send(Crash())

        # Give time for restart
        await asyncio.sleep(0.1)

        # Actor should have restarted and be able to respond
        result = await ref.ask(Ping())
        assert result == "pong"
        assert crash_count >= 1
