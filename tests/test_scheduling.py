import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox


@dataclass
class Tick:
    pass


@dataclass
class GetCount:
    pass


@actor
async def ticker(*, mailbox: Mailbox[Tick | GetCount]):
    count = 0
    async for msg, ctx in mailbox:
        match msg:
            case Tick():
                count += 1
            case GetCount():
                await ctx.reply(count)


@pytest.mark.asyncio
async def test_system_schedule_delay():
    from casty.system import ActorSystem

    async with ActorSystem() as system:
        ref = await system.actor(ticker(), name="t1")

        await system.schedule(Tick(), to=ref, delay=0.05)

        count = await ref.ask(GetCount())
        assert count == 0

        await asyncio.sleep(0.1)

        count = await ref.ask(GetCount())
        assert count == 1


@pytest.mark.asyncio
async def test_system_schedule_every():
    from casty.system import ActorSystem

    async with ActorSystem() as system:
        ref = await system.actor(ticker(), name="t1")

        cancel = await system.schedule(Tick(), to=ref, every=0.03)

        await asyncio.sleep(0.1)
        await cancel()

        count = await ref.ask(GetCount())
        assert count >= 2


@pytest.mark.asyncio
async def test_context_schedule():
    from casty.system import ActorSystem

    @dataclass
    class Start:
        pass

    @actor
    async def self_ticker(*, mailbox: Mailbox[Start | Tick | GetCount]):
        count = 0
        async for msg, ctx in mailbox:
            match msg:
                case Start():
                    await ctx.schedule(Tick(), delay=0.05)
                case Tick():
                    count += 1
                case GetCount():
                    await ctx.reply(count)

    async with ActorSystem() as system:
        ref = await system.actor(self_ticker(), name="st1")

        await ref.send(Start())
        await asyncio.sleep(0.1)

        count = await ref.ask(GetCount())
        assert count == 1
