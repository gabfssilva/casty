import asyncio
import pytest
from dataclasses import dataclass

from casty import ActorSystem, actor, Mailbox


@dataclass
class Tick:
    pass


@dataclass
class Start:
    pass


@pytest.mark.asyncio
async def test_schedule_delay_delivers_after_time():
    """schedule(msg, delay=X) delivers message after X seconds"""
    received_at: float | None = None
    start_time: float = 0

    @actor
    async def ticker(*, mailbox: Mailbox[Tick]):
        nonlocal received_at
        async for msg, ctx in mailbox:
            received_at = asyncio.get_event_loop().time()

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(ticker(), name="ticker")

            start_time = asyncio.get_event_loop().time()
            await system.schedule(Tick(), to=ref, delay=0.1)

            await asyncio.sleep(0.2)

    assert received_at is not None
    assert received_at - start_time >= 0.1


@pytest.mark.asyncio
async def test_schedule_periodic_delivers_repeatedly():
    """schedule(msg, every=X) delivers message every X seconds"""
    tick_count = 0

    @actor
    async def ticker(*, mailbox: Mailbox[Tick]):
        nonlocal tick_count
        async for msg, ctx in mailbox:
            tick_count += 1

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(ticker(), name="ticker")

            await system.schedule(Tick(), to=ref, every=0.05)

            await asyncio.sleep(0.2)

    assert tick_count >= 3


@pytest.mark.asyncio
async def test_cancel_schedule_stops_delivery():
    """Canceling a periodic schedule stops future deliveries"""
    tick_count = 0

    @actor
    async def ticker(*, mailbox: Mailbox[Tick]):
        nonlocal tick_count
        async for msg, ctx in mailbox:
            tick_count += 1

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(ticker(), name="ticker")

            cancel = await system.schedule(Tick(), to=ref, every=0.03)
            await asyncio.sleep(0.1)

            await cancel()
            count_at_cancel = tick_count

            await asyncio.sleep(0.1)

            assert tick_count == count_at_cancel


@pytest.mark.asyncio
async def test_schedule_via_context():
    """ctx.schedule() schedules message from within actor"""
    ticks: list[int] = []

    @actor
    async def self_ticker(*, mailbox: Mailbox[Start | Tick]):
        count = 0
        async for msg, ctx in mailbox:
            match msg:
                case Start():
                    await ctx.schedule(Tick(), delay=0.05)
                case Tick():
                    count += 1
                    ticks.append(count)
                    if count < 3:
                        await ctx.schedule(Tick(), delay=0.05)

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(self_ticker(), name="ticker")

            await ref.send(Start())

            await asyncio.sleep(0.3)

    assert ticks == [1, 2, 3]
