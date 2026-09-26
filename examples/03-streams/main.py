"""An actor that also acts on its own: `ctx.merge` reads the mailbox and a stream together, in arrival order.

Run with `uv run main.py`. The meter receives readings as messages and closes a window every half second, from a
ticker that is just an async generator. Both arrive in the same loop, so the state needs no lock.
"""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import assert_never

from casty import ActorSystem, Askable, Context, actor


@dataclass(frozen=True)
class Window:
    """Readings of the window that is open, and the average of each window already closed."""

    readings: tuple[float, ...] = ()
    averages: tuple[float, ...] = ()


@dataclass(frozen=True)
class Reading:
    value: float


@dataclass(frozen=True)
class Averages(Askable[tuple[float, ...]]):
    pass


@dataclass(frozen=True)
class Tick:
    pass


type MeterMsg = Reading | Averages


async def every(period: timedelta, /) -> AsyncIterator[Tick]:
    while True:
        await asyncio.sleep(period.total_seconds())
        yield Tick()


@actor(initial=Window())
async def meter(ctx: Context[Window, MeterMsg]) -> None:
    async for event in ctx.merge(every(timedelta(milliseconds=500))):
        match event:
            case Reading(value):
                await ctx.state.set(Window((*ctx.state.value.readings, value), ctx.state.value.averages))
            case Tick() if ctx.state.value.readings:
                average = sum(ctx.state.value.readings) / len(ctx.state.value.readings)
                await ctx.state.set(Window((), (*ctx.state.value.averages, average)))
            case Tick():
                pass
            case Averages(reply_to=reply_to):
                reply_to.tell(ctx.state.value.averages)
            case _:
                assert_never(event)


async def main() -> None:
    async with ActorSystem() as system:
        kitchen = system.ref(meter, "kitchen")
        for value in range(20):
            kitchen.tell(Reading(20 + value / 2))
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.6)
        print([round(average, 2) for average in await kitchen.ask(Averages())])


asyncio.run(main())
