"""Scheduling: Timed messages and periodic tasks.

Demonstrates:
- ctx.schedule(delay=) for one-time delayed messages
- ctx.schedule(every=) for periodic messages
- Building a state machine with timeouts

Run with:
    uv run python examples/03-scheduling.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, ActorSystem, Mailbox
from casty.state import State


@dataclass
class Start:
    pass


@dataclass
class Tick:
    pass


@dataclass
class Stop:
    pass


@dataclass
class GetState:
    pass


type TrafficLightMsg = Start | Tick | Stop | GetState

STATES = ["green", "yellow", "red"]
DURATIONS = {"green": 1.0, "yellow": 0.5, "red": 1.0}


@actor
async def traffic_light(state: State[int], *, mailbox: Mailbox[TrafficLightMsg]):
    running = False
    cancel_tick = None

    async for msg, ctx in mailbox:
        match msg:
            case Start() if not running:
                running = True
                state.value = 0
                print(f"Started -> {STATES[state.value].upper()}")
                cancel_tick = await ctx.schedule(Tick(), delay=DURATIONS[STATES[state.value]])

            case Tick() if running:
                state.value = (state.value + 1) % len(STATES)
                print(f"Tick -> {STATES[state.value].upper()}")
                cancel_tick = await ctx.schedule(Tick(), delay=DURATIONS[STATES[state.value]])

            case Stop():
                if cancel_tick:
                    await cancel_tick()
                running = False
                print("Stopped")

            case GetState():
                if running:
                    await ctx.reply(STATES[state.value])
                else:
                    await ctx.reply("off")


async def main():
    async with ActorSystem() as system:
        light = await system.actor(traffic_light(State(0)), name="traffic-light")

        await light.send(Start())

        for _ in range(6):
            await asyncio.sleep(0.8)
            state = await light.ask(GetState())
            print(f"  Current: {state}")

        await light.send(Stop())


if __name__ == "__main__":
    asyncio.run(main())
