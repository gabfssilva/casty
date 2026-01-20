"""State Machine: FSM with behavior switching.

Demonstrates:
- Traffic light FSM: green -> yellow -> red -> green
- Using ctx.schedule() for timed state transitions
- State as local variable in the actor

Run with:
    uv run python examples/basics/04-state-machine.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, ActorSystem, Mailbox


@dataclass
class Timeout:
    pass


@dataclass
class GetState:
    pass


@dataclass
class Start:
    pass


GREEN_DURATION = 1.0
YELLOW_DURATION = 0.5
RED_DURATION = 1.0


@actor
async def traffic_light(mailbox: Mailbox[Start | Timeout | GetState]):
    current_state = "off"
    cycles_completed = 0

    async for msg, ctx in mailbox:
        match msg:
            case Start() if current_state == "off":
                print("[TrafficLight] Starting in GREEN state")
                current_state = "green"
                await ctx.schedule(Timeout(), delay=GREEN_DURATION)

            case Timeout() if current_state == "green":
                print("[TrafficLight] GREEN -> YELLOW")
                current_state = "yellow"
                await ctx.schedule(Timeout(), delay=YELLOW_DURATION)

            case Timeout() if current_state == "yellow":
                print("[TrafficLight] YELLOW -> RED")
                current_state = "red"
                await ctx.schedule(Timeout(), delay=RED_DURATION)

            case Timeout() if current_state == "red":
                cycles_completed += 1
                print(f"[TrafficLight] RED -> GREEN (cycle {cycles_completed} complete)")
                current_state = "green"
                await ctx.schedule(Timeout(), delay=GREEN_DURATION)

            case GetState():
                await ctx.reply(current_state)

            case Start():
                pass  # Already running


async def main():
    print("=" * 60)
    print("Casty State Machine Example: Traffic Light")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        light = await system.actor(traffic_light(), name="traffic-light")

        state = await light.ask(GetState())
        print(f"Initial state: {state}")
        print()

        await light.send(Start())

        print("\nWatching traffic light for 2 full cycles...\n")

        for i in range(12):
            await asyncio.sleep(0.5)
            state = await light.ask(GetState())
            match state:
                case "green":
                    visual = "[  G  ]"
                case "yellow":
                    visual = "[  Y  ]"
                case "red":
                    visual = "[  R  ]"
                case _:
                    visual = "[ OFF ]"
            print(f"  t={i * 0.5:.1f}s: {visual} {state.upper()}")

    print()
    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
