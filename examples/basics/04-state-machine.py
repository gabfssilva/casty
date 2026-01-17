"""State Machine: FSM with become() for behavior switching.

Demonstrates:
- Traffic light FSM: green -> yellow -> red -> green
- Using ctx.become() to switch behavior/state
- Using ctx.schedule() for timed state transitions
- Each state as a separate behavior function

Run with:
    uv run python examples/basics/04-state-machine.py
"""

import asyncio
from dataclasses import dataclass

from casty import Actor, ActorSystem, Context


# --- Messages ---

@dataclass
class Timeout:
    """Internal message for timed transitions."""
    pass


@dataclass
class GetState:
    """Query current traffic light state."""
    pass


@dataclass
class Start:
    """Start the traffic light cycle."""
    pass


# --- Traffic Light Actor ---

class TrafficLight(Actor[Start | Timeout | GetState]):
    """Traffic light that cycles: green -> yellow -> red -> green.

    Each state is a separate behavior function. The light uses
    ctx.schedule() to trigger automatic transitions after a delay.

    Timing:
    - Green: 3 seconds
    - Yellow: 1 second
    - Red: 3 seconds
    """

    # Durations in seconds (shortened for demo)
    GREEN_DURATION = 1.0
    YELLOW_DURATION = 0.5
    RED_DURATION = 1.0

    def __init__(self):
        self.current_state = "off"
        self.cycles_completed = 0

    async def receive(self, msg: Start | Timeout | GetState, ctx: Context):
        """Initial state (off) - waiting to be started."""
        match msg:
            case Start():
                print("[TrafficLight] Starting in GREEN state")
                self.current_state = "green"
                # Schedule transition to yellow
                await ctx.schedule(self.GREEN_DURATION, Timeout())
                # Switch to green behavior
                ctx.become(self.green_behavior)

            case GetState():
                await ctx.reply(self.current_state)

    async def green_behavior(self, msg: Start | Timeout | GetState, ctx: Context):
        """Green state - cars can go."""
        match msg:
            case Timeout():
                print("[TrafficLight] GREEN -> YELLOW")
                self.current_state = "yellow"
                await ctx.schedule(self.YELLOW_DURATION, Timeout())
                ctx.become(self.yellow_behavior, discard_old=True)

            case GetState():
                await ctx.reply(self.current_state)

            case Start():
                pass  # Already running

    async def yellow_behavior(self, msg: Start | Timeout | GetState, ctx: Context):
        """Yellow state - prepare to stop."""
        match msg:
            case Timeout():
                print("[TrafficLight] YELLOW -> RED")
                self.current_state = "red"
                await ctx.schedule(self.RED_DURATION, Timeout())
                ctx.become(self.red_behavior, discard_old=True)

            case GetState():
                await ctx.reply(self.current_state)

            case Start():
                pass  # Already running

    async def red_behavior(self, msg: Start | Timeout | GetState, ctx: Context):
        """Red state - cars must stop."""
        match msg:
            case Timeout():
                self.cycles_completed += 1
                print(f"[TrafficLight] RED -> GREEN (cycle {self.cycles_completed} complete)")
                self.current_state = "green"
                await ctx.schedule(self.GREEN_DURATION, Timeout())
                ctx.become(self.green_behavior, discard_old=True)

            case GetState():
                await ctx.reply(self.current_state)

            case Start():
                pass  # Already running


async def main():
    print("=" * 60)
    print("Casty State Machine Example: Traffic Light")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        traffic_light = await system.spawn(TrafficLight)

        # Check initial state
        state = await traffic_light.ask(GetState())
        print(f"Initial state: {state}")
        print()

        # Start the traffic light
        await traffic_light.send(Start())

        # Watch the traffic light cycle through states
        print("\nWatching traffic light for 2 full cycles...\n")

        for i in range(12):
            await asyncio.sleep(0.5)
            state = await traffic_light.ask(GetState())
            # Visual representation of traffic light
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
