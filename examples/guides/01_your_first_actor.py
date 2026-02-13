"""Traffic light — state machines via behavior recursion."""

import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class Tick:
    pass


@dataclass(frozen=True)
class GetColor:
    reply_to: ActorRef[str]


type TrafficLightMsg = Tick | GetColor


def green() -> Behavior[TrafficLightMsg]:
    async def receive(
        ctx: ActorContext[TrafficLightMsg], msg: TrafficLightMsg
    ) -> Behavior[TrafficLightMsg]:
        match msg:
            case Tick():
                print("🟢 → 🟡")
                return yellow()
            case GetColor(reply_to):
                reply_to.tell("green")
                return Behaviors.same()

    return Behaviors.receive(receive)


def yellow() -> Behavior[TrafficLightMsg]:
    async def receive(
        ctx: ActorContext[TrafficLightMsg], msg: TrafficLightMsg
    ) -> Behavior[TrafficLightMsg]:
        match msg:
            case Tick():
                print("🟡 → 🔴")
                return red()
            case GetColor(reply_to):
                reply_to.tell("yellow")
                return Behaviors.same()

    return Behaviors.receive(receive)


def red() -> Behavior[TrafficLightMsg]:
    async def receive(
        ctx: ActorContext[TrafficLightMsg], msg: TrafficLightMsg
    ) -> Behavior[TrafficLightMsg]:
        match msg:
            case Tick():
                print("🔴 → 🟢")
                return green()
            case GetColor(reply_to):
                reply_to.tell("red")
                return Behaviors.same()

    return Behaviors.receive(receive)


async def main() -> None:
    async with ActorSystem() as system:
        light = system.spawn(green(), "traffic-light")

        light.tell(Tick())  # green → yellow
        light.tell(Tick())  # yellow → red
        light.tell(Tick())  # red → green

        await asyncio.sleep(0.1)

        color = await system.ask(
            light, lambda r: GetColor(reply_to=r), timeout=5.0
        )
        print(f"Current color: {color}")  # green


asyncio.run(main())
