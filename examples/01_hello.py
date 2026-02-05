"""Hello World — o exemplo mais simples possível."""

import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class Greet:
    name: str


def greeter() -> Behavior[Greet]:
    async def receive(ctx: ActorContext[Greet], msg: Greet) -> Behavior[Greet]:
        print(f"Hello, {msg.name}!")
        return Behaviors.same()

    return Behaviors.receive(receive)


async def main() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(greeter(), "greeter")

        ref.tell(Greet("Alice"))
        ref.tell(Greet("Bob"))
        ref.tell(Greet("Charlie"))

        await asyncio.sleep(0.1)


asyncio.run(main())
