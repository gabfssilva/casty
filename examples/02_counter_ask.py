"""Counter com ask — estado funcional via closures + request-reply."""

import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class Increment:
    amount: int = 1


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetValue


def counter(value: int = 0) -> Behavior[CounterMsg]:
    async def receive(
        ctx: ActorContext[CounterMsg], msg: CounterMsg
    ) -> Behavior[CounterMsg]:
        match msg:
            case Increment(amount):
                new_value = value + amount
                print(f"  counter: {value} -> {new_value}")
                return counter(new_value)
            case GetValue(reply_to):
                reply_to.tell(value)
                return Behaviors.same()

    return Behaviors.receive(receive)


async def main() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(counter(), "counter")

        ref.tell(Increment(10))
        ref.tell(Increment(5))
        ref.tell(Increment(3))
        await asyncio.sleep(0.1)

        result = await system.ask(ref, lambda r: GetValue(reply_to=r), timeout=5.0)
        print(f"  valor final: {result}")


asyncio.run(main())
