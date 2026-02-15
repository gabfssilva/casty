"""Lifecycle + EventStream — hooks e observabilidade do sistema."""

import asyncio
from typing import Any

from casty import (
    ActorContext,
    ActorStarted,
    ActorStopped,
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    EventStreamSubscribe,
)


def my_actor() -> Behavior[str]:
    async def pre_start(ctx: ActorContext[str]) -> None:
        print(f"  [pre_start] {ctx.self} iniciando")

    async def post_stop(ctx: ActorContext[str]) -> None:
        print(f"  [post_stop] {ctx.self} parou")

    async def receive(ctx: ActorContext[str], msg: str) -> Behavior[str]:
        if msg == "stop":
            print("  [actor] recebeu ordem de parar")
            return Behaviors.stopped()
        print(f"  [actor] recebeu: {msg}")
        return Behaviors.same()

    return Behaviors.with_lifecycle(
        Behaviors.receive(receive),
        pre_start=pre_start,
        post_stop=post_stop,
    )


def event_logger() -> Behavior[Any]:
    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case ActorStarted(ref=ref):
                print(f"  [event] ator iniciou: {ref}")
            case ActorStopped(ref=ref):
                print(f"  [event] ator parou: {ref}")
            case DeadLetter(message=message):
                print(f"  [dead letter] msg={message}")
            case _:
                pass
        return Behaviors.same()
    return Behaviors.receive(receive)


async def main() -> None:
    async with ActorSystem(name="lifecycle-demo") as system:
        logger = system.spawn(event_logger(), "_event_logger")
        system.event_stream.tell(EventStreamSubscribe(event_type=ActorStarted, handler=logger))
        system.event_stream.tell(EventStreamSubscribe(event_type=ActorStopped, handler=logger))
        system.event_stream.tell(EventStreamSubscribe(event_type=DeadLetter, handler=logger))

        ref = system.spawn(my_actor(), "meu-ator")
        await asyncio.sleep(0.1)

        ref.tell("oi")
        ref.tell("stop")
        await asyncio.sleep(0.1)

        # Mensagem para ator morto -> dead letter
        ref.tell("alguém aí?")
        await asyncio.sleep(0.1)


asyncio.run(main())
