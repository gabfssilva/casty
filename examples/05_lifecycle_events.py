"""Lifecycle + EventStream — hooks e observabilidade do sistema."""

import asyncio

from casty import (
    ActorContext,
    ActorStarted,
    ActorStopped,
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
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


async def main() -> None:
    async with ActorSystem(name="lifecycle-demo") as system:
        system.event_stream.subscribe(
            ActorStarted, lambda e: print(f"  [event] ator iniciou: {e.ref}")
        )
        system.event_stream.subscribe(
            ActorStopped, lambda e: print(f"  [event] ator parou: {e.ref}")
        )
        system.event_stream.subscribe(
            DeadLetter, lambda e: print(f"  [dead letter] msg={e.message}")
        )

        ref = system.spawn(my_actor(), "meu-ator")
        await asyncio.sleep(0.1)

        ref.tell("oi")
        ref.tell("stop")
        await asyncio.sleep(0.1)

        # Mensagem para ator morto -> dead letter
        ref.tell("alguém aí?")
        await asyncio.sleep(0.1)


asyncio.run(main())
