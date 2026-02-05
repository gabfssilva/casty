"""Supervision — ator falha e é reiniciado automaticamente."""

import asyncio

from casty import ActorContext, ActorSystem, Behavior, Behaviors, Directive, OneForOneStrategy


def unreliable_worker() -> Behavior[str]:
    call_count = 0

    async def receive(ctx: ActorContext[str], msg: str) -> Behavior[str]:
        nonlocal call_count
        call_count += 1

        if msg == "crash":
            print(f"  [worker] BOOM! (tentativa {call_count})")
            raise RuntimeError("falha proposital")

        print(f"  [worker] processou: {msg} (tentativa {call_count})")
        return Behaviors.same()

    return Behaviors.receive(receive)


async def main() -> None:
    strategy = OneForOneStrategy(
        max_restarts=3,
        within=60.0,
        decider=lambda exc: Directive.restart,
    )

    async with ActorSystem() as system:
        ref = system.spawn(
            Behaviors.supervise(unreliable_worker(), strategy),
            "worker",
        )

        ref.tell("crash")
        await asyncio.sleep(0.2)

        ref.tell("hello depois do restart")
        await asyncio.sleep(0.1)

        ref.tell("crash")
        await asyncio.sleep(0.2)

        ref.tell("ainda funcionando!")
        await asyncio.sleep(0.1)


asyncio.run(main())
