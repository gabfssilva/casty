from __future__ import annotations

import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorSystem, Behavior, Behaviors


async def test_delivery_interceptor_captures_matching_messages() -> None:
    captured: list[object] = []

    @dataclass(frozen=True)
    class Special:
        value: int

    @dataclass(frozen=True)
    class Normal:
        value: int

    type Msg = Special | Normal

    def intercept(msg: object) -> bool:
        match msg:
            case Special():
                captured.append(msg)
                return True
            case _:
                return False

    results: list[object] = []

    def my_actor() -> Behavior[Msg]:
        async def setup(ctx: ActorContext[Msg]) -> Behavior[Msg]:
            ctx.register_interceptor(intercept)

            async def receive(ctx: ActorContext[Msg], msg: Msg) -> Behavior[Msg]:
                results.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        return Behaviors.setup(setup)

    async with ActorSystem("test") as system:
        ref = system.spawn(my_actor(), "actor")
        ref.tell(Normal(1))
        ref.tell(Special(42))
        ref.tell(Normal(2))
        await asyncio.sleep(0.1)

    assert len(results) == 2
    assert all(isinstance(r, Normal) for r in results)
    assert len(captured) == 1
    assert captured[0] == Special(42)
