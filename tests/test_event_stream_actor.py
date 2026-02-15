from __future__ import annotations

import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorSystem, Behavior, Behaviors
from casty.core.event_stream import (
    Publish,
    Subscribe,
    Unsubscribe,
    event_stream_actor,
)


async def test_event_stream_actor_subscribe_and_publish() -> None:
    received: list[object] = []

    @dataclass(frozen=True)
    class MyEvent:
        value: int

    def collector() -> Behavior[MyEvent]:
        async def receive(ctx: ActorContext[MyEvent], msg: MyEvent) -> Behavior[MyEvent]:
            received.append(msg)
            return Behaviors.same()
        return Behaviors.receive(receive)

    async with ActorSystem("test") as system:
        stream = system.spawn(event_stream_actor(), "_event_stream")
        observer = system.spawn(collector(), "observer")

        stream.tell(Subscribe(event_type=MyEvent, handler=observer))
        await asyncio.sleep(0.05)

        stream.tell(Publish(event=MyEvent(42)))
        await asyncio.sleep(0.05)

    assert received == [MyEvent(42)]


async def test_event_stream_unsubscribe() -> None:
    received: list[object] = []

    @dataclass(frozen=True)
    class Evt:
        v: int

    def collector() -> Behavior[Evt]:
        async def receive(ctx: ActorContext[Evt], msg: Evt) -> Behavior[Evt]:
            received.append(msg)
            return Behaviors.same()
        return Behaviors.receive(receive)

    async with ActorSystem("test") as system:
        stream = system.spawn(event_stream_actor(), "_event_stream")
        observer = system.spawn(collector(), "observer")

        stream.tell(Subscribe(event_type=Evt, handler=observer))
        await asyncio.sleep(0.05)

        stream.tell(Publish(event=Evt(1)))
        await asyncio.sleep(0.05)

        stream.tell(Unsubscribe(event_type=Evt, handler=observer))
        await asyncio.sleep(0.05)

        stream.tell(Publish(event=Evt(2)))
        await asyncio.sleep(0.05)

    assert received == [Evt(1)]
