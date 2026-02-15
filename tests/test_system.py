# tests/test_system.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty.actor import Behaviors, Behavior
from casty.core.events import ActorStarted
from casty.ref import ActorRef
from casty.core.system import ActorSystem


@dataclass(frozen=True)
class Greet:
    name: str


@dataclass(frozen=True)
class GetCount:
    reply_to: ActorRef[int]


type GreeterMsg = Greet | GetCount


def greeter(count: int = 0) -> Behavior[GreeterMsg]:
    async def receive(ctx: Any, msg: GreeterMsg) -> Any:
        match msg:
            case Greet():
                return greeter(count + 1)
            case GetCount(reply_to):
                reply_to.tell(count)
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


async def test_system_context_manager() -> None:
    async with ActorSystem() as system:
        assert system is not None


async def test_system_spawn_and_tell() -> None:
    received: list[str] = []

    async def handler(ctx: Any, msg: Greet) -> Any:
        received.append(msg.name)
        return Behaviors.same()

    async with ActorSystem() as system:
        ref = system.spawn(Behaviors.receive(handler), "greeter")
        ref.tell(Greet("Gabriel"))
        await asyncio.sleep(0.1)

    assert received == ["Gabriel"]


async def test_system_ask() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(greeter(), "greeter")
        ref.tell(Greet("a"))
        ref.tell(Greet("b"))
        await asyncio.sleep(0.1)

        count = await system.ask(ref, lambda r: GetCount(reply_to=r), timeout=5.0)
        assert count == 2


async def test_system_ask_timeout() -> None:
    async def black_hole(ctx: Any, msg: Any) -> Any:
        return Behaviors.same()  # never replies

    async with ActorSystem() as system:
        ref = system.spawn(Behaviors.receive(black_hole), "hole")
        with pytest.raises(TimeoutError):
            await system.ask(ref, lambda r: "ignored", timeout=0.1)


async def test_system_lookup() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(
            Behaviors.receive(lambda ctx, msg: Behaviors.same()), "my-actor"
        )
        found = system.lookup("/my-actor")
        assert found is ref


async def test_system_lookup_not_found() -> None:
    async with ActorSystem() as system:
        assert system.lookup("/nonexistent") is None


async def test_system_event_stream() -> None:
    events: list[ActorStarted] = []
    from casty.core.event_stream import Subscribe as ESSubscribe

    async with ActorSystem() as system:
        observer = system.spawn(
            Behaviors.receive(
                lambda ctx, msg: (events.append(msg), Behaviors.same())[1]
            ),
            "observer",
        )
        system.event_stream.tell(ESSubscribe(event_type=ActorStarted, handler=observer))
        await asyncio.sleep(0.05)

        system.spawn(Behaviors.receive(lambda ctx, msg: Behaviors.same()), "actor")
        await asyncio.sleep(0.1)

    assert len(events) >= 1


async def test_system_shutdown_stops_all_actors() -> None:
    async with ActorSystem() as system:
        system.spawn(
            Behaviors.receive(lambda ctx, msg: Behaviors.same()),
            "actor",
        )
        await asyncio.sleep(0.1)

    cell = system._root_cells.get("actor")
    assert cell is None


async def test_system_named() -> None:
    async with ActorSystem(name="test-system") as system:
        assert system.name == "test-system"
