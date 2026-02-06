# tests/test_system.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty.actor import Behaviors, Behavior
from casty.events import ActorStarted
from casty.ref import ActorRef
from casty.system import ActorSystem


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

    async with ActorSystem() as system:
        system.event_stream.subscribe(ActorStarted, lambda e: events.append(e))  # type: ignore[arg-type,return-value]
        system.spawn(
            Behaviors.receive(lambda ctx, msg: Behaviors.same()), "actor"
        )
        await asyncio.sleep(0.1)

    assert len(events) >= 1


async def test_system_shutdown_stops_all_actors() -> None:
    stopped = False

    async def post_stop(ctx: Any) -> None:
        nonlocal stopped
        stopped = True

    behavior = Behaviors.with_lifecycle(
        Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        post_stop=post_stop,
    )

    async with ActorSystem() as system:
        system.spawn(behavior, "actor")
        await asyncio.sleep(0.1)

    # After exiting context manager, shutdown was called
    assert stopped


async def test_system_named() -> None:
    async with ActorSystem(name="test-system") as system:
        assert system.name == "test-system"
