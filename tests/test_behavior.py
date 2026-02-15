from __future__ import annotations

from typing import Any

from casty.actor import Behaviors
from casty.core.behavior import Behavior, Signal


async def test_receive_creates_receive_behavior() -> None:
    async def handler(ctx: object, msg: str) -> Behavior[Any]:
        return Behaviors.same()

    b = Behaviors.receive(handler)
    assert b.on_receive is handler
    assert b.on_setup is None
    assert b.signal is None


async def test_setup_creates_setup_behavior() -> None:
    async def factory(ctx: object) -> Behavior[Any]:
        return Behaviors.same()

    b = Behaviors.setup(factory)
    assert b.on_setup is factory
    assert b.on_receive is None
    assert b.signal is None


async def test_same_returns_signal() -> None:
    b = Behaviors.same()
    assert b.signal is Signal.same


async def test_stopped_returns_signal() -> None:
    b = Behaviors.stopped()
    assert b.signal is Signal.stopped


async def test_unhandled_returns_signal() -> None:
    b = Behaviors.unhandled()
    assert b.signal is Signal.unhandled


async def test_restart_returns_signal() -> None:
    b = Behaviors.restart()
    assert b.signal is Signal.restart


async def test_with_lifecycle_wraps_behavior() -> None:
    inner = Behaviors.receive(lambda ctx, msg: Behaviors.same())
    b = Behaviors.with_lifecycle(inner, pre_start=lambda ctx: None)
    assert b.on_setup is not None


async def test_ignore_returns_same_for_any_message() -> None:
    b = Behaviors.ignore()
    assert b.on_receive is not None
    assert b.signal is None

    from casty.core.actor import CellContext

    class FakeCell:
        pass

    result = await b.on_receive(CellContext(FakeCell()), "anything")  # type: ignore[arg-type]
    assert result.signal is Signal.same


async def test_ignore_in_actor_system() -> None:
    import asyncio
    from casty import ActorSystem, Behaviors

    async with ActorSystem("test") as system:
        ref = system.spawn(Behaviors.ignore(), "sink")
        ref.tell("hello")
        ref.tell(42)
        ref.tell({"key": "value"})
        await asyncio.sleep(0.1)


async def test_supervise_wraps_behavior() -> None:
    inner = Behaviors.receive(lambda ctx, msg: Behaviors.same())

    class FakeStrategy:
        def decide(self, exc: Exception, **kw: object) -> None:
            pass

    b = Behaviors.supervise(inner, FakeStrategy())  # type: ignore[arg-type]
    assert b.on_setup is not None
