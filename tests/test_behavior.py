from __future__ import annotations

import pytest

from casty.actor import (
    Behaviors,
    LifecycleBehavior,
    ReceiveBehavior,
    RestartBehavior,
    SameBehavior,
    SetupBehavior,
    StoppedBehavior,
    SupervisedBehavior,
    UnhandledBehavior,
)


async def test_receive_creates_receive_behavior() -> None:
    async def handler(ctx: object, msg: str) -> SameBehavior:
        return Behaviors.same()

    b = Behaviors.receive(handler)
    assert isinstance(b, ReceiveBehavior)
    assert b.handler is handler


async def test_setup_creates_setup_behavior() -> None:
    async def factory(ctx: object) -> SameBehavior:
        return Behaviors.same()

    b = Behaviors.setup(factory)
    assert isinstance(b, SetupBehavior)
    assert b.factory is factory


async def test_same_returns_singleton_type() -> None:
    b = Behaviors.same()
    assert isinstance(b, SameBehavior)


async def test_stopped_returns_singleton_type() -> None:
    b = Behaviors.stopped()
    assert isinstance(b, StoppedBehavior)


async def test_unhandled_returns_singleton_type() -> None:
    b = Behaviors.unhandled()
    assert isinstance(b, UnhandledBehavior)


async def test_restart_returns_singleton_type() -> None:
    b = Behaviors.restart()
    assert isinstance(b, RestartBehavior)


async def test_with_lifecycle_wraps_behavior() -> None:
    inner = Behaviors.receive(lambda ctx, msg: Behaviors.same())
    pre = lambda ctx: None
    post = lambda ctx: None

    b = Behaviors.with_lifecycle(inner, pre_start=pre, post_stop=post)
    assert isinstance(b, LifecycleBehavior)
    assert b.behavior is inner
    assert b.pre_start is pre
    assert b.post_stop is post
    assert b.pre_restart is None
    assert b.post_restart is None


async def test_supervise_wraps_behavior_with_strategy() -> None:
    inner = Behaviors.receive(lambda ctx, msg: Behaviors.same())

    class FakeStrategy:
        def decide(self, exc: Exception) -> None:
            pass

    strategy = FakeStrategy()
    b = Behaviors.supervise(inner, strategy)  # type: ignore[arg-type]
    assert isinstance(b, SupervisedBehavior)
    assert b.behavior is inner
    assert b.strategy is strategy
