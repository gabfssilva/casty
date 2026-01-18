"""Shared fixtures and test actors for Casty tests."""

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty import Actor, ActorSystem, Context, supervised, SupervisionStrategy


# Test message types


@dataclass
class Increment:
    """Increment a counter by amount."""

    amount: int = 1


@dataclass
class Decrement:
    """Decrement a counter by amount."""

    amount: int = 1


@dataclass
class GetValue:
    """Request current value (use with ask)."""

    pass


@dataclass
class SetValue:
    """Set the value directly."""

    value: int


@dataclass
class Reset:
    """Reset to initial value."""

    pass


@dataclass
class Ping:
    """Simple ping message."""

    pass


@dataclass
class Pong:
    """Simple pong response."""

    pass


@dataclass
class ForwardTo:
    """Forward a message to another actor."""

    target: Any
    message: Any


@dataclass
class SpawnChild:
    """Request to spawn a child actor."""

    name: str | None = None


@dataclass
class FailNow:
    """Intentionally fail processing."""

    error_message: str = "Intentional failure"


@dataclass
class GetChildren:
    """Get list of child actor IDs."""

    pass


# Test actors


class Counter(Actor[Increment | Decrement | GetValue | SetValue | Reset]):
    """Simple counter actor for testing."""

    def __init__(self, initial: int = 0):
        self.count = initial
        self.started = False
        self.stopped = False

    async def on_start(self) -> None:
        self.started = True

    async def on_stop(self) -> None:
        self.stopped = True

    async def receive(
        self, msg: Increment | Decrement | GetValue | SetValue | Reset, ctx: Context
    ) -> None:
        match msg:
            case Increment(amount):
                self.count += amount
            case Decrement(amount):
                self.count -= amount
            case GetValue():
                await ctx.reply(self.count)
            case SetValue(value):
                self.count = value
            case Reset():
                self.count = 0


class EchoActor(Actor[Any]):
    """Actor that echoes back any message."""

    async def receive(self, msg: Any, ctx: Context) -> None:
        await ctx.reply(msg)


class ForwarderActor(Actor[ForwardTo | Any]):
    """Actor that forwards messages to other actors."""

    async def receive(self, msg: ForwardTo | Any, ctx: Context) -> None:
        match msg:
            case ForwardTo(target, message):
                await target.send(message)
            case _:
                await ctx.reply(msg)


class FailingActor(Actor[FailNow | GetValue]):
    """Actor that fails on command."""

    def __init__(self):
        self.failure_count = 0
        self.success_count = 0

    async def receive(self, msg: FailNow | GetValue, ctx: Context) -> None:
        match msg:
            case FailNow(error_message):
                self.failure_count += 1
                raise RuntimeError(error_message)
            case GetValue():
                self.success_count += 1
                await ctx.reply(self.failure_count)


@supervised(strategy=SupervisionStrategy.RESTART, max_restarts=5)
class SupervisedCounter(Counter):
    """Counter with supervision configured."""

    def __init__(self, initial: int = 0):
        super().__init__(initial)
        self.restart_count = 0

    async def post_restart(self, exc: Exception) -> None:
        await super().post_restart(exc)
        self.restart_count += 1


class ParentActor(Actor[SpawnChild | GetChildren | GetValue]):
    """Actor that can spawn children."""

    def __init__(self):
        self.spawn_count = 0

    async def receive(
        self, msg: SpawnChild | GetChildren | GetValue, ctx: Context
    ) -> None:
        match msg:
            case SpawnChild(name):
                child = await ctx.spawn(Counter, name=name)
                self.spawn_count += 1
                await ctx.reply(child)
            case GetChildren():
                await ctx.reply(list(ctx.children.keys()))
            case GetValue():
                await ctx.reply(self.spawn_count)


# Fixtures


@pytest.fixture
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def system():
    """Create and cleanup an ActorSystem."""
    async with ActorSystem.local() as sys:
        yield sys
