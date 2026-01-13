"""Tests for actor registry and lookup."""

from dataclasses import dataclass

import pytest

from casty import Actor, Context, DistributedActorSystem


@dataclass
class Increment:
    amount: int = 1


@dataclass
class GetValue:
    pass


class Counter(Actor[Increment | GetValue]):
    """Simple counter actor for testing."""

    def __init__(self, initial: int = 0) -> None:
        self.value = initial

    async def receive(self, msg: Increment | GetValue, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.value += amount
            case GetValue():
                ctx.reply(self.value)


pytestmark = pytest.mark.slow


class TestNamedActors:
    """Tests for named actor registration."""

    async def test_spawn_named_actor_registers(self, get_port) -> None:
        system = DistributedActorSystem("127.0.0.1", get_port())
        async with system:
            await system.spawn(Counter, name="counter")
            assert "counter" in system._cluster.registry

    async def test_spawn_unnamed_actor_not_registered(self, get_port) -> None:
        system = DistributedActorSystem("127.0.0.1", get_port())
        async with system:
            await system.spawn(Counter)
            assert len(system._cluster.registry) == 0


class TestLocalLookup:
    """Tests for local actor lookup."""

    async def test_lookup_local_actor(self, get_port) -> None:
        system = DistributedActorSystem("127.0.0.1", get_port())
        async with system:
            original = await system.spawn(Counter, name="counter", initial=42)

            found = await system.lookup("counter")

            assert found.id == original.id
            value = await found.ask(GetValue())
            assert value == 42

    async def test_lookup_not_found(self, get_port) -> None:
        system = DistributedActorSystem("127.0.0.1", get_port())
        async with system:
            with pytest.raises(KeyError, match="not found"):
                await system.lookup("nonexistent")
