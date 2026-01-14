"""Tests for singleton actors in distributed system."""

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty import Actor, ActorSystem, Context
from casty.cluster.control_plane import RaftConfig


# Fast config for tests
FAST_RAFT_CONFIG = RaftConfig(
    heartbeat_interval=0.05,
    election_timeout_min=0.1,
    election_timeout_max=0.2,
)


@dataclass
class GetState:
    """Message to get actor state."""
    pass


@dataclass
class Increment:
    """Message to increment counter."""
    amount: int = 1


class SingletonCounter(Actor[GetState | Increment]):
    """A singleton counter actor for testing."""

    def __init__(self):
        self.count = 0
        self.node_id: str | None = None

    async def receive(self, msg: GetState | Increment, ctx: Context) -> None:
        match msg:
            case GetState():
                ctx.reply({
                    "count": self.count,
                    "node_id": self.node_id,
                })
            case Increment(amount):
                self.count += amount


class TestSingletonBasic:
    """Basic singleton tests."""

    @pytest.mark.asyncio
    async def test_singleton_spawn_returns_ref(self):
        """Test that spawning a singleton returns an ActorRef."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)  # Wait for election

            ref = await system.spawn(
                SingletonCounter,
                name="counter-singleton",
                singleton=True,
            )

            assert ref is not None
            assert hasattr(ref, "send")
            assert hasattr(ref, "ask")

    @pytest.mark.asyncio
    async def test_singleton_receives_messages(self):
        """Test that singleton can receive and process messages."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            ref = await system.spawn(
                SingletonCounter,
                name="counter",
                singleton=True,
            )

            # Send messages
            await ref.send(Increment(5))
            await ref.send(Increment(3))
            await asyncio.sleep(0.1)

            # Ask for state
            state = await ref.ask(GetState())
            assert state["count"] == 8

    @pytest.mark.asyncio
    async def test_singleton_requires_name(self):
        """Test that singleton without name raises error."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            with pytest.raises(ValueError, match="require"):
                await system.spawn(SingletonCounter, singleton=True)

    @pytest.mark.asyncio
    async def test_singleton_cannot_be_sharded(self):
        """Test that singleton and sharded are mutually exclusive."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            with pytest.raises(ValueError, match="both"):
                await system.spawn(
                    SingletonCounter,
                    name="counter",
                    singleton=True,
                    sharded=True,
                )


class TestSingletonLookup:
    """Tests for singleton lookup."""

    @pytest.mark.asyncio
    async def test_singleton_can_be_looked_up(self):
        """Test that singleton can be found by name."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            # Spawn singleton
            original = await system.spawn(
                SingletonCounter,
                name="lookup-counter",
                singleton=True,
            )

            # Send a message
            await original.send(Increment(10))
            await asyncio.sleep(0.05)

            # Look it up
            found = await system.lookup("lookup-counter")

            assert found is not None
            state = await found.ask(GetState())
            assert state["count"] == 10

    @pytest.mark.asyncio
    async def test_lookup_nonexistent_singleton(self):
        """Test that looking up nonexistent singleton returns None."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            found = await system.lookup("nonexistent-singleton")
            assert found is None


class TestSingletonRegistry:
    """Tests for singleton registration in cluster state."""

    @pytest.mark.asyncio
    async def test_singleton_registered_in_cluster_state(self):
        """Test that singleton is registered in cluster state."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            await system.spawn(
                SingletonCounter,
                name="registered-counter",
                singleton=True,
            )

            # Give time for Raft to commit
            await asyncio.sleep(0.2)

            # Check cluster state
            state = system.cluster_state
            singleton_info = state.get_singleton("registered-counter")

            # May be None if we're not leader
            if system.is_leader:
                assert singleton_info is not None
                assert singleton_info.name == "registered-counter"
                assert singleton_info.node_id == system.node_id


class TestSingletonUniqueness:
    """Tests for singleton uniqueness guarantees."""

    @pytest.mark.asyncio
    async def test_same_singleton_returns_same_ref(self):
        """Test that spawning same singleton twice returns same ref."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            ref1 = await system.spawn(
                SingletonCounter,
                name="unique-counter",
                singleton=True,
            )

            # Send increment
            await ref1.send(Increment(5))
            await asyncio.sleep(0.05)

            # Try to spawn again - should get same instance
            ref2 = await system.spawn(
                SingletonCounter,
                name="unique-counter",
                singleton=True,
            )

            # Both refs should point to same actor with same state
            state1 = await ref1.ask(GetState())
            state2 = await ref2.ask(GetState())

            assert state1["count"] == state2["count"] == 5


class TestMultipleSingletons:
    """Tests for multiple singleton actors."""

    @pytest.mark.asyncio
    async def test_multiple_different_singletons(self):
        """Test that different singletons work independently."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            # Create two different singletons
            counter1 = await system.spawn(
                SingletonCounter,
                name="counter-a",
                singleton=True,
            )
            counter2 = await system.spawn(
                SingletonCounter,
                name="counter-b",
                singleton=True,
            )

            # Increment each differently
            await counter1.send(Increment(10))
            await counter2.send(Increment(20))
            await asyncio.sleep(0.1)

            # Verify independence
            state1 = await counter1.ask(GetState())
            state2 = await counter2.ask(GetState())

            assert state1["count"] == 10
            assert state2["count"] == 20

    @pytest.mark.asyncio
    async def test_singletons_tracked_separately(self):
        """Test that singletons are tracked separately in state."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            await system.spawn(
                SingletonCounter,
                name="tracked-a",
                singleton=True,
            )
            await system.spawn(
                SingletonCounter,
                name="tracked-b",
                singleton=True,
            )

            await asyncio.sleep(0.2)

            # Both should be lookupable
            found_a = await system.lookup("tracked-a")
            found_b = await system.lookup("tracked-b")

            assert found_a is not None
            assert found_b is not None


class TestSingletonWithConstructorArgs:
    """Tests for singletons with constructor arguments."""

    @pytest.mark.asyncio
    async def test_singleton_with_initial_value(self):
        """Test singleton with constructor argument."""

        class CounterWithInitial(Actor[GetState | Increment]):
            def __init__(self, initial: int = 0):
                self.count = initial

            async def receive(self, msg: GetState | Increment, ctx: Context) -> None:
                match msg:
                    case GetState():
                        ctx.reply({"count": self.count})
                    case Increment(amount):
                        self.count += amount

        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            ref = await system.spawn(
                CounterWithInitial,
                name="initial-counter",
                singleton=True,
                initial=100,
            )

            state = await ref.ask(GetState())
            assert state["count"] == 100

            await ref.send(Increment(50))
            await asyncio.sleep(0.05)

            state = await ref.ask(GetState())
            assert state["count"] == 150
