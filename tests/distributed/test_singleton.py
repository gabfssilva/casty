"""Tests for singleton actors in distributed clusters."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, Context, DistributedActorSystem
from casty.cluster.remote import RemoteRef

from .helpers import wait_for_leader


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


class TestSingletonBasic:
    """Basic singleton functionality tests."""

    async def test_spawn_singleton_creates_actor(self, get_port) -> None:
        """Singleton spawn creates the actor locally."""
        system = DistributedActorSystem("127.0.0.1", get_port())
        async with system:
            ref = await system.spawn(Counter, name="counter", singleton=True, initial=10)

            # Actor exists locally
            assert "counter" in system._cluster.singleton_local_refs
            assert ref.id.name == "counter"

            # Can interact with it
            value = await ref.ask(GetValue())
            assert value == 10

    async def test_spawn_singleton_requires_name(self, get_port) -> None:
        """Singleton spawn requires a name."""
        system = DistributedActorSystem("127.0.0.1", get_port())
        async with system:
            with pytest.raises(ValueError, match="must have a name"):
                await system.spawn(Counter, singleton=True)

    async def test_lookup_singleton(self, get_port) -> None:
        """Can lookup singleton by name."""
        system = DistributedActorSystem("127.0.0.1", get_port())
        async with system:
            original = await system.spawn(Counter, name="counter", singleton=True, initial=42)

            found = await system.lookup("counter", singleton=True)

            assert found.id.name == original.id.name
            value = await found.ask(GetValue())
            assert value == 42

    async def test_lookup_singleton_not_found(self, get_port) -> None:
        """Lookup raises KeyError for nonexistent singleton."""
        system = DistributedActorSystem("127.0.0.1", get_port())
        async with system:
            with pytest.raises(KeyError, match="not found"):
                await system.lookup("nonexistent", singleton=True)


class TestSingletonTwoNodes:
    """Singleton tests with two-node cluster."""

    async def test_singleton_returns_remote_ref_on_second_node(self, get_port) -> None:
        """Second node gets RemoteRef to existing singleton."""
        port1 = get_port()
        port2 = get_port()

        node1 = DistributedActorSystem(
            "127.0.0.1", port1,
            seeds=[],
            expected_cluster_size=2,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1", port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
        )

        async with node1, node2:
            # Wait for leader election
            leader = await wait_for_leader([node1, node2], timeout=5.0)
            follower = node2 if leader is node1 else node1

            # Create singleton on leader
            ref1 = await leader.spawn(Counter, name="counter", singleton=True, initial=100)

            # Wait for Raft replication
            await asyncio.sleep(0.5)

            # Try to create same singleton on follower - should get RemoteRef
            ref2 = await follower.spawn(Counter, name="counter", singleton=True, initial=200)

            # ref2 should be a RemoteRef, not a local actor
            assert isinstance(ref2, RemoteRef)

            # Both refs should point to the same actor with value 100
            value1 = await ref1.ask(GetValue())
            value2 = await ref2.ask(GetValue())
            assert value1 == 100
            assert value2 == 100

    async def test_singleton_lookup_from_second_node(self, get_port) -> None:
        """Second node can lookup singleton on first node."""
        port1 = get_port()
        port2 = get_port()

        node1 = DistributedActorSystem(
            "127.0.0.1", port1,
            seeds=[],
            expected_cluster_size=2,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1", port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
        )

        async with node1, node2:
            # Wait for leader election
            leader = await wait_for_leader([node1, node2], timeout=5.0)
            follower = node2 if leader is node1 else node1

            # Create singleton on leader
            await leader.spawn(Counter, name="counter", singleton=True, initial=50)

            # Wait for Raft replication
            await asyncio.sleep(0.5)

            # Lookup from follower
            ref = await follower.lookup("counter", singleton=True)

            assert isinstance(ref, RemoteRef)
            value = await ref.ask(GetValue())
            assert value == 50

    async def test_singleton_mutation_visible_everywhere(self, get_port) -> None:
        """Mutations to singleton are visible from all nodes."""
        port1 = get_port()
        port2 = get_port()

        node1 = DistributedActorSystem(
            "127.0.0.1", port1,
            seeds=[],
            expected_cluster_size=2,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1", port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
        )

        async with node1, node2:
            # Wait for leader election
            leader = await wait_for_leader([node1, node2], timeout=5.0)
            follower = node2 if leader is node1 else node1

            # Create singleton on leader
            ref1 = await leader.spawn(Counter, name="counter", singleton=True, initial=0)

            await asyncio.sleep(0.5)

            # Get reference from follower
            ref2 = await follower.lookup("counter", singleton=True)

            # Mutate via leader
            await ref1.send(Increment(10))
            await asyncio.sleep(0.1)

            # Read from follower
            value = await ref2.ask(GetValue())
            assert value == 10

            # Mutate via follower (via RemoteRef)
            await ref2.send(Increment(5))
            await asyncio.sleep(0.1)

            # Read from leader
            value = await ref1.ask(GetValue())
            assert value == 15


class TestSingletonRegistry:
    """Tests for singleton registry state."""

    async def test_singleton_registry_updated_on_spawn(self, get_port) -> None:
        """Singleton registry is updated when singleton is spawned."""
        port1 = get_port()
        port2 = get_port()

        node1 = DistributedActorSystem(
            "127.0.0.1", port1,
            seeds=[],
            expected_cluster_size=2,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1", port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
        )

        async with node1, node2:
            # Wait for leader election
            leader = await wait_for_leader([node1, node2], timeout=5.0)

            # Create singleton on leader
            await leader.spawn(Counter, name="counter", singleton=True)

            # Wait for Raft replication
            await asyncio.sleep(0.5)

            # Both nodes should have registry entry
            assert "counter" in node1._cluster.singleton_registry
            assert "counter" in node2._cluster.singleton_registry

            # Check entry details
            entry = leader._cluster.singleton_registry["counter"]
            assert entry.name == "counter"
            assert entry.node_id == leader.node_id
            assert entry.status == "active"

    async def test_local_and_singleton_registries_are_separate(self, get_port) -> None:
        """Local registry and singleton registry are independent."""
        system = DistributedActorSystem("127.0.0.1", get_port())
        async with system:
            # Create local actor
            await system.spawn(Counter, name="local-counter")

            # Create singleton
            await system.spawn(Counter, name="singleton-counter", singleton=True)

            # They're in different registries
            assert "local-counter" in system._cluster.registry
            assert "local-counter" not in system._cluster.singleton_registry

            assert "singleton-counter" not in system._cluster.registry
            assert "singleton-counter" in system._cluster.singleton_local_refs
