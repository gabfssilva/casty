"""Tests for ClusteredActorSystem."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import ActorSystem, Actor, Context


# Test messages
@dataclass(frozen=True)
class Ping:
    value: int


@dataclass(frozen=True)
class Pong:
    value: int


# Test actor
class EchoActor(Actor[Ping]):
    """Simple echo actor for testing."""

    async def receive(self, msg: Ping, ctx: Context) -> None:
        ctx.reply(Pong(msg.value))


class TestClusteredFactoryMethod:
    """Test ActorSystem.clustered() factory method."""

    @pytest.mark.asyncio
    async def test_clustered_creates_system(self):
        """ActorSystem.clustered() should create a ClusteredActorSystem."""
        from casty.cluster.clustered_system import ClusteredActorSystem

        system = ActorSystem.clustered()
        assert isinstance(system, ClusteredActorSystem)

    @pytest.mark.asyncio
    async def test_clustered_with_parameters(self):
        """ActorSystem.clustered() should accept configuration parameters."""
        system = ActorSystem.clustered(
            host="127.0.0.1",
            port=9000,
            node_id="test-node",
        )
        assert system._cluster_config.bind_host == "127.0.0.1"
        assert system._cluster_config.bind_port == 9000
        assert system._cluster_config.node_id == "test-node"

    @pytest.mark.asyncio
    async def test_clustered_with_seeds(self):
        """ActorSystem.clustered() should accept seed nodes."""
        system = ActorSystem.clustered(
            seeds=["192.168.1.10:7946", "192.168.1.11:7946"],
        )
        assert len(system._cluster_config.seeds) == 2


class TestClusteredActorSystem:
    """Test ClusteredActorSystem functionality."""

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """ClusteredActorSystem should work as context manager."""
        async with ActorSystem.clustered() as system:
            assert system._running is True
            assert system._cluster is not None

    @pytest.mark.asyncio
    async def test_spawn_named_actor_registers(self):
        """Spawning named actor should register in cluster."""
        async with ActorSystem.clustered() as system:
            # Wait for cluster to be ready
            await asyncio.sleep(0.2)

            # Spawn named actor
            echo = await system.spawn(EchoActor, name="echo-1")

            # Give time for registration
            await asyncio.sleep(0.1)

            # Actor should be registered
            members = await system.get_members()
            assert "echo-1" in members

    @pytest.mark.asyncio
    async def test_spawn_unnamed_actor_does_not_register(self):
        """Spawning unnamed actor should NOT register in cluster."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            # Spawn unnamed actor
            echo = await system.spawn(EchoActor)

            await asyncio.sleep(0.1)

            # Actor should NOT be registered
            members = await system.get_members()
            # Only cluster-internal actors might be there, not our unnamed one
            assert len([k for k in members.keys() if k.startswith("echo")]) == 0

    @pytest.mark.asyncio
    async def test_spawn_with_cluster_false(self):
        """Spawning with cluster=False should not register even if named."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            # Spawn named actor but explicitly disable cluster registration
            echo = await system.spawn(EchoActor, name="echo-local", cluster=False)

            await asyncio.sleep(0.1)

            members = await system.get_members()
            assert "echo-local" not in members

    @pytest.mark.asyncio
    async def test_get_ref_local_actor(self):
        """get_ref should return reference to local actor."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            # Spawn and register actor
            echo = await system.spawn(EchoActor, name="echo-test")
            await asyncio.sleep(0.1)

            # Get reference
            ref = await system.lookup("echo-test")

            # Should get a reference (could be local or remote wrapper)
            assert ref is not None

    @pytest.mark.asyncio
    async def test_get_ref_unknown_actor(self):
        """get_ref should return None for unknown actor."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            ref = await system.lookup("nonexistent-actor")
            assert ref is None

    @pytest.mark.asyncio
    async def test_unregister_actor(self):
        """unregister should remove actor from cluster."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            # Register actor
            echo = await system.spawn(EchoActor, name="echo-unreg")
            await asyncio.sleep(0.1)

            # Verify registered
            members = await system.get_members()
            assert "echo-unreg" in members

            # Unregister
            await system.unregister("echo-unreg")
            await asyncio.sleep(0.1)

            # Verify unregistered
            members = await system.get_members()
            assert "echo-unreg" not in members

    @pytest.mark.asyncio
    async def test_node_id_property(self):
        """node_id property should return configured node ID."""
        async with ActorSystem.clustered(node_id="my-node") as system:
            assert system.node_id == "my-node"

    @pytest.mark.asyncio
    async def test_cluster_property(self):
        """cluster property should return Cluster actor reference."""
        async with ActorSystem.clustered() as system:
            assert system.cluster is not None

    @pytest.mark.asyncio
    async def test_wait_ready(self):
        """wait_ready should return True when cluster is ready."""
        async with ActorSystem.clustered() as system:
            ready = await system.wait_ready(timeout=5.0)
            assert ready is True
