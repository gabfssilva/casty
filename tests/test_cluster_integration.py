"""Integration tests for Cluster actor."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import ActorSystem, Actor, Context
from casty.cluster import (
    Cluster,
    ClusterConfig,
    LocalRegister,
    GetLocalActors,
    RemoteRef,
)


# Test messages
@dataclass(frozen=True)
class Ping:
    value: int


@dataclass(frozen=True)
class Pong:
    value: int


@dataclass(frozen=True)
class GetValue:
    pass


# Test actor
class EchoActor(Actor[Ping | GetValue]):
    """Simple echo actor for testing."""

    def __init__(self):
        self.last_value = 0

    async def receive(self, msg: Ping | GetValue, ctx: Context) -> None:
        match msg:
            case Ping(value):
                self.last_value = value
                ctx.reply(Pong(value))
            case GetValue():
                ctx.reply(self.last_value)


class TestClusterBasic:
    """Basic Cluster actor tests."""

    @pytest.mark.asyncio
    async def test_cluster_starts_and_binds(self):
        """Cluster should start and bind to a port."""
        async with ActorSystem.clustered() as system:
            # Give it time to bind
            await asyncio.sleep(0.2)

            # Cluster should be running
            assert system.cluster is not None

    @pytest.mark.asyncio
    async def test_local_actor_registration(self):
        """Should be able to register local actors."""
        async with ActorSystem.clustered() as system:
            cluster = system.cluster

            # Spawn a local actor
            echo = await system.spawn(EchoActor)

            # Register it with the cluster
            await cluster.send(LocalRegister("echo-1", echo))

            # Query local actors
            local_actors = await cluster.ask(GetLocalActors())

            assert "echo-1" in local_actors
            assert local_actors["echo-1"] == echo

    @pytest.mark.asyncio
    async def test_multiple_local_actor_registration(self):
        """Should be able to register multiple local actors."""
        async with ActorSystem.clustered() as system:
            cluster = system.cluster

            # Spawn multiple actors
            echo1 = await system.spawn(EchoActor)
            echo2 = await system.spawn(EchoActor)
            echo3 = await system.spawn(EchoActor)

            # Register them
            await cluster.send(LocalRegister("echo-1", echo1))
            await cluster.send(LocalRegister("echo-2", echo2))
            await cluster.send(LocalRegister("echo-3", echo3))

            # Query local actors
            local_actors = await cluster.ask(GetLocalActors())

            assert len(local_actors) == 3
            assert "echo-1" in local_actors
            assert "echo-2" in local_actors
            assert "echo-3" in local_actors


class TestRemoteRef:
    """Tests for RemoteRef class."""

    def test_remote_ref_equality(self):
        """RemoteRefs with same actor/node should be equal."""
        ref1 = RemoteRef("worker-1", "node-1", None)
        ref2 = RemoteRef("worker-1", "node-1", None)
        ref3 = RemoteRef("worker-2", "node-1", None)
        ref4 = RemoteRef("worker-1", "node-2", None)

        assert ref1 == ref2
        assert ref1 != ref3
        assert ref1 != ref4

    def test_remote_ref_hash(self):
        """RemoteRefs should be hashable."""
        ref1 = RemoteRef("worker-1", "node-1", None)
        ref2 = RemoteRef("worker-1", "node-1", None)
        ref3 = RemoteRef("worker-2", "node-1", None)

        # Same refs should have same hash
        assert hash(ref1) == hash(ref2)

        # Can be used in sets
        refs = {ref1, ref2, ref3}
        assert len(refs) == 2

    def test_remote_ref_repr(self):
        """RemoteRef should have readable repr."""
        ref = RemoteRef("worker-1", "node-abc", None)

        assert "worker-1" in repr(ref)
        assert "node-abc" in repr(ref)


class TestClusterConfig:
    """Tests for ClusterConfig."""

    def test_development_config(self):
        """Development config should have fast timeouts."""
        config = ClusterConfig.development()

        assert config.bind_host == "127.0.0.1"
        assert config.connect_timeout < 5.0
        assert config.handshake_timeout < 5.0

    def test_production_config(self):
        """Production config should have standard settings."""
        config = ClusterConfig.production()

        assert config.bind_host == "0.0.0.0"
        assert config.bind_port == 7946

    def test_config_with_bind_address(self):
        """Should be able to change bind address."""
        config = ClusterConfig.development()
        config = config.with_bind_address("0.0.0.0", 9000)

        assert config.bind_host == "0.0.0.0"
        assert config.bind_port == 9000

    def test_config_with_seeds(self):
        """Should be able to set seeds."""
        config = ClusterConfig.development()
        config = config.with_seeds(["192.168.1.10:7946", "192.168.1.11:7946"])

        assert len(config.seeds) == 2
        assert "192.168.1.10:7946" in config.seeds

    def test_config_with_node_id(self):
        """Should be able to set node ID."""
        config = ClusterConfig.development()
        config = config.with_node_id("my-custom-node")

        assert config.node_id == "my-custom-node"
