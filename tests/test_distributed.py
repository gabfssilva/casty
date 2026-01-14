"""Tests for DistributedActorSystem."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, ActorSystem, Context
from casty.cluster.control_plane import RaftConfig


@dataclass
class Ping:
    pass


@dataclass
class GetValue:
    pass


class Counter(Actor[Ping | GetValue]):
    """Simple counter actor for testing."""

    def __init__(self, initial: int = 0):
        self.count = initial

    async def receive(self, msg: Ping | GetValue, ctx: Context) -> None:
        match msg:
            case Ping():
                self.count += 1
            case GetValue():
                ctx.reply(self.count)


class TestDistributedActorSystemBasic:
    """Basic tests for DistributedActorSystem."""

    def test_creation(self):
        """Test creating a distributed system."""
        system = ActorSystem.distributed("127.0.0.1", 0)
        assert system.node_id is not None
        assert "127.0.0.1" in system.node_id or system.node_id.startswith("127.0.0.1")

    def test_custom_node_id(self):
        """Test creating with custom node ID."""
        system = ActorSystem.distributed(
            "127.0.0.1",
            0,
            node_id="my-custom-node",
        )
        assert system.node_id == "my-custom-node"

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Test starting and stopping the system."""
        system = ActorSystem.distributed("127.0.0.1", 0)

        await system.start()
        assert system._running is True

        await system.shutdown()
        assert system._running is False

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager."""
        async with ActorSystem.distributed("127.0.0.1", 0) as system:
            assert system._running is True

        assert system._running is False


class TestDistributedActorSystemActors:
    """Tests for actor spawning in distributed system."""

    @pytest.mark.asyncio
    async def test_spawn_local_actor(self):
        """Test spawning a local actor."""
        async with ActorSystem.distributed("127.0.0.1", 0) as system:
            counter = await system.spawn(Counter)

            await counter.send(Ping())
            await asyncio.sleep(0.05)

            result = await counter.ask(GetValue())
            assert result == 1

    @pytest.mark.asyncio
    async def test_spawn_named_actor(self):
        """Test spawning a named actor."""
        async with ActorSystem.distributed("127.0.0.1", 0) as system:
            counter = await system.spawn(Counter, name="my-counter")

            await counter.send(Ping())
            await asyncio.sleep(0.05)

            # Look up by name
            found = await system.lookup("my-counter")
            assert found is not None
            result = await found.ask(GetValue())
            assert result == 1


class TestDistributedActorSystemSingletons:
    """Tests for singleton actors."""

    @pytest.mark.asyncio
    async def test_spawn_singleton(self):
        """Test spawning a singleton actor."""
        async with ActorSystem.distributed("127.0.0.1", 0) as system:
            # Wait for leader election
            await asyncio.sleep(0.5)

            counter = await system.spawn(
                Counter, name="singleton-counter", singleton=True
            )

            await counter.send(Ping())
            await asyncio.sleep(0.05)

            result = await counter.ask(GetValue())
            assert result == 1

    @pytest.mark.asyncio
    async def test_singleton_requires_name(self):
        """Test that singleton requires a name."""
        async with ActorSystem.distributed("127.0.0.1", 0) as system:
            with pytest.raises(ValueError, match="require"):
                await system.spawn(Counter, singleton=True)

    @pytest.mark.asyncio
    async def test_singleton_or_sharded_not_both(self):
        """Test that actor can't be both singleton and sharded."""
        async with ActorSystem.distributed("127.0.0.1", 0) as system:
            with pytest.raises(ValueError, match="both"):
                await system.spawn(
                    Counter, name="test", singleton=True, sharded=True
                )


class TestDistributedActorSystemSharded:
    """Tests for sharded actors."""

    @pytest.mark.asyncio
    async def test_spawn_sharded(self):
        """Test spawning a sharded entity type."""

        class ShardedCounter(Actor[Ping | GetValue]):
            def __init__(self, entity_id: str):
                self.entity_id = entity_id
                self.count = 0

            async def receive(self, msg: Ping | GetValue, ctx: Context) -> None:
                match msg:
                    case Ping():
                        self.count += 1
                    case GetValue():
                        ctx.reply(self.count)

        async with ActorSystem.distributed("127.0.0.1", 0) as system:
            # Wait for leader election
            await asyncio.sleep(0.5)

            counters = await system.spawn(ShardedCounter, name="counters", sharded=True)

            # Send to specific entity
            await counters["entity-1"].send(Ping())
            await asyncio.sleep(0.05)

            result = await counters["entity-1"].ask(GetValue())
            assert result == 1


class TestDistributedActorSystemProperties:
    """Tests for system properties."""

    @pytest.mark.asyncio
    async def test_is_leader_single_node(self):
        """Test that single node becomes leader."""
        # Use fast election timeout for test
        fast_config = RaftConfig(
            heartbeat_interval=0.05,
            election_timeout_min=0.1,
            election_timeout_max=0.2,
        )
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=fast_config
        ) as system:
            # Wait for election (timeout is 0.1-0.2s)
            await asyncio.sleep(0.3)
            assert system.is_leader is True

    @pytest.mark.asyncio
    async def test_cluster_state(self):
        """Test accessing cluster state."""
        async with ActorSystem.distributed("127.0.0.1", 0) as system:
            state = system.cluster_state
            assert state is not None

    def test_repr(self):
        """Test string representation."""
        system = ActorSystem.distributed("127.0.0.1", 8001, node_id="test-node")
        r = repr(system)
        assert "DistributedActorSystem" in r
        assert "test-node" in r
