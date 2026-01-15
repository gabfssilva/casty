"""Tests for shard consistency levels.

Tests the ADT consistency levels for sharded actor registration:
- Eventual: Fire-and-forget (default)
- Strong: Wait for all nodes
- Quorum: Wait for majority
- AtLeast(n): Wait for at least n nodes
"""

import asyncio
import pytest
from dataclasses import dataclass

from casty import Actor, Context, ActorSystem
from casty.cluster import (
    ShardConsistency,
    Strong,
    Eventual,
    Quorum,
    AtLeast,
    DevelopmentCluster,
)


# =============================================================================
# Test Actor
# =============================================================================


@dataclass
class SetValue:
    value: int


@dataclass
class GetValue:
    pass


class ValueActor(Actor):
    """Simple actor that stores a value."""

    def __init__(self, entity_id: str = "") -> None:
        self.entity_id = entity_id
        self.value = 0

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            case SetValue(value):
                self.value = value
            case GetValue():
                ctx.reply(self.value)


# =============================================================================
# Unit Tests for Consistency ADT
# =============================================================================


class TestConsistencyADT:
    """Test the consistency level ADT behavior."""

    def test_eventual_should_not_wait(self):
        """Eventual consistency should not wait for acks."""
        eventual = Eventual()
        assert not eventual.should_wait()
        assert eventual.required_acks(5) == 0

    def test_strong_waits_for_all(self):
        """Strong consistency waits for all nodes."""
        strong = Strong()
        assert strong.should_wait()
        assert strong.required_acks(1) == 1
        assert strong.required_acks(3) == 3
        assert strong.required_acks(5) == 5

    def test_quorum_waits_for_majority(self):
        """Quorum waits for majority of nodes."""
        quorum = Quorum()
        assert quorum.should_wait()
        # n/2 + 1
        assert quorum.required_acks(1) == 1
        assert quorum.required_acks(2) == 2
        assert quorum.required_acks(3) == 2
        assert quorum.required_acks(4) == 3
        assert quorum.required_acks(5) == 3

    def test_at_least_waits_for_n(self):
        """AtLeast waits for specified number of nodes."""
        at_least_2 = AtLeast(2)
        assert at_least_2.should_wait()
        assert at_least_2.required_acks(5) == 2
        assert at_least_2.required_acks(2) == 2
        # Caps at total nodes
        assert at_least_2.required_acks(1) == 1

    def test_at_least_validates_count(self):
        """AtLeast requires count >= 1."""
        with pytest.raises(ValueError):
            AtLeast(0)
        with pytest.raises(ValueError):
            AtLeast(-1)

    def test_repr(self):
        """Test string representation."""
        assert repr(Strong()) == "Strong()"
        assert repr(Eventual()) == "Eventual()"
        assert repr(Quorum()) == "Quorum()"
        assert repr(AtLeast(3)) == "AtLeast(3)"


# =============================================================================
# Integration Tests
# =============================================================================


@pytest.fixture
def base_port():
    """Get a unique base port for each test."""
    import random
    return random.randint(20000, 30000)


class TestEventualConsistency:
    """Test Eventual consistency (default behavior)."""

    @pytest.mark.asyncio
    async def test_eventual_returns_immediately(self, base_port):
        """Eventual consistency should return immediately."""
        async with ActorSystem.clustered(
            host="127.0.0.1",
            port=base_port,
        ) as system:
            # Give cluster time to stabilize
            await asyncio.sleep(0.3)

            # Spawn with eventual consistency (default)
            start = asyncio.get_event_loop().time()
            accounts = await system.spawn(
                ValueActor,
                name="accounts",
                sharded=True,
                consistency=Eventual(),
            )
            elapsed = asyncio.get_event_loop().time() - start

            # Should return almost immediately
            assert elapsed < 0.5
            assert accounts is not None

            # Should still work
            await accounts["acc-1"].send(SetValue(100))
            await asyncio.sleep(0.1)
            result = await accounts["acc-1"].ask(GetValue())
            assert result == 100


class TestStrongConsistency:
    """Test Strong consistency across cluster."""

    @pytest.mark.asyncio
    async def test_strong_waits_for_all_nodes(self):
        """Strong consistency waits for all nodes to ack."""
        async with DevelopmentCluster(3) as systems:
            await asyncio.sleep(1.0)

            # Spawn with Strong consistency - should wait for all nodes
            accounts = await systems[0].spawn(
                ValueActor,
                name="accounts",
                sharded=True,
                consistency=Strong(),
            )

            assert accounts is not None

            # After Strong consistency returns, all nodes should have the type
            for i in range(10):
                await accounts[f"acc-{i}"].send(SetValue(i * 10))

            await asyncio.sleep(0.2)

            # Verify all entities work
            for i in range(10):
                result = await accounts[f"acc-{i}"].ask(GetValue())
                assert result == i * 10


class TestQuorumConsistency:
    """Test Quorum consistency."""

    @pytest.mark.asyncio
    async def test_quorum_waits_for_majority(self):
        """Quorum consistency waits for majority of nodes."""
        async with DevelopmentCluster(3) as systems:
            await asyncio.sleep(1.0)

            # Spawn with Quorum consistency - should wait for 2 of 3 nodes
            accounts = await systems[0].spawn(
                ValueActor,
                name="accounts",
                sharded=True,
                consistency=Quorum(),
            )

            assert accounts is not None

            # Test functionality
            await accounts["test-1"].send(SetValue(42))
            await asyncio.sleep(0.1)
            result = await accounts["test-1"].ask(GetValue())
            assert result == 42


class TestAtLeastConsistency:
    """Test AtLeast(n) consistency."""

    @pytest.mark.asyncio
    async def test_at_least_waits_for_n_nodes(self):
        """AtLeast(n) waits for specified number of nodes."""
        async with DevelopmentCluster(5) as systems:
            await asyncio.sleep(1.5)

            # Spawn with AtLeast(3) - should wait for at least 3 nodes
            accounts = await systems[0].spawn(
                ValueActor,
                name="accounts",
                sharded=True,
                consistency=AtLeast(3),
            )

            assert accounts is not None

            # Test functionality
            await accounts["entity-1"].send(SetValue(123))
            await asyncio.sleep(0.1)
            result = await accounts["entity-1"].ask(GetValue())
            assert result == 123


class TestConsistencyWithSingleNode:
    """Test consistency levels with single node cluster."""

    @pytest.mark.asyncio
    async def test_strong_works_with_single_node(self, base_port):
        """Strong consistency should work with single node (immediate return)."""
        async with ActorSystem.clustered(
            host="127.0.0.1",
            port=base_port,
        ) as system:
            await asyncio.sleep(0.3)

            # Should not hang - single node means 1 ack needed (self)
            accounts = await system.spawn(
                ValueActor,
                name="accounts",
                sharded=True,
                consistency=Strong(),
            )

            assert accounts is not None
            await accounts["test"].send(SetValue(99))
            await asyncio.sleep(0.1)
            result = await accounts["test"].ask(GetValue())
            assert result == 99

    @pytest.mark.asyncio
    async def test_quorum_works_with_single_node(self, base_port):
        """Quorum should work with single node."""
        async with ActorSystem.clustered(
            host="127.0.0.1",
            port=base_port,
        ) as system:
            await asyncio.sleep(0.3)

            accounts = await system.spawn(
                ValueActor,
                name="accounts",
                sharded=True,
                consistency=Quorum(),
            )

            assert accounts is not None
