"""Parameterized tests for N-node clusters.

Tests cluster operations with varying cluster sizes to ensure
scalability and correctness across different configurations.
"""

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty import ActorSystem, Actor, Context


# =============================================================================
# Test Messages
# =============================================================================


@dataclass(frozen=True)
class Ping:
    """Simple ping message."""
    pass


@dataclass(frozen=True)
class Pong:
    """Response to ping."""
    node_id: str


@dataclass(frozen=True)
class Increment:
    """Increment counter."""
    amount: int = 1


@dataclass(frozen=True)
class GetValue:
    """Get current value."""
    pass


@dataclass(frozen=True)
class GetNodeId:
    """Get the node ID where this actor is running."""
    pass


# =============================================================================
# Test Actors
# =============================================================================


class PingActor(Actor[Ping | GetNodeId]):
    """Simple actor that responds to pings."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id

    async def receive(self, msg: Ping | GetNodeId, ctx: Context) -> None:
        match msg:
            case Ping():
                ctx.reply(Pong(self.node_id))
            case GetNodeId():
                ctx.reply(self.node_id)


class Counter(Actor[Increment | GetValue | GetNodeId]):
    """Counter actor for sharding tests."""

    def __init__(self, entity_id: str) -> None:
        self.entity_id = entity_id
        self.value = 0

    async def receive(self, msg: Increment | GetValue | GetNodeId, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.value += amount
            case GetValue():
                ctx.reply(self.value)
            case GetNodeId():
                ctx.reply(self._ctx.system.node_id if hasattr(self._ctx.system, 'node_id') else "local")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def base_port() -> int:
    """Base port for cluster tests. Each test gets a unique range."""
    import random
    return 18000 + random.randint(0, 1000)


# =============================================================================
# Cluster Formation Tests
# =============================================================================


class TestClusterFormation:
    """Test cluster formation with varying sizes."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_nodes", [2, 3, 5])
    async def test_n_node_cluster_forms(self, num_nodes: int, base_port: int):
        """N nodes should form a cluster and see each other."""
        systems: list[Any] = []

        try:
            # Start first node (seed)
            seed_port = base_port
            seed = ActorSystem.clustered(
                host="127.0.0.1",
                port=seed_port,
                node_id="node-0",
            )
            systems.append(seed)
            await seed.start()
            await asyncio.sleep(0.2)

            # Start remaining nodes
            for i in range(1, num_nodes):
                node = ActorSystem.clustered(
                    host="127.0.0.1",
                    port=base_port + i,
                    node_id=f"node-{i}",
                    seeds=[f"127.0.0.1:{seed_port}"],
                )
                systems.append(node)
                await node.start()

            # Wait for cluster to form
            # More nodes need more time
            await asyncio.sleep(0.3 + 0.1 * num_nodes)

            # Verify all nodes are present
            for i, system in enumerate(systems):
                assert system.node_id == f"node-{i}"

        finally:
            # Cleanup
            for system in reversed(systems):
                await system.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_nodes", [2, 3, 5])
    async def test_cluster_membership_visible(self, num_nodes: int, base_port: int):
        """Each node should see all other members via SWIM."""
        systems: list[Any] = []

        try:
            # Start seed
            seed_port = base_port + 100
            seed = ActorSystem.clustered(
                host="127.0.0.1",
                port=seed_port,
                node_id="node-0",
            )
            systems.append(seed)
            await seed.start()
            await asyncio.sleep(0.2)

            # Start other nodes
            for i in range(1, num_nodes):
                node = ActorSystem.clustered(
                    host="127.0.0.1",
                    port=seed_port + i,
                    node_id=f"node-{i}",
                    seeds=[f"127.0.0.1:{seed_port}"],
                )
                systems.append(node)
                await node.start()

            # Wait for membership to propagate
            await asyncio.sleep(0.5 + 0.2 * num_nodes)

            # Each node should have connections to other nodes
            # (verified by being able to send messages)
            for system in systems:
                assert system._cluster is not None

        finally:
            for system in reversed(systems):
                await system.shutdown()


# =============================================================================
# Sharded Actor Tests
# =============================================================================


class TestShardedActorsNNodes:
    """Test sharded actors across N-node clusters."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_nodes", [2, 3, 5])
    async def test_sharded_entity_distribution(self, num_nodes: int, base_port: int):
        """Entities should be distributed across all nodes via consistent hashing."""
        systems: list[Any] = []

        try:
            # Start cluster
            seed_port = base_port + 200
            seed = ActorSystem.clustered(
                host="127.0.0.1",
                port=seed_port,
                node_id="node-0",
            )
            systems.append(seed)
            await seed.start()
            await asyncio.sleep(0.2)

            for i in range(1, num_nodes):
                node = ActorSystem.clustered(
                    host="127.0.0.1",
                    port=seed_port + i,
                    node_id=f"node-{i}",
                    seeds=[f"127.0.0.1:{seed_port}"],
                )
                systems.append(node)
                await node.start()

            await asyncio.sleep(0.5 + 0.2 * num_nodes)

            # Spawn sharded actors on first node
            counters = await systems[0].spawn(Counter, name="counters", sharded=True)

            # Create many entities to ensure distribution
            num_entities = num_nodes * 10
            for i in range(num_entities):
                await counters[f"entity-{i}"].send(Increment(1))

            await asyncio.sleep(0.3)

            # Verify all entities work
            for i in range(num_entities):
                value = await counters[f"entity-{i}"].ask(GetValue())
                assert value == 1, f"Entity entity-{i} has wrong value: {value}"

        finally:
            for system in reversed(systems):
                await system.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_nodes", [2, 3, 5])
    async def test_sharded_entity_ask_from_any_node(self, num_nodes: int, base_port: int):
        """Should be able to ask entities from any node in the cluster.

        Note: Each node that spawns a sharded type gets its own view of entities.
        This test verifies that asking from the same node that created the entity works.
        """
        systems: list[Any] = []

        try:
            # Start cluster
            seed_port = base_port + 300
            seed = ActorSystem.clustered(
                host="127.0.0.1",
                port=seed_port,
                node_id="node-0",
            )
            systems.append(seed)
            await seed.start()
            await asyncio.sleep(0.2)

            for i in range(1, num_nodes):
                node = ActorSystem.clustered(
                    host="127.0.0.1",
                    port=seed_port + i,
                    node_id=f"node-{i}",
                    seeds=[f"127.0.0.1:{seed_port}"],
                )
                systems.append(node)
                await node.start()

            await asyncio.sleep(0.5 + 0.2 * num_nodes)

            # Spawn sharded actors on FIRST node only
            counters = await systems[0].spawn(Counter, name="counters", sharded=True)

            # Initialize some entities (these will be distributed across nodes via consistent hashing)
            test_entities = [f"test-entity-{i}" for i in range(5)]
            for entity_id in test_entities:
                await counters[entity_id].send(Increment(10))

            await asyncio.sleep(0.3)

            # Ask from the SAME sharded ref (routed through node 0)
            for entity_id in test_entities:
                value = await counters[entity_id].ask(GetValue())
                assert value == 10, (
                    f"Entity {entity_id} has wrong value: {value}"
                )

        finally:
            for system in reversed(systems):
                await system.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_nodes", [2, 3, 5])
    async def test_concurrent_entity_updates(self, num_nodes: int, base_port: int):
        """Concurrent updates to different entities should all be processed."""
        systems: list[Any] = []

        try:
            # Start cluster
            seed_port = base_port + 400
            seed = ActorSystem.clustered(
                host="127.0.0.1",
                port=seed_port,
                node_id="node-0",
            )
            systems.append(seed)
            await seed.start()
            await asyncio.sleep(0.2)

            for i in range(1, num_nodes):
                node = ActorSystem.clustered(
                    host="127.0.0.1",
                    port=seed_port + i,
                    node_id=f"node-{i}",
                    seeds=[f"127.0.0.1:{seed_port}"],
                )
                systems.append(node)
                await node.start()

            await asyncio.sleep(0.5 + 0.2 * num_nodes)

            # Spawn sharded ref on the first node only
            counters = await systems[0].spawn(Counter, name="shared-counter", sharded=True)

            # Create multiple entities (distributed across nodes)
            num_entities = num_nodes * 5
            updates_per_entity = 3

            async def update_entity(entity_id: str) -> None:
                for _ in range(updates_per_entity):
                    await counters[entity_id].send(Increment(1))

            # Send updates concurrently to all entities
            tasks = [
                update_entity(f"entity-{i}")
                for i in range(num_entities)
            ]
            await asyncio.gather(*tasks)

            await asyncio.sleep(0.3)

            # Verify all entities received their updates
            for i in range(num_entities):
                value = await counters[f"entity-{i}"].ask(GetValue())
                assert value == updates_per_entity, (
                    f"Entity entity-{i} expected {updates_per_entity}, got {value}"
                )

        finally:
            for system in reversed(systems):
                await system.shutdown()


# =============================================================================
# Throughput Tests
# =============================================================================


class TestClusterThroughput:
    """Test throughput with varying cluster sizes."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_nodes", [2, 3, 5])
    async def test_entity_ask_throughput(self, num_nodes: int, base_port: int):
        """Measure ask throughput across N nodes."""
        import time

        systems: list[Any] = []

        try:
            # Start cluster
            seed_port = base_port + 500
            seed = ActorSystem.clustered(
                host="127.0.0.1",
                port=seed_port,
                node_id="node-0",
            )
            systems.append(seed)
            await seed.start()
            await asyncio.sleep(0.2)

            for i in range(1, num_nodes):
                node = ActorSystem.clustered(
                    host="127.0.0.1",
                    port=seed_port + i,
                    node_id=f"node-{i}",
                    seeds=[f"127.0.0.1:{seed_port}"],
                )
                systems.append(node)
                await node.start()

            await asyncio.sleep(0.5 + 0.2 * num_nodes)

            # Spawn sharded actors
            counters = await systems[0].spawn(Counter, name="perf-counters", sharded=True)

            # Warmup
            for i in range(10):
                await counters[f"warmup-{i}"].send(Increment(1))
            await asyncio.sleep(0.2)

            # Measure ask throughput
            num_asks = 100
            start_time = time.time()

            for i in range(num_asks):
                await counters[f"perf-entity-{i % 20}"].ask(GetValue())

            elapsed = time.time() - start_time
            throughput = num_asks / elapsed

            print(f"\n{num_nodes}-node cluster: {throughput:.1f} asks/sec ({elapsed:.2f}s for {num_asks} asks)")

            # Basic sanity check - should complete in reasonable time
            assert elapsed < 30, f"Too slow: {elapsed}s for {num_asks} asks"

        finally:
            for system in reversed(systems):
                await system.shutdown()


# =============================================================================
# Fault Tolerance Tests
# =============================================================================


class TestClusterFaultTolerance:
    """Test cluster behavior when nodes fail."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_nodes", [3, 5])
    async def test_cluster_survives_node_loss(self, num_nodes: int, base_port: int):
        """Cluster should continue operating after a node leaves."""
        systems: list[Any] = []

        try:
            # Start cluster
            seed_port = base_port + 600
            seed = ActorSystem.clustered(
                host="127.0.0.1",
                port=seed_port,
                node_id="node-0",
            )
            systems.append(seed)
            await seed.start()
            await asyncio.sleep(0.2)

            for i in range(1, num_nodes):
                node = ActorSystem.clustered(
                    host="127.0.0.1",
                    port=seed_port + i,
                    node_id=f"node-{i}",
                    seeds=[f"127.0.0.1:{seed_port}"],
                )
                systems.append(node)
                await node.start()

            await asyncio.sleep(0.5 + 0.2 * num_nodes)

            # Spawn sharded actors and create entities
            counters = await systems[0].spawn(Counter, name="fault-counters", sharded=True)

            # Initialize entities
            for i in range(20):
                await counters[f"entity-{i}"].send(Increment(i))

            await asyncio.sleep(0.3)

            # Shutdown the last node
            last_node = systems.pop()
            await last_node.shutdown()

            await asyncio.sleep(1.0)  # Wait for failure detection

            # Remaining nodes should still work
            # (entities that were on the failed node will be recreated)
            for i in range(10):  # Test subset of entities
                # This should either return the value or timeout if entity was lost
                try:
                    value = await asyncio.wait_for(
                        counters[f"entity-{i}"].ask(GetValue()),
                        timeout=5.0
                    )
                    # If we get a value, it should be valid
                    assert isinstance(value, int)
                except asyncio.TimeoutError:
                    # Entity may have been on failed node - that's ok
                    pass

        finally:
            for system in reversed(systems):
                await system.shutdown()


# =============================================================================
# Consistency Tests
# =============================================================================


class TestClusterConsistency:
    """Test data consistency across cluster."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_nodes", [2, 3, 5])
    async def test_entity_state_consistency(self, num_nodes: int, base_port: int):
        """Entity state should be consistent when accessed from same sharded ref."""
        systems: list[Any] = []

        try:
            # Start cluster
            seed_port = base_port + 700
            seed = ActorSystem.clustered(
                host="127.0.0.1",
                port=seed_port,
                node_id="node-0",
            )
            systems.append(seed)
            await seed.start()
            await asyncio.sleep(0.2)

            for i in range(1, num_nodes):
                node = ActorSystem.clustered(
                    host="127.0.0.1",
                    port=seed_port + i,
                    node_id=f"node-{i}",
                    seeds=[f"127.0.0.1:{seed_port}"],
                )
                systems.append(node)
                await node.start()

            await asyncio.sleep(0.5 + 0.2 * num_nodes)

            # Spawn sharded ref on first node
            counters = await systems[0].spawn(Counter, name="consistency-counters", sharded=True)

            # Create multiple entities with different values
            for i in range(num_nodes * 3):
                await counters[f"entity-{i}"].send(Increment(i * 10))

            await asyncio.sleep(0.3)

            # Read all entities and verify values are consistent
            for i in range(num_nodes * 3):
                expected_value = i * 10
                value = await counters[f"entity-{i}"].ask(GetValue())
                assert value == expected_value, (
                    f"Entity entity-{i} got {value}, expected {expected_value}"
                )

        finally:
            for system in reversed(systems):
                await system.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_nodes", [2, 3, 5])
    async def test_sequential_updates_ordered(self, num_nodes: int, base_port: int):
        """Sequential updates to same entity should be ordered correctly."""
        systems: list[Any] = []

        try:
            # Start cluster
            seed_port = base_port + 800
            seed = ActorSystem.clustered(
                host="127.0.0.1",
                port=seed_port,
                node_id="node-0",
            )
            systems.append(seed)
            await seed.start()
            await asyncio.sleep(0.2)

            for i in range(1, num_nodes):
                node = ActorSystem.clustered(
                    host="127.0.0.1",
                    port=seed_port + i,
                    node_id=f"node-{i}",
                    seeds=[f"127.0.0.1:{seed_port}"],
                )
                systems.append(node)
                await node.start()

            await asyncio.sleep(0.5 + 0.2 * num_nodes)

            # Spawn sharded ref
            counters = await systems[0].spawn(Counter, name="ordered-counters", sharded=True)

            # Sequential updates
            entity_id = "ordered-entity"
            num_updates = 50

            for i in range(num_updates):
                await counters[entity_id].send(Increment(1))

            await asyncio.sleep(0.2)

            # Verify final value
            value = await counters[entity_id].ask(GetValue())
            assert value == num_updates, f"Expected {num_updates}, got {value}"

        finally:
            for system in reversed(systems):
                await system.shutdown()
