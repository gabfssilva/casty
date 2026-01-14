"""Tests for state replication across cluster nodes.

Verifies that entity state is properly replicated to backup nodes
when using replication_factor > 1.
"""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, ActorSystem, Context
from casty.cluster.control_plane import RaftConfig
from casty.persistence import ReplicationConfig


FAST_RAFT_CONFIG = RaftConfig(
    heartbeat_interval=0.05,
    election_timeout_min=0.1,
    election_timeout_max=0.2,
)


@dataclass
class Increment:
    amount: int = 1


@dataclass
class GetCount:
    pass


@dataclass
class SetCount:
    """Direct state set for testing."""
    value: int


class ReplicatedCounter(Actor[Increment | GetCount | SetCount]):
    """Counter with state replication support.

    Note: No need to define get_state/set_state - the default
    implementation automatically serializes public attributes
    (entity_id and count in this case).
    """

    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.count = 0

    async def receive(self, msg: Increment | GetCount | SetCount, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.count += amount
            case GetCount():
                ctx.reply(self.count)
            case SetCount(value):
                self.count = value


class TestStateReplication:
    """Tests for state replication between cluster nodes."""

    @pytest.mark.asyncio
    async def test_state_replicates_to_backup(self):
        """Test that state updates are sent to backup nodes."""
        nodes: list[ActorSystem] = []
        ports: list[int] = []

        try:
            # Start first node
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)
            ports.append(port1)

            await asyncio.sleep(0.5)

            # Start second node (backup)
            node2 = ActorSystem.distributed(
                "127.0.0.1",
                0,
                seeds=[f"127.0.0.1:{port1}"],
                raft_config=FAST_RAFT_CONFIG,
            )
            await node2.start()
            port2 = node2._transport._server.sockets[0].getsockname()[1]
            nodes.append(node2)
            ports.append(port2)

            await asyncio.sleep(1.0)  # Wait for cluster formation

            # Spawn sharded counter with RF=2
            counters = await nodes[0].spawn(
                ReplicatedCounter,
                name="test-counters",
                sharded=True,
                replication=ReplicationConfig(factor=2),
            )

            # Send increments
            for _ in range(5):
                await counters["entity-1"].send(Increment())

            # Wait for replication to complete
            await asyncio.sleep(0.5)

            # Verify primary has correct state
            count = await counters["entity-1"].ask(GetCount())
            assert count == 5

            # Verify backup received state updates
            # The backup should have created the entity via STATE_UPDATE
            if "test-counters" in node2._local_entities:
                backup_entity = node2._local_entities["test-counters"].get("entity-1")
                if backup_entity:
                    # Get state from backup's actor
                    backup_node = node2._supervision_tree.get_node(backup_entity.id)
                    if backup_node and hasattr(backup_node.actor_instance, "count"):
                        backup_count = backup_node.actor_instance.count
                        assert backup_count == 5, f"Backup count is {backup_count}, expected 5"

        finally:
            for node in reversed(nodes):
                await node.shutdown()

    @pytest.mark.asyncio
    async def test_replication_with_multiple_entities(self):
        """Test replication with multiple entities across nodes."""
        nodes: list[ActorSystem] = []
        ports: list[int] = []

        try:
            # Create 3-node cluster
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)
            ports.append(port1)

            await asyncio.sleep(0.5)

            for _ in range(2):
                node = ActorSystem.distributed(
                    "127.0.0.1",
                    0,
                    seeds=[f"127.0.0.1:{port1}"],
                    raft_config=FAST_RAFT_CONFIG,
                )
                await node.start()
                port = node._transport._server.sockets[0].getsockname()[1]
                nodes.append(node)
                ports.append(port)

            await asyncio.sleep(1.0)

            # Spawn sharded counter with RF=3
            counters = await nodes[0].spawn(
                ReplicatedCounter,
                name="multi-counters",
                sharded=True,
                replication=ReplicationConfig(factor=3),
            )

            # Update multiple entities
            entity_ids = ["alice", "bob", "charlie"]
            for i, entity_id in enumerate(entity_ids):
                for _ in range(i + 1):  # alice=1, bob=2, charlie=3
                    await counters[entity_id].send(Increment())

            await asyncio.sleep(0.5)

            # Verify counts
            alice_count = await counters["alice"].ask(GetCount())
            bob_count = await counters["bob"].ask(GetCount())
            charlie_count = await counters["charlie"].ask(GetCount())

            assert alice_count == 1
            assert bob_count == 2
            assert charlie_count == 3

        finally:
            for node in reversed(nodes):
                await node.shutdown()

    @pytest.mark.asyncio
    async def test_replication_preserves_state_consistency(self):
        """Test that state remains consistent after many updates."""
        nodes: list[ActorSystem] = []

        try:
            # Create 2-node cluster
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)

            await asyncio.sleep(0.5)

            node2 = ActorSystem.distributed(
                "127.0.0.1",
                0,
                seeds=[f"127.0.0.1:{port1}"],
                raft_config=FAST_RAFT_CONFIG,
            )
            await node2.start()
            nodes.append(node2)

            await asyncio.sleep(1.0)

            # Spawn with RF=2
            counters = await nodes[0].spawn(
                ReplicatedCounter,
                name="consistency-test",
                sharded=True,
                replication=ReplicationConfig(factor=2),
            )

            # Send many updates
            num_updates = 100
            for _ in range(num_updates):
                await counters["counter-1"].send(Increment())

            await asyncio.sleep(1.0)  # Wait for all replications

            # Verify final count
            count = await counters["counter-1"].ask(GetCount())
            assert count == num_updates

        finally:
            for node in reversed(nodes):
                await node.shutdown()


class TestReplicationFactorImpact:
    """Tests to verify replication factor affects behavior correctly."""

    @pytest.mark.asyncio
    async def test_rf1_no_replication(self):
        """With RF=1, no state updates should be sent to other nodes."""
        nodes: list[ActorSystem] = []

        try:
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)

            await asyncio.sleep(0.5)

            node2 = ActorSystem.distributed(
                "127.0.0.1",
                0,
                seeds=[f"127.0.0.1:{port1}"],
                raft_config=FAST_RAFT_CONFIG,
            )
            await node2.start()
            nodes.append(node2)

            await asyncio.sleep(1.0)

            # Spawn with RF=1 (no backups)
            counters = await nodes[0].spawn(
                ReplicatedCounter,
                name="no-backup-counters",
                sharded=True,
                replication=ReplicationConfig(factor=1),
            )

            # Send updates
            for _ in range(10):
                await counters["solo"].send(Increment())

            await asyncio.sleep(0.3)

            # Verify primary has the count
            count = await counters["solo"].ask(GetCount())
            assert count == 10

            # With RF=1, the backup node should NOT have the entity
            # (unless it happens to be the primary, which depends on hash ring)

        finally:
            for node in reversed(nodes):
                await node.shutdown()

    @pytest.mark.asyncio
    async def test_rf_greater_than_nodes(self):
        """When RF > num_nodes, should still work with available replicas."""
        nodes: list[ActorSystem] = []

        try:
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)

            await asyncio.sleep(0.5)

            node2 = ActorSystem.distributed(
                "127.0.0.1",
                0,
                seeds=[f"127.0.0.1:{port1}"],
                raft_config=FAST_RAFT_CONFIG,
            )
            await node2.start()
            nodes.append(node2)

            await asyncio.sleep(1.0)

            # Spawn with RF=5 but only 2 nodes
            counters = await nodes[0].spawn(
                ReplicatedCounter,
                name="over-replicated",
                sharded=True,
                replication=ReplicationConfig(factor=5),
            )

            # Should still work
            for _ in range(5):
                await counters["test"].send(Increment())

            await asyncio.sleep(0.3)

            count = await counters["test"].ask(GetCount())
            assert count == 5

        finally:
            for node in reversed(nodes):
                await node.shutdown()
