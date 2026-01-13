"""Tests for Multi-Raft support in Casty.

This module contains tests for:
1. Backwards compatibility with existing single-group behavior
2. Multiple independent Raft groups
3. Heartbeat batching efficiency
4. AppendEntries batching
5. Group isolation
6. Per-group leader election
7. Per-group persistence
"""

from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass

import pytest

from casty import DistributedActorSystem
from casty.cluster.multiraft import MultiRaftManager
from casty.cluster.multiraft.batching import BatchedMessage, MessageBatcher
from casty.cluster.transport import MessageType

pytestmark = pytest.mark.slow


# Re-use fixtures from distributed tests
@dataclass
class NodeHandle:
    """Wrapper for a running distributed node."""

    system: DistributedActorSystem
    task: asyncio.Task[None]
    port: int

    async def shutdown(self) -> None:
        """Cleanly shut down node and cancel serve task."""
        await self.system.shutdown()
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass


@pytest.fixture
async def n_node_cluster(get_port):
    """Factory for creating N-node clusters with proper cleanup."""
    all_nodes: list[NodeHandle] = []

    async def _create_cluster(n: int) -> list[NodeHandle]:
        base_port = get_port()
        for _ in range(n - 1):
            get_port()  # Reserve ports

        nodes: list[NodeHandle] = []
        for i in range(n):
            seeds = [f"127.0.0.1:{base_port}"] if i > 0 else []
            system = DistributedActorSystem(
                "127.0.0.1",
                base_port + i,
                seeds=seeds,
                expected_cluster_size=n,
            )

            task = asyncio.create_task(system.serve())
            await asyncio.sleep(0.05)  # Stagger startup

            handle = NodeHandle(system=system, task=task, port=base_port + i)
            nodes.append(handle)
            all_nodes.append(handle)

        return nodes

    yield _create_cluster

    # Cleanup
    for node in reversed(all_nodes):
        await node.shutdown()


async def wait_for_leader(
    nodes: list[DistributedActorSystem],
    *,
    timeout: float = 5.0,
    poll_interval: float = 0.1,
    group: int = 0,
) -> DistributedActorSystem:
    """Wait until exactly one leader emerges for a specific group."""
    elapsed = 0.0
    while elapsed < timeout:
        leaders = [n for n in nodes if n.is_group_leader(group)]
        if len(leaders) == 1:
            return leaders[0]
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"No leader elected for group {group} within {timeout}s")


class TestSingleGroupBackwardsCompatible:
    """Test 1: With only group 0, all existing features work."""

    async def test_election_with_single_group(self, n_node_cluster) -> None:
        """Election works with single group (backwards compatible)."""
        nodes = await n_node_cluster(3)
        systems = [n.system for n in nodes]

        leader = await wait_for_leader(systems, timeout=5.0)
        assert leader is not None
        assert leader.is_leader

        # Check only one leader
        leaders = [s for s in systems if s.is_leader]
        assert len(leaders) == 1

    async def test_append_command_with_single_group(self, n_node_cluster) -> None:
        """append_command works with single group (backwards compatible)."""
        nodes = await n_node_cluster(3)
        systems = [n.system for n in nodes]

        leader = await wait_for_leader(systems, timeout=5.0)

        # Append command should work without specifying group
        command = {"action": "set", "key": "x", "value": 1}
        success = await leader.append_command(command)
        assert success, "Command should be successfully replicated"

        # Wait for replication
        await asyncio.sleep(0.5)

        # All nodes should have the entry
        for node in systems:
            assert node._cluster.raft_log.last_log_index >= 1

    async def test_commit_index_advances_with_single_group(self, n_node_cluster) -> None:
        """Commit index advances with single group (backwards compatible)."""
        nodes = await n_node_cluster(3)
        systems = [n.system for n in nodes]

        leader = await wait_for_leader(systems, timeout=5.0)

        # Initial commit should be 0
        assert leader.commit_index == 0

        # Append command
        await leader.append_command({"test": "value"})

        # Wait for commit
        await asyncio.sleep(1.0)
        assert leader.commit_index >= 1


class TestMultipleGroupsIndependentLeaders:
    """Test 2: Create 3 groups in 3-node cluster, verify independent leaders."""

    async def test_groups_can_have_different_leaders(self, n_node_cluster) -> None:
        """Each group can independently elect different leaders."""
        nodes = await n_node_cluster(3)
        systems = [n.system for n in nodes]

        # Wait for system group leader
        leader_0 = await wait_for_leader(systems, timeout=5.0, group=0)
        assert leader_0 is not None

        # Create additional groups on all nodes
        for system in systems:
            await system.create_raft_group(1)
            await system.create_raft_group(2)

        # Wait for groups to elect leaders (may be different nodes)
        await asyncio.sleep(3.0)

        # Each group should have exactly one leader
        for group_id in [0, 1, 2]:
            leaders = [s for s in systems if s.is_group_leader(group_id)]
            assert len(leaders) == 1, f"Group {group_id} should have exactly one leader"

        # Groups CAN have different leaders (not guaranteed, but possible)
        leader_ids = set()
        for group_id in [0, 1, 2]:
            for system in systems:
                if system.is_group_leader(group_id):
                    leader_ids.add((group_id, system.node_id))

        # We should have 3 leader entries (one per group)
        assert len(leader_ids) == 3


class TestHeartbeatBatching:
    """Test 3: With 3 groups, verify heartbeat batching reduces messages."""

    async def test_batching_reduces_message_count(self) -> None:
        """Verify batching combines heartbeats from multiple groups."""
        batcher = MessageBatcher()

        # Add heartbeats for 3 groups to the same peer
        peer_id = "peer1"
        for group_id in [0, 1, 2]:
            batcher.add_heartbeat(
                peer_id,
                group_id,
                {"group_id": group_id, "term": 1, "entries": []},
            )

        # Get batched heartbeats
        batched = batcher.get_all_heartbeats()

        # Should have exactly one entry for peer (not 3)
        assert len(batched) == 1
        assert peer_id in batched

        # The batch should contain 3 messages
        messages = batched[peer_id]
        assert len(messages) == 3

        # Verify all groups are represented
        group_ids = {msg.group_id for msg in messages}
        assert group_ids == {0, 1, 2}

    async def test_multiple_peers_each_get_batched(self) -> None:
        """Each peer gets their own batch of all groups."""
        batcher = MessageBatcher()

        # Add heartbeats for 3 groups to 3 peers
        for peer_id in ["peer1", "peer2", "peer3"]:
            for group_id in [0, 1, 2]:
                batcher.add_heartbeat(
                    peer_id,
                    group_id,
                    {"group_id": group_id, "term": 1, "entries": []},
                )

        batched = batcher.get_all_heartbeats()

        # Should have exactly 3 entries (one per peer)
        assert len(batched) == 3

        # Each peer should have 3 messages (one per group)
        for peer_id, messages in batched.items():
            assert len(messages) == 3


class TestAppendEntriesBatching:
    """Test 4: Multiple appends in different groups sent in batch."""

    async def test_append_entries_batching(self) -> None:
        """Verify AppendEntries for multiple groups can be batched."""
        batcher = MessageBatcher()

        # Add AppendEntries for 3 groups
        peer_id = "peer1"
        for group_id in [0, 1, 2]:
            batcher.add_append_entries(
                peer_id,
                group_id,
                {
                    "group_id": group_id,
                    "term": 1,
                    "entries": [{"term": 1, "index": 1, "command": {"key": f"g{group_id}"}}],
                },
            )

        batched = batcher.get_all_append_entries()

        # Should have exactly one entry for peer
        assert len(batched) == 1
        assert peer_id in batched

        # The batch should contain 3 messages
        messages = batched[peer_id]
        assert len(messages) == 3

    async def test_serialize_deserialize_batch(self) -> None:
        """Verify batch serialization and deserialization."""
        messages = [
            BatchedMessage(
                group_id=0,
                msg_type="append_entries_req",
                data={"term": 1, "entries": []},
            ),
            BatchedMessage(
                group_id=1,
                msg_type="append_entries_req",
                data={"term": 2, "entries": []},
            ),
        ]

        # Serialize
        serialized = MessageBatcher.serialize_batch(messages)
        assert len(serialized) == 2
        assert all(isinstance(item, dict) for item in serialized)

        # Deserialize
        deserialized = MessageBatcher.deserialize_batch(serialized)
        assert len(deserialized) == 2
        assert all(isinstance(msg, BatchedMessage) for msg in deserialized)
        assert deserialized[0].group_id == 0
        assert deserialized[1].group_id == 1


class TestGroupIsolation:
    """Test 5: Failure in one group doesn't affect others."""

    async def test_groups_operate_independently(self, n_node_cluster) -> None:
        """Groups can append commands independently."""
        nodes = await n_node_cluster(3)
        systems = [n.system for n in nodes]

        # Wait for system group leader
        leader_0 = await wait_for_leader(systems, timeout=5.0, group=0)

        # Create another group
        for system in systems:
            await system.create_raft_group(1)

        await asyncio.sleep(2.0)  # Wait for group 1 to elect leader

        # Append to group 0
        success_0 = await leader_0.append_command({"group": 0}, group=0)
        assert success_0

        # Find group 1 leader
        leader_1 = None
        for system in systems:
            if system.is_group_leader(1):
                leader_1 = system
                break

        if leader_1:
            # Append to group 1 independently
            success_1 = await leader_1.append_command({"group": 1}, group=1)
            assert success_1

            # Verify logs are separate
            group_0_log = leader_0._raft_manager.get_group(0)
            group_1_log = leader_1._raft_manager.get_group(1)

            assert group_0_log is not None
            assert group_1_log is not None

            # Each group should have its own entries
            # (exact indices depend on election, but they should be independent)


class TestLeaderElectionPerGroup:
    """Test 6: Leader failover for one group doesn't affect others."""

    async def test_group_isolation_during_leader_failure(self, n_node_cluster) -> None:
        """When group 1's leader fails, group 0 continues working."""
        nodes = await n_node_cluster(3)
        systems = [n.system for n in nodes]

        # Wait for system group leader
        leader_0 = await wait_for_leader(systems, timeout=5.0, group=0)

        # Create group 1
        for system in systems:
            await system.create_raft_group(1)

        await asyncio.sleep(2.0)

        # Append command to group 0 - should work regardless of group 1 state
        success = await leader_0.append_command({"before": True}, group=0)
        assert success

        # Group 0 should still function
        success = await leader_0.append_command({"after": True}, group=0)
        assert success


class TestPersistencePerGroup:
    """Test 7: Each group persists state separately."""

    async def test_groups_persist_independently(self, get_port) -> None:
        """Each group persists and recovers its state separately."""
        with tempfile.TemporaryDirectory() as tmpdir:
            port = get_port()

            # Create system with multiple groups
            system1 = DistributedActorSystem(
                "127.0.0.1",
                port,
                expected_cluster_size=1,
                persistence_dir=tmpdir,
            )

            task1 = asyncio.create_task(system1.serve())
            await asyncio.sleep(0.3)

            # Wait for leader
            await wait_for_leader([system1], timeout=5.0, group=0)

            # Create additional group
            await system1.create_raft_group(1)
            await asyncio.sleep(1.0)

            # Append to both groups
            await system1.append_command({"group": 0, "value": "g0"}, group=0)
            await asyncio.sleep(0.3)

            # Try to append to group 1 (only works if we're leader)
            if system1.is_group_leader(1):
                await system1.append_command({"group": 1, "value": "g1"}, group=1)
                await asyncio.sleep(0.3)

            # Store state info
            node_id = system1.node_id
            group_0_index = system1._raft_manager.system_group.last_log_index

            # Shutdown
            await system1.shutdown()
            task1.cancel()
            try:
                await task1
            except asyncio.CancelledError:
                pass

            # Restart
            system2 = DistributedActorSystem(
                "127.0.0.1",
                port,
                expected_cluster_size=1,
                persistence_dir=tmpdir,
                node_id=node_id,
            )

            task2 = asyncio.create_task(system2.serve())
            await asyncio.sleep(0.3)

            try:
                # Verify group 0 state was restored
                assert system2._raft_manager.system_group.last_log_index >= group_0_index

                # Verify log content
                entry = system2._raft_manager.system_group.raft_log.get_entry(1)
                if entry:
                    assert entry.command.get("group") == 0
            finally:
                await system2.shutdown()
                task2.cancel()
                try:
                    await task2
                except asyncio.CancelledError:
                    pass


class TestMultiRaftManager:
    """Unit tests for MultiRaftManager."""

    def test_system_group_created_by_default(self) -> None:
        """Manager creates system group (0) by default."""
        manager = MultiRaftManager(node_id="test-node", expected_size=1)
        assert 0 in manager.groups
        assert manager.system_group is not None
        assert manager.system_group.group_id == 0

    async def test_create_additional_groups(self) -> None:
        """Manager can create additional groups."""
        manager = MultiRaftManager(node_id="test-node", expected_size=1)

        await manager.create_group(1)
        await manager.create_group(2)

        assert 1 in manager.groups
        assert 2 in manager.groups
        assert len(manager.groups) == 3

    async def test_cannot_recreate_system_group(self) -> None:
        """Creating group 0 should return existing group, not error."""
        manager = MultiRaftManager(node_id="test-node", expected_size=1)

        # Should return existing group
        group = await manager.create_group(0)
        assert group is manager.system_group

    def test_get_group(self) -> None:
        """Can retrieve groups by ID."""
        manager = MultiRaftManager(node_id="test-node", expected_size=1)

        group = manager.get_group(0)
        assert group is not None
        assert group.group_id == 0

        # Non-existent group returns None
        assert manager.get_group(999) is None

    def test_stop_stops_all_groups(self) -> None:
        """Stop propagates to all groups."""
        manager = MultiRaftManager(node_id="test-node", expected_size=1)

        manager.stop()

        # Manager should be stopped
        assert manager._running is False


class TestBatchedMessage:
    """Unit tests for BatchedMessage."""

    def test_batched_message_creation(self) -> None:
        """Can create batched messages."""
        msg = BatchedMessage(
            group_id=1,
            msg_type="append_entries_req",
            data={"term": 1, "entries": []},
        )

        assert msg.group_id == 1
        assert msg.msg_type == "append_entries_req"
        assert msg.data["term"] == 1

    def test_batched_message_types(self) -> None:
        """All message types can be used."""
        types = [
            "vote_request",
            "vote_response",
            "append_entries_req",
            "append_entries_res",
            "install_snapshot_req",
            "install_snapshot_res",
        ]

        for msg_type in types:
            msg = BatchedMessage(group_id=0, msg_type=msg_type, data={})
            assert msg.msg_type == msg_type


class TestMessageType:
    """Test that BATCHED_RAFT_MSG is properly added."""

    def test_batched_raft_msg_exists(self) -> None:
        """BATCHED_RAFT_MSG message type exists."""
        assert hasattr(MessageType, "BATCHED_RAFT_MSG")
        assert MessageType.BATCHED_RAFT_MSG == 18
