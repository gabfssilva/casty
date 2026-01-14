"""Tests for control plane components."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from casty.cluster.control_plane import (
    # Commands
    JoinCluster,
    LeaveCluster,
    NodeFailed,
    NodeRecovered,
    RegisterSingleton,
    UnregisterSingleton,
    TransferSingleton,
    RegisterEntityType,
    UnregisterEntityType,
    UpdateEntityTypeConfig,
    RegisterNamedActor,
    UnregisterNamedActor,
    get_command_type,
    # State
    ClusterState,
    NodeInfo,
    SingletonInfo,
    EntityTypeInfo,
    NamedActorInfo,
    # Raft
    RaftManager,
    RaftConfig,
    RaftRole,
    LogEntry,
    VoteRequest,
    VoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
)


class TestCommands:
    """Tests for control plane commands."""

    def test_join_cluster(self):
        """Test JoinCluster command."""
        cmd = JoinCluster(
            node_id="node-1",
            address=("localhost", 8001),
            timestamp=123.45,
        )
        assert cmd.node_id == "node-1"
        assert cmd.address == ("localhost", 8001)
        assert cmd.timestamp == 123.45

    def test_leave_cluster(self):
        """Test LeaveCluster command."""
        cmd = LeaveCluster(node_id="node-1", timestamp=123.45)
        assert cmd.node_id == "node-1"

    def test_node_failed(self):
        """Test NodeFailed command."""
        cmd = NodeFailed(
            node_id="node-1",
            detected_by="node-2",
            timestamp=123.45,
        )
        assert cmd.node_id == "node-1"
        assert cmd.detected_by == "node-2"

    def test_register_singleton(self):
        """Test RegisterSingleton command."""
        cmd = RegisterSingleton(
            name="cache",
            node_id="node-1",
            actor_class="myapp.CacheActor",
            version=1,
        )
        assert cmd.name == "cache"
        assert cmd.actor_class == "myapp.CacheActor"
        assert cmd.version == 1

    def test_register_entity_type(self):
        """Test RegisterEntityType command."""
        cmd = RegisterEntityType(
            entity_type="User",
            actor_class="myapp.UserActor",
            replication_factor=3,
        )
        assert cmd.entity_type == "User"
        assert cmd.replication_factor == 3

    def test_get_command_type(self):
        """Test get_command_type helper."""
        cmd = JoinCluster("node-1", ("localhost", 8001), 123.45)
        assert get_command_type(cmd) == "JoinCluster"

    def test_commands_are_frozen(self):
        """Test that commands are immutable."""
        cmd = JoinCluster("node-1", ("localhost", 8001), 123.45)
        with pytest.raises(AttributeError):
            cmd.node_id = "node-2"  # type: ignore


class TestClusterState:
    """Tests for ClusterState."""

    def test_initial_state(self):
        """Test initial empty state."""
        state = ClusterState()
        assert len(state.get_all_members()) == 0
        assert len(state.get_all_singletons()) == 0
        assert len(state.get_all_entity_types()) == 0
        assert len(state.get_all_named_actors()) == 0

    def test_apply_join_cluster(self):
        """Test applying JoinCluster command."""
        state = ClusterState()
        state.apply(JoinCluster("node-1", ("localhost", 8001), time.time()))

        members = state.get_all_members()
        assert "node-1" in members
        assert members["node-1"].status == "active"
        assert members["node-1"].address == ("localhost", 8001)

    def test_apply_leave_cluster(self):
        """Test applying LeaveCluster command."""
        state = ClusterState()
        state.apply(JoinCluster("node-1", ("localhost", 8001), time.time()))
        state.apply(LeaveCluster("node-1", time.time()))

        members = state.get_all_members()
        assert members["node-1"].status == "leaving"

    def test_apply_node_failed(self):
        """Test applying NodeFailed command."""
        state = ClusterState()
        state.apply(JoinCluster("node-1", ("localhost", 8001), time.time()))
        state.apply(NodeFailed("node-1", "node-2", time.time()))

        members = state.get_all_members()
        assert members["node-1"].status == "failed"
        assert members["node-1"].failed_at is not None

    def test_apply_node_recovered(self):
        """Test applying NodeRecovered command."""
        state = ClusterState()
        state.apply(JoinCluster("node-1", ("localhost", 8001), time.time()))
        state.apply(NodeFailed("node-1", "node-2", time.time()))
        state.apply(NodeRecovered("node-1", ("localhost", 8001), time.time()))

        members = state.get_all_members()
        assert members["node-1"].status == "active"
        assert members["node-1"].failed_at is None

    def test_get_active_members(self):
        """Test getting active members."""
        state = ClusterState()
        state.apply(JoinCluster("node-1", ("localhost", 8001), time.time()))
        state.apply(JoinCluster("node-2", ("localhost", 8002), time.time()))
        state.apply(JoinCluster("node-3", ("localhost", 8003), time.time()))
        state.apply(NodeFailed("node-2", "node-1", time.time()))

        active = state.get_active_members()
        assert set(active) == {"node-1", "node-3"}

    def test_get_failed_members(self):
        """Test getting failed members."""
        state = ClusterState()
        state.apply(JoinCluster("node-1", ("localhost", 8001), time.time()))
        state.apply(JoinCluster("node-2", ("localhost", 8002), time.time()))
        state.apply(NodeFailed("node-2", "node-1", time.time()))

        failed = state.get_failed_members()
        assert failed == ["node-2"]


class TestClusterStateSingletons:
    """Tests for singleton management in ClusterState."""

    def test_register_singleton(self):
        """Test registering a singleton."""
        state = ClusterState()
        state.apply(RegisterSingleton("cache", "node-1", "app.Cache", 1))

        singleton = state.get_singleton("cache")
        assert singleton is not None
        assert singleton.node_id == "node-1"
        assert singleton.actor_class == "app.Cache"
        assert singleton.version == 1
        assert singleton.status == "active"

    def test_register_singleton_version_conflict(self):
        """Test that higher version wins."""
        state = ClusterState()
        state.apply(RegisterSingleton("cache", "node-1", "app.Cache", 2))
        state.apply(RegisterSingleton("cache", "node-2", "app.Cache", 1))

        singleton = state.get_singleton("cache")
        assert singleton.node_id == "node-1"  # Higher version stays
        assert singleton.version == 2

    def test_unregister_singleton(self):
        """Test unregistering a singleton."""
        state = ClusterState()
        state.apply(RegisterSingleton("cache", "node-1", "app.Cache", 1))
        state.apply(UnregisterSingleton("cache"))

        assert state.get_singleton("cache") is None

    def test_transfer_singleton(self):
        """Test transferring a singleton."""
        state = ClusterState()
        state.apply(RegisterSingleton("cache", "node-1", "app.Cache", 1))
        state.apply(TransferSingleton("cache", "node-1", "node-2"))

        singleton = state.get_singleton("cache")
        assert singleton.node_id == "node-2"
        assert singleton.version == 2  # Version incremented

    def test_orphan_singletons_on_node_failure(self):
        """Test that singletons become orphan when node fails."""
        state = ClusterState()
        state.apply(JoinCluster("node-1", ("localhost", 8001), time.time()))
        state.apply(RegisterSingleton("cache", "node-1", "app.Cache", 1))
        state.apply(RegisterSingleton("registry", "node-1", "app.Registry", 1))

        state.apply(NodeFailed("node-1", "node-2", time.time()))

        orphans = state.get_orphan_singletons()
        assert set(orphans) == {"cache", "registry"}

    def test_get_singletons_on_node(self):
        """Test getting singletons on a specific node."""
        state = ClusterState()
        state.apply(RegisterSingleton("cache", "node-1", "app.Cache", 1))
        state.apply(RegisterSingleton("registry", "node-1", "app.Registry", 1))
        state.apply(RegisterSingleton("config", "node-2", "app.Config", 1))

        singletons = state.get_singletons_on_node("node-1")
        assert set(singletons) == {"cache", "registry"}


class TestClusterStateEntityTypes:
    """Tests for entity type management in ClusterState."""

    def test_register_entity_type(self):
        """Test registering an entity type."""
        state = ClusterState()
        state.apply(RegisterEntityType("User", "app.UserActor", 3))

        et = state.get_entity_type("User")
        assert et is not None
        assert et.actor_class == "app.UserActor"
        assert et.replication_factor == 3

    def test_unregister_entity_type(self):
        """Test unregistering an entity type."""
        state = ClusterState()
        state.apply(RegisterEntityType("User", "app.UserActor", 3))
        state.apply(UnregisterEntityType("User"))

        assert state.get_entity_type("User") is None

    def test_update_entity_type_config(self):
        """Test updating entity type configuration."""
        state = ClusterState()
        state.apply(RegisterEntityType("User", "app.UserActor", 3))
        state.apply(UpdateEntityTypeConfig("User", 5))

        et = state.get_entity_type("User")
        assert et.replication_factor == 5


class TestClusterStateNamedActors:
    """Tests for named actor management in ClusterState."""

    def test_register_named_actor(self):
        """Test registering a named actor."""
        state = ClusterState()
        state.apply(RegisterNamedActor("counter", "node-1", "actor-123"))

        actor = state.get_named_actor("counter")
        assert actor is not None
        assert actor.node_id == "node-1"
        assert actor.actor_id == "actor-123"

    def test_unregister_named_actor(self):
        """Test unregistering a named actor."""
        state = ClusterState()
        state.apply(RegisterNamedActor("counter", "node-1", "actor-123"))
        state.apply(UnregisterNamedActor("counter"))

        assert state.get_named_actor("counter") is None

    def test_get_named_actors_on_node(self):
        """Test getting named actors on a specific node."""
        state = ClusterState()
        state.apply(RegisterNamedActor("counter", "node-1", "actor-1"))
        state.apply(RegisterNamedActor("cache", "node-1", "actor-2"))
        state.apply(RegisterNamedActor("worker", "node-2", "actor-3"))

        actors = state.get_named_actors_on_node("node-1")
        assert set(actors) == {"counter", "cache"}


class TestClusterStateSerialization:
    """Tests for ClusterState serialization."""

    def test_to_dict_and_from_dict(self):
        """Test serialization round-trip."""
        state = ClusterState()
        state.apply(JoinCluster("node-1", ("localhost", 8001), 100.0))
        state.apply(JoinCluster("node-2", ("localhost", 8002), 101.0))
        state.apply(RegisterSingleton("cache", "node-1", "app.Cache", 1))
        state.apply(RegisterEntityType("User", "app.UserActor", 3))
        state.apply(RegisterNamedActor("counter", "node-1", "actor-123"))

        # Serialize
        data = state.to_dict()

        # Deserialize
        restored = ClusterState.from_dict(data)

        # Verify
        assert len(restored.get_all_members()) == 2
        assert "node-1" in restored.get_all_members()
        assert restored.get_singleton("cache") is not None
        assert restored.get_entity_type("User") is not None
        assert restored.get_named_actor("counter") is not None


class TestRaftConfig:
    """Tests for RaftConfig."""

    def test_default_config(self):
        """Test default Raft configuration."""
        config = RaftConfig()
        assert config.heartbeat_interval == 0.3
        assert config.election_timeout_min == 1.0
        assert config.election_timeout_max == 2.0
        assert config.max_entries_per_append == 100

    def test_custom_config(self):
        """Test custom Raft configuration."""
        config = RaftConfig(
            heartbeat_interval=0.5,
            election_timeout_min=2.0,
            election_timeout_max=4.0,
        )
        assert config.heartbeat_interval == 0.5
        assert config.election_timeout_min == 2.0


class TestRaftMessages:
    """Tests for Raft RPC messages."""

    def test_vote_request(self):
        """Test VoteRequest message."""
        req = VoteRequest(
            term=5,
            candidate_id="node-1",
            last_log_index=10,
            last_log_term=4,
        )
        assert req.term == 5
        assert req.candidate_id == "node-1"

    def test_vote_response(self):
        """Test VoteResponse message."""
        resp = VoteResponse(term=5, vote_granted=True)
        assert resp.term == 5
        assert resp.vote_granted is True

    def test_append_entries_request(self):
        """Test AppendEntriesRequest message."""
        req = AppendEntriesRequest(
            term=5,
            leader_id="node-1",
            prev_log_index=10,
            prev_log_term=4,
            entries=[],
            leader_commit=8,
        )
        assert req.term == 5
        assert req.leader_id == "node-1"
        assert req.leader_commit == 8

    def test_append_entries_response(self):
        """Test AppendEntriesResponse message."""
        resp = AppendEntriesResponse(term=5, success=True, match_index=10)
        assert resp.term == 5
        assert resp.success is True
        assert resp.match_index == 10


class TestRaftManager:
    """Tests for RaftManager."""

    def test_manager_creation(self):
        """Test creating a RaftManager."""
        raft = RaftManager(node_id="node-1", peers=["node-2", "node-3"])
        assert raft.node_id == "node-1"
        assert raft.current_term == 0
        assert raft.is_leader is False
        assert raft.leader_id is None

    def test_add_peer(self):
        """Test adding a peer."""
        raft = RaftManager(node_id="node-1")
        raft.add_peer("node-2")
        assert "node-2" in raft._peers

    def test_remove_peer(self):
        """Test removing a peer."""
        raft = RaftManager(node_id="node-1", peers=["node-2", "node-3"])
        raft.remove_peer("node-2")
        assert "node-2" not in raft._peers
        assert "node-3" in raft._peers

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Test starting and stopping the Raft manager."""
        raft = RaftManager(node_id="node-1")

        await raft.start()
        assert raft._running is True

        await raft.stop()
        assert raft._running is False

    @pytest.mark.asyncio
    async def test_single_node_becomes_leader(self):
        """Test that a single node becomes leader."""
        config = RaftConfig(
            election_timeout_min=0.1,
            election_timeout_max=0.2,
        )
        raft = RaftManager(node_id="node-1", config=config)

        await raft.start()

        # Wait for election timeout
        await asyncio.sleep(0.5)

        assert raft.is_leader is True
        assert raft.leader_id == "node-1"

        await raft.stop()


class TestRaftManagerVoting:
    """Tests for Raft voting."""

    @pytest.mark.asyncio
    async def test_handle_vote_request_grant(self):
        """Test handling a vote request that should be granted."""
        raft = RaftManager(node_id="node-1")

        request = VoteRequest(
            term=1,
            candidate_id="node-2",
            last_log_index=0,
            last_log_term=0,
        )
        response = await raft.handle_vote_request(request)

        assert response.vote_granted is True
        assert raft._persistent.voted_for == "node-2"

    @pytest.mark.asyncio
    async def test_handle_vote_request_deny_stale_term(self):
        """Test denying vote for stale term."""
        raft = RaftManager(node_id="node-1")
        raft._persistent.current_term = 5

        request = VoteRequest(
            term=3,
            candidate_id="node-2",
            last_log_index=0,
            last_log_term=0,
        )
        response = await raft.handle_vote_request(request)

        assert response.vote_granted is False

    @pytest.mark.asyncio
    async def test_handle_vote_request_deny_already_voted(self):
        """Test denying vote when already voted in same term."""
        raft = RaftManager(node_id="node-1")
        # Set current term and voted_for in same term
        raft._persistent.current_term = 1
        raft._persistent.voted_for = "node-3"

        request = VoteRequest(
            term=1,  # Same term - should deny
            candidate_id="node-2",
            last_log_index=0,
            last_log_term=0,
        )
        response = await raft.handle_vote_request(request)

        assert response.vote_granted is False
        assert raft._persistent.voted_for == "node-3"  # Unchanged


class TestRaftManagerAppendEntries:
    """Tests for Raft append entries."""

    @pytest.mark.asyncio
    async def test_handle_append_entries_heartbeat(self):
        """Test handling a heartbeat (empty AppendEntries)."""
        raft = RaftManager(node_id="node-1")

        request = AppendEntriesRequest(
            term=1,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        response = await raft.handle_append_entries(request)

        assert response.success is True
        assert raft._leader_id == "node-2"

    @pytest.mark.asyncio
    async def test_handle_append_entries_rejects_stale_term(self):
        """Test rejecting AppendEntries with stale term."""
        raft = RaftManager(node_id="node-1")
        raft._persistent.current_term = 5

        request = AppendEntriesRequest(
            term=3,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        response = await raft.handle_append_entries(request)

        assert response.success is False
        assert response.term == 5

    @pytest.mark.asyncio
    async def test_handle_append_entries_with_entries(self):
        """Test handling AppendEntries with log entries."""
        raft = RaftManager(node_id="node-1")

        cmd = JoinCluster("node-3", ("localhost", 8003), time.time())
        entry = LogEntry(term=1, index=1, command=cmd)

        request = AppendEntriesRequest(
            term=1,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=[entry],
            leader_commit=0,
        )
        response = await raft.handle_append_entries(request)

        assert response.success is True
        assert len(raft._persistent.log) == 1
        assert raft._persistent.log[0].command == cmd

    @pytest.mark.asyncio
    async def test_handle_append_entries_commits_entries(self):
        """Test that committed entries are applied to state machine."""
        raft = RaftManager(node_id="node-1")

        cmd = JoinCluster("node-3", ("localhost", 8003), time.time())
        entry = LogEntry(term=1, index=1, command=cmd)

        request = AppendEntriesRequest(
            term=1,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=[entry],
            leader_commit=1,  # Leader has committed
        )
        response = await raft.handle_append_entries(request)

        assert response.success is True
        assert raft._volatile.commit_index == 1
        assert raft._volatile.last_applied == 1
        assert "node-3" in raft.state.get_all_members()


class TestRaftManagerRepr:
    """Tests for RaftManager string representation."""

    def test_repr(self):
        """Test repr."""
        raft = RaftManager(node_id="node-1")
        r = repr(raft)
        assert "RaftManager" in r
        assert "node-1" in r
        assert "FOLLOWER" in r
