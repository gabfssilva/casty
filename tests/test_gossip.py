"""Tests for SWIM gossip protocol."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from casty.cluster.data_plane import (
    SWIMProtocol,
    GossipConfig,
    MemberInfo,
    MemberStatus,
    MembershipUpdate,
    Ping,
    PingReq,
    Ack,
)


class TestGossipConfig:
    """Tests for GossipConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = GossipConfig()
        assert config.probe_interval == 1.0
        assert config.probe_timeout == 0.5
        assert config.indirect_probes == 3
        assert config.suspicion_multiplier == 5
        assert config.gossip_fanout == 3
        assert config.dead_node_timeout == 60.0

    def test_custom_config(self):
        """Test custom configuration."""
        config = GossipConfig(
            probe_interval=2.0,
            probe_timeout=1.0,
            indirect_probes=5,
        )
        assert config.probe_interval == 2.0
        assert config.probe_timeout == 1.0
        assert config.indirect_probes == 5


class TestMemberInfo:
    """Tests for MemberInfo."""

    def test_member_info_creation(self):
        """Test creating member info."""
        member = MemberInfo(
            node_id="node-1",
            address=("localhost", 8001),
        )
        assert member.node_id == "node-1"
        assert member.address == ("localhost", 8001)
        assert member.status == MemberStatus.ALIVE
        assert member.incarnation == 0
        assert member.last_seen > 0

    def test_member_info_custom(self):
        """Test member info with custom values."""
        member = MemberInfo(
            node_id="node-1",
            address=("localhost", 8001),
            status=MemberStatus.SUSPECT,
            incarnation=5,
        )
        assert member.status == MemberStatus.SUSPECT
        assert member.incarnation == 5


class TestGossipMessages:
    """Tests for gossip message types."""

    def test_ping_message(self):
        """Test Ping message."""
        ping = Ping(sender_id="node-1", incarnation=3)
        assert ping.sender_id == "node-1"
        assert ping.incarnation == 3

    def test_ping_req_message(self):
        """Test PingReq message."""
        ping_req = PingReq(
            sender_id="node-1",
            incarnation=3,
            target_id="node-2",
        )
        assert ping_req.sender_id == "node-1"
        assert ping_req.target_id == "node-2"

    def test_ack_message(self):
        """Test Ack message."""
        ack = Ack(sender_id="node-1", incarnation=3)
        assert ack.sender_id == "node-1"
        assert ack.target_id is None

    def test_ack_with_target(self):
        """Test Ack message with target (for indirect probes)."""
        ack = Ack(
            sender_id="node-1",
            incarnation=3,
            target_id="node-2",
        )
        assert ack.target_id == "node-2"

    def test_membership_update(self):
        """Test MembershipUpdate message."""
        update = MembershipUpdate(
            node_id="node-1",
            status=MemberStatus.ALIVE,
            incarnation=5,
            address=("localhost", 8001),
        )
        assert update.node_id == "node-1"
        assert update.status == MemberStatus.ALIVE
        assert update.incarnation == 5
        assert update.address == ("localhost", 8001)


class TestSWIMProtocol:
    """Tests for SWIMProtocol basic operations."""

    def test_protocol_creation(self):
        """Test creating SWIM protocol."""
        gossip = SWIMProtocol(
            node_id="node-1",
            address=("localhost", 8001),
        )
        assert gossip.node_id == "node-1"
        assert gossip.incarnation == 0

    def test_protocol_with_config(self):
        """Test creating protocol with custom config."""
        config = GossipConfig(probe_interval=2.0)
        gossip = SWIMProtocol(
            node_id="node-1",
            address=("localhost", 8001),
            config=config,
        )
        assert gossip._config.probe_interval == 2.0

    def test_add_member(self):
        """Test adding a member."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002))

        members = gossip.get_all_members()
        assert "node-2" in members
        assert members["node-2"].status == MemberStatus.ALIVE

    def test_add_self_ignored(self):
        """Test that adding self is ignored."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-1", ("localhost", 8001))

        assert "node-1" not in gossip.get_all_members()

    def test_add_member_update_incarnation(self):
        """Test that adding member with higher incarnation updates."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002), incarnation=1)

        members = gossip.get_all_members()
        assert members["node-2"].incarnation == 1

        # Update with higher incarnation
        gossip.add_member("node-2", ("localhost", 8002), incarnation=5)
        members = gossip.get_all_members()
        assert members["node-2"].incarnation == 5

    def test_remove_member(self):
        """Test removing a member."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002))
        gossip.add_member("node-3", ("localhost", 8003))

        gossip.remove_member("node-2")

        members = gossip.get_all_members()
        assert "node-2" not in members
        assert "node-3" in members

    def test_get_alive_members(self):
        """Test getting alive members."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002))
        gossip.add_member("node-3", ("localhost", 8003))

        alive = gossip.get_alive_members()
        assert set(alive) == {"node-2", "node-3"}


class TestSWIMProtocolLifecycle:
    """Tests for SWIM protocol lifecycle."""

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Test starting and stopping the protocol."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))

        await gossip.start()
        assert gossip._running is True

        await gossip.stop()
        assert gossip._running is False

    @pytest.mark.asyncio
    async def test_start_idempotent(self):
        """Test that start is idempotent."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))

        await gossip.start()
        task1 = gossip._probe_task

        await gossip.start()  # Should not create new task
        assert gossip._probe_task is task1

        await gossip.stop()

    @pytest.mark.asyncio
    async def test_stop_idempotent(self):
        """Test that stop is idempotent."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))

        await gossip.start()
        await gossip.stop()
        await gossip.stop()  # Should not raise

        assert gossip._running is False


class TestSWIMProtocolHandlers:
    """Tests for SWIM message handlers."""

    @pytest.mark.asyncio
    async def test_handle_ping(self):
        """Test handling Ping message."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002))

        # Mock transport
        transport = MagicMock()
        transport.send_gossip = AsyncMock()
        gossip.set_transport(transport)

        ping = Ping(sender_id="node-2", incarnation=5)
        await gossip.handle_ping(ping, ("localhost", 8002))

        # Should have sent ACK
        transport.send_gossip.assert_called_once()
        args = transport.send_gossip.call_args
        assert args[0][0] == ("localhost", 8002)  # Address
        assert isinstance(args[0][1], Ack)

    @pytest.mark.asyncio
    async def test_handle_ping_updates_member(self):
        """Test that handling Ping updates member info."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002), incarnation=1)

        transport = MagicMock()
        transport.send_gossip = AsyncMock()
        gossip.set_transport(transport)

        # Send ping with higher incarnation
        ping = Ping(sender_id="node-2", incarnation=5)
        await gossip.handle_ping(ping, ("localhost", 8002))

        # Member incarnation should be updated
        members = gossip.get_all_members()
        assert members["node-2"].incarnation == 5

    @pytest.mark.asyncio
    async def test_handle_ack(self):
        """Test handling Ack message."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002), incarnation=1)

        ack = Ack(sender_id="node-2", incarnation=3)
        await gossip.handle_ack(ack, ("localhost", 8002))

        # Member incarnation should be updated
        members = gossip.get_all_members()
        assert members["node-2"].incarnation == 3


class TestSWIMProtocolMembershipUpdates:
    """Tests for membership update handling."""

    @pytest.mark.asyncio
    async def test_handle_alive_update_new_member(self):
        """Test handling alive update for new member."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))

        alive_called = []
        gossip.on_member_alive = AsyncMock(side_effect=lambda n: alive_called.append(n))

        update = MembershipUpdate(
            node_id="node-2",
            status=MemberStatus.ALIVE,
            incarnation=1,
            address=("localhost", 8002),
        )
        await gossip.handle_membership_updates([update])

        # New member should be added
        members = gossip.get_all_members()
        assert "node-2" in members

        # Give async task time to run
        await asyncio.sleep(0.01)
        assert len(alive_called) == 1

    @pytest.mark.asyncio
    async def test_handle_suspect_update(self):
        """Test handling suspect update."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002), incarnation=1)

        suspect_called = []
        gossip.on_member_suspect = AsyncMock(side_effect=lambda n: suspect_called.append(n))

        update = MembershipUpdate(
            node_id="node-2",
            status=MemberStatus.SUSPECT,
            incarnation=1,
        )
        await gossip.handle_membership_updates([update])

        # Member should be suspect
        members = gossip.get_all_members()
        assert members["node-2"].status == MemberStatus.SUSPECT
        # Callback should have been called
        await asyncio.sleep(0.01)  # Let async task run
        assert len(suspect_called) == 1

    @pytest.mark.asyncio
    async def test_handle_dead_update(self):
        """Test handling dead update."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002))

        dead_called = []
        gossip.on_member_dead = AsyncMock(side_effect=lambda n: dead_called.append(n))

        update = MembershipUpdate(
            node_id="node-2",
            status=MemberStatus.DEAD,
            incarnation=1,
        )
        await gossip.handle_membership_updates([update])

        # Member should be dead
        members = gossip.get_all_members()
        assert members["node-2"].status == MemberStatus.DEAD
        assert len(dead_called) == 1

    @pytest.mark.asyncio
    async def test_refute_self_suspicion(self):
        """Test that protocol refutes suspicion about itself."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))

        initial_incarnation = gossip.incarnation

        # Someone thinks we're suspect
        update = MembershipUpdate(
            node_id="node-1",
            status=MemberStatus.SUSPECT,
            incarnation=initial_incarnation,
        )
        await gossip.handle_membership_updates([update])

        # We should have incremented our incarnation
        assert gossip.incarnation > initial_incarnation


class TestSWIMProtocolRepr:
    """Tests for string representation."""

    def test_repr(self):
        """Test repr."""
        gossip = SWIMProtocol("node-1", ("localhost", 8001))
        gossip.add_member("node-2", ("localhost", 8002))
        gossip.add_member("node-3", ("localhost", 8003))

        r = repr(gossip)
        assert "SWIMProtocol" in r
        assert "node-1" in r
        assert "2/2" in r  # 2 alive out of 2 members
