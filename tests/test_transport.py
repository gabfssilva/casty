"""Tests for transport layer."""

import asyncio

import pytest

from casty.cluster.transport import (
    MessageType,
    WireMessage,
    Connection,
    Transport,
)


class TestMessageType:
    """Tests for MessageType enum."""

    def test_message_types_exist(self):
        """Test that all message types are defined."""
        assert MessageType.ACTOR_MSG == 1
        assert MessageType.ASK_REQUEST == 2
        assert MessageType.VOTE_REQUEST == 10
        assert MessageType.GOSSIP_PING == 20
        assert MessageType.ENTITY_MSG == 40

    def test_message_type_values_unique(self):
        """Test that all message types have unique values."""
        values = [m.value for m in MessageType]
        assert len(values) == len(set(values))


class TestWireMessage:
    """Tests for WireMessage."""

    def test_create_message(self):
        """Test creating a wire message."""
        msg = WireMessage(
            msg_type=MessageType.ACTOR_MSG,
            name="test-actor",
            payload=b"hello",
        )
        assert msg.msg_type == MessageType.ACTOR_MSG
        assert msg.name == "test-actor"
        assert msg.payload == b"hello"

    def test_to_bytes(self):
        """Test serializing message to bytes."""
        msg = WireMessage(
            msg_type=MessageType.ACTOR_MSG,
            name="test",
            payload=b"data",
        )
        data = msg.to_bytes()

        # Check structure: type (1) + name_len (2) + name + payload_len (4) + payload
        assert len(data) == 1 + 2 + 4 + 4 + 4
        assert data[0] == MessageType.ACTOR_MSG

    def test_to_bytes_empty_name(self):
        """Test serializing message with empty name."""
        msg = WireMessage(
            msg_type=MessageType.GOSSIP_PING,
            name="",
            payload=b"",
        )
        data = msg.to_bytes()
        assert len(data) == 1 + 2 + 0 + 4 + 0  # type + name_len + name + payload_len + payload

    def test_to_bytes_unicode_name(self):
        """Test serializing message with unicode name."""
        msg = WireMessage(
            msg_type=MessageType.ACTOR_MSG,
            name="actor-\u00e9\u00e8",
            payload=b"test",
        )
        data = msg.to_bytes()
        # Should not raise, and name should be UTF-8 encoded
        assert len(data) > 0

    @pytest.mark.asyncio
    async def test_roundtrip(self):
        """Test message serialization roundtrip."""
        original = WireMessage(
            msg_type=MessageType.ASK_REQUEST,
            name="request-123",
            payload=b"request data here",
        )

        # Serialize to bytes
        data = original.to_bytes()

        # Create a stream from bytes
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()

        # Read back
        restored = await WireMessage.from_reader(reader)

        assert restored.msg_type == original.msg_type
        assert restored.name == original.name
        assert restored.payload == original.payload


class TestConnection:
    """Tests for Connection."""

    def test_connection_creation(self):
        """Test creating a connection."""
        conn = Connection("localhost", 8001)
        assert conn.address == ("localhost", 8001)
        assert conn.is_connected is False

    @pytest.mark.asyncio
    async def test_connect_to_invalid_address(self):
        """Test connecting to invalid address fails gracefully."""
        conn = Connection("invalid.host.example", 99999)
        # Should not raise, just return False
        result = await conn.connect()
        assert result is False
        assert conn.is_connected is False

    @pytest.mark.asyncio
    async def test_close_not_connected(self):
        """Test closing a connection that's not connected."""
        conn = Connection("localhost", 8001)
        # Should not raise
        await conn.close()
        assert conn.is_connected is False


class TestTransport:
    """Tests for Transport."""

    def test_transport_creation(self):
        """Test creating a transport."""
        transport = Transport("node-1", "localhost", 8001)
        assert transport.node_id == "node-1"
        assert transport.address == ("localhost", 8001)

    def test_add_peer(self):
        """Test adding a peer."""
        transport = Transport("node-1", "localhost", 8001)
        transport.add_peer("node-2", "localhost", 8002)
        assert "node-2" in transport._peer_addresses
        assert transport._peer_addresses["node-2"] == ("localhost", 8002)

    def test_add_self_ignored(self):
        """Test that adding self as peer is ignored."""
        transport = Transport("node-1", "localhost", 8001)
        transport.add_peer("node-1", "localhost", 8001)
        assert "node-1" not in transport._peer_addresses

    def test_remove_peer(self):
        """Test removing a peer."""
        transport = Transport("node-1", "localhost", 8001)
        transport.add_peer("node-2", "localhost", 8002)
        transport.remove_peer("node-2")
        assert "node-2" not in transport._peer_addresses

    def test_remove_nonexistent_peer(self):
        """Test removing a peer that doesn't exist."""
        transport = Transport("node-1", "localhost", 8001)
        # Should not raise
        transport.remove_peer("node-999")

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Test starting and stopping transport."""
        transport = Transport("node-1", "127.0.0.1", 0)  # Port 0 = random available

        await transport.start()
        assert transport._running is True
        assert transport._server is not None

        await transport.stop()
        assert transport._running is False
        assert transport._server is None

    def test_set_handler(self):
        """Test setting a message handler."""
        transport = Transport("node-1", "localhost", 8001)

        async def handler(msg, addr):
            return None

        transport.set_handler(MessageType.ACTOR_MSG, handler)
        assert MessageType.ACTOR_MSG in transport._handlers

    def test_repr(self):
        """Test string representation."""
        transport = Transport("node-1", "localhost", 8001)
        transport.add_peer("node-2", "localhost", 8002)

        r = repr(transport)
        assert "Transport" in r
        assert "node-1" in r


class TestTransportIntegration:
    """Integration tests for Transport (requires actual network)."""

    @pytest.mark.asyncio
    async def test_two_transports_communicate(self):
        """Test two transports can communicate."""
        # Create two transports
        t1 = Transport("node-1", "127.0.0.1", 0)
        t2 = Transport("node-2", "127.0.0.1", 0)

        received = []

        async def handle_actor_msg(msg, addr):
            received.append(msg)
            return None

        t1.set_handler(MessageType.ACTOR_MSG, handle_actor_msg)

        try:
            # Start both
            await t1.start()
            await t2.start()

            # Get actual port assigned to t1
            t1_port = t1._server.sockets[0].getsockname()[1]

            # Add t1 as peer to t2
            t2.add_peer("node-1", "127.0.0.1", t1_port)

            # Send message from t2 to t1
            from casty.cluster.serialize import serialize

            msg = WireMessage(
                msg_type=MessageType.ACTOR_MSG,
                name="test-actor",
                payload=serialize({"hello": "world"}),
            )
            result = await t2._send_to_peer("node-1", msg)
            assert result is True

            # Wait for message to be processed
            await asyncio.sleep(0.1)

            assert len(received) == 1
            assert received[0].name == "test-actor"

        finally:
            await t1.stop()
            await t2.stop()

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Request-response requires bidirectional connection handling - to be implemented in distributed.py")
    async def test_request_response_pattern(self):
        """Test request-response pattern between transports.

        Note: This test is skipped because the current transport implementation
        doesn't handle bidirectional communication on client connections.
        The full request-response pattern will be implemented in the
        DistributedActorSystem where peers maintain persistent bidirectional
        connections.
        """
        t1 = Transport("node-1", "127.0.0.1", 0)
        t2 = Transport("node-2", "127.0.0.1", 0)

        from casty.cluster.serialize import serialize

        async def handle_ask(msg, addr):
            # Name format is "actor_name:request_id"
            # Extract request ID (everything after first colon)
            parts = msg.name.split(":", 1)
            request_id = parts[1] if len(parts) > 1 else msg.name

            # Echo back with response type
            return WireMessage(
                msg_type=MessageType.ASK_RESPONSE,
                name=request_id,
                payload=serialize({"response": "ok"}),
            )

        t1.set_handler(MessageType.ASK_REQUEST, handle_ask)

        try:
            await t1.start()
            await t2.start()

            t1_port = t1._server.sockets[0].getsockname()[1]
            t2.add_peer("node-1", "127.0.0.1", t1_port)

            # Send ask from t2 to t1
            response = await t2.ask_actor("node-1", "test-actor", {"query": "test"}, timeout=2.0)
            assert response == {"response": "ok"}

        finally:
            await t1.stop()
            await t2.stop()
