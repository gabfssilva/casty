"""Tests for GossipCodec wire protocol."""

import pytest

from casty.gossip.codec import GossipCodec
from casty.gossip.messages import (
    Handshake,
    HandshakeAck,
    GossipPush,
    GossipPushAck,
    SyncRequest,
    SyncResponse,
    SyncData,
    Ping,
    Pong,
)
from casty.gossip.state import StateEntry, StateDigest, EntryStatus
from casty.gossip.versioning import VectorClock


class TestGossipCodec:
    """Tests for GossipCodec encoding/decoding."""

    def test_encode_decode_handshake(self):
        """Roundtrip handshake message."""
        msg = Handshake(
            node_id="node-1",
            address=("192.168.1.10", 9000),
        )

        encoded = GossipCodec.encode(msg)
        decoded, remaining = GossipCodec.decode(encoded)

        assert decoded == msg
        assert remaining == b""

    def test_encode_decode_handshake_ack(self):
        """Roundtrip handshake ack message."""
        msg = HandshakeAck(
            node_id="node-2",
            address=("192.168.1.11", 9000),
            known_peers=(("192.168.1.12", 9000), ("192.168.1.13", 9000)),
        )

        encoded = GossipCodec.encode(msg)
        decoded, remaining = GossipCodec.decode(encoded)

        assert decoded == msg
        assert remaining == b""

    def test_encode_decode_gossip_push(self):
        """Roundtrip gossip push message."""
        clock = VectorClock().increment("node-1")
        entry = StateEntry(
            key="test/key",
            value=b"test value",
            version=clock,
            status=EntryStatus.ACTIVE,
            ttl=60.0,
            origin="node-1",
            timestamp=1234567890.0,
        )

        msg = GossipPush(
            sender_id="node-1",
            sender_address=("localhost", 9000),
            updates=(entry,),
        )

        encoded = GossipCodec.encode(msg)
        decoded, remaining = GossipCodec.decode(encoded)

        assert decoded.sender_id == msg.sender_id
        assert decoded.sender_address == msg.sender_address
        assert len(decoded.updates) == 1
        assert decoded.updates[0].key == entry.key
        assert decoded.updates[0].value == entry.value
        assert decoded.updates[0].version == entry.version
        assert remaining == b""

    def test_encode_decode_gossip_push_ack(self):
        """Roundtrip gossip push ack message."""
        clock = VectorClock().increment("node-1")
        digest = StateDigest(
            entries=(("key1", clock),),
            node_id="node-1",
            generation=1,
        )

        msg = GossipPushAck(
            sender_id="node-1",
            received_count=5,
            digest=digest,
        )

        encoded = GossipCodec.encode(msg)
        decoded, remaining = GossipCodec.decode(encoded)

        assert decoded.sender_id == msg.sender_id
        assert decoded.received_count == msg.received_count
        assert decoded.digest is not None
        assert decoded.digest.node_id == digest.node_id
        assert remaining == b""

    def test_encode_decode_sync_request(self):
        """Roundtrip sync request message."""
        clock = VectorClock().increment("node-1")
        digest = StateDigest(
            entries=(("key1", clock), ("key2", clock)),
            node_id="node-1",
            generation=5,
        )

        msg = SyncRequest(
            sender_id="node-1",
            sender_address=("localhost", 9000),
            digest=digest,
        )

        encoded = GossipCodec.encode(msg)
        decoded, remaining = GossipCodec.decode(encoded)

        assert decoded.sender_id == msg.sender_id
        assert decoded.digest.generation == digest.generation
        assert len(decoded.digest.entries) == 2
        assert remaining == b""

    def test_encode_decode_sync_response(self):
        """Roundtrip sync response message."""
        clock = VectorClock().increment("node-2")
        entry = StateEntry(
            key="new/key",
            value=b"new value",
            version=clock,
        )

        msg = SyncResponse(
            sender_id="node-2",
            entries_for_you=(entry,),
            keys_i_need=("missing/key1", "missing/key2"),
        )

        encoded = GossipCodec.encode(msg)
        decoded, remaining = GossipCodec.decode(encoded)

        assert decoded.sender_id == msg.sender_id
        assert len(decoded.entries_for_you) == 1
        assert decoded.keys_i_need == ("missing/key1", "missing/key2")
        assert remaining == b""

    def test_encode_decode_sync_data(self):
        """Roundtrip sync data message."""
        clock = VectorClock().increment("node-1")
        entries = (
            StateEntry(key="key1", value=b"val1", version=clock),
            StateEntry(key="key2", value=b"val2", version=clock),
        )

        msg = SyncData(
            sender_id="node-1",
            entries=entries,
        )

        encoded = GossipCodec.encode(msg)
        decoded, remaining = GossipCodec.decode(encoded)

        assert decoded.sender_id == msg.sender_id
        assert len(decoded.entries) == 2
        assert remaining == b""

    def test_encode_decode_ping_pong(self):
        """Roundtrip ping/pong messages."""
        ping = Ping(sender_id="node-1", sequence=42)
        pong = Pong(sender_id="node-2", sequence=42)

        encoded_ping = GossipCodec.encode(ping)
        decoded_ping, _ = GossipCodec.decode(encoded_ping)
        assert decoded_ping == ping

        encoded_pong = GossipCodec.encode(pong)
        decoded_pong, _ = GossipCodec.decode(encoded_pong)
        assert decoded_pong == pong

    def test_decode_incomplete_data(self):
        """Incomplete data returns None."""
        msg = Handshake(node_id="node-1", address=("localhost", 9000))
        encoded = GossipCodec.encode(msg)

        # Truncate data
        decoded, remaining = GossipCodec.decode(encoded[:5])
        assert decoded is None
        assert remaining == encoded[:5]

    def test_decode_multiple_messages(self):
        """Decode multiple messages from buffer."""
        msg1 = Ping(sender_id="node-1", sequence=1)
        msg2 = Pong(sender_id="node-2", sequence=1)
        msg3 = Ping(sender_id="node-1", sequence=2)

        data = GossipCodec.encode(msg1) + GossipCodec.encode(msg2) + GossipCodec.encode(msg3)

        messages, remaining = GossipCodec.decode_all(data)

        assert len(messages) == 3
        assert messages[0] == msg1
        assert messages[1] == msg2
        assert messages[2] == msg3
        assert remaining == b""

    def test_decode_message_too_large(self):
        """Large messages are rejected."""
        # Create oversized data
        data = b"\x00\x10\x00\x00"  # Length = 1MB (too large)
        data += b"\x00" * 100

        with pytest.raises(ValueError, match="too large"):
            GossipCodec.decode(data)

    def test_unknown_message_type(self):
        """Unknown message type raises error."""
        import struct
        # Valid length, invalid type
        data = struct.pack(">I", 10) + bytes([0xFF]) + b"x" * 9

        with pytest.raises(ValueError, match="Unknown message type"):
            GossipCodec.decode(data)
