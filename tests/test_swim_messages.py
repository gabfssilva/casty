"""Tests for SWIM+ messages and codec."""

import pytest
import time

from casty.swim.messages import (
    GossipType,
    GossipEntry,
    Ping,
    Ack,
    PingReq,
    IndirectAck,
    Nack,
)
from casty.swim.codec import SwimCodec, SwimMessageType


class TestGossipEntry:
    """Tests for GossipEntry dataclass."""

    def test_create_alive_entry(self):
        entry = GossipEntry(
            type=GossipType.ALIVE,
            node_id="node-1",
            incarnation=5,
            address=("127.0.0.1", 7946),
            timestamp=time.time(),
            local_seq=1,
        )
        assert entry.type == GossipType.ALIVE
        assert entry.node_id == "node-1"
        assert entry.incarnation == 5

    def test_create_suspect_entry(self):
        entry = GossipEntry(
            type=GossipType.SUSPECT,
            node_id="node-2",
            incarnation=3,
            address=None,
            timestamp=time.time(),
            local_seq=2,
        )
        assert entry.type == GossipType.SUSPECT
        assert entry.address is None


class TestWireMessages:
    """Tests for wire protocol messages."""

    def test_ping_message(self):
        ping = Ping(
            source="node-1",
            source_incarnation=5,
            sequence=42,
            piggyback=(),
        )
        assert ping.source == "node-1"
        assert ping.sequence == 42

    def test_ping_with_piggyback(self):
        entry = GossipEntry(
            type=GossipType.ALIVE,
            node_id="node-2",
            incarnation=1,
            address=("127.0.0.1", 7946),
            timestamp=time.time(),
            local_seq=1,
        )
        ping = Ping(
            source="node-1",
            source_incarnation=5,
            sequence=42,
            piggyback=(entry,),
        )
        assert len(ping.piggyback) == 1

    def test_ack_message(self):
        ack = Ack(
            source="node-1",
            source_incarnation=5,
            sequence=42,
            piggyback=(),
        )
        assert ack.source == "node-1"
        assert ack.sequence == 42

    def test_ping_req_message(self):
        ping_req = PingReq(
            source="node-1",
            target="node-2",
            sequence=42,
        )
        assert ping_req.source == "node-1"
        assert ping_req.target == "node-2"

    def test_indirect_ack_message(self):
        indirect_ack = IndirectAck(
            original_source="node-1",
            target="node-2",
            sequence=42,
            target_incarnation=3,
        )
        assert indirect_ack.original_source == "node-1"
        assert indirect_ack.target_incarnation == 3

    def test_nack_message(self):
        nack = Nack(
            suspected="node-2",
            reporters=("node-1", "node-3"),
            first_suspected_at=time.time(),
        )
        assert nack.suspected == "node-2"
        assert len(nack.reporters) == 2


class TestSwimCodec:
    """Tests for SWIM+ wire protocol codec."""

    def test_encode_decode_ping(self):
        original = Ping(
            source="node-1",
            source_incarnation=5,
            sequence=42,
            piggyback=(),
        )

        encoded = SwimCodec.encode(original)
        decoded, remaining = SwimCodec.decode(encoded)

        assert remaining == b""
        assert isinstance(decoded, Ping)
        assert decoded.source == original.source
        assert decoded.source_incarnation == original.source_incarnation
        assert decoded.sequence == original.sequence

    def test_encode_decode_ping_with_piggyback(self):
        entry = GossipEntry(
            type=GossipType.ALIVE,
            node_id="node-2",
            incarnation=1,
            address=("127.0.0.1", 7946),
            timestamp=1234567890.0,
            local_seq=1,
        )
        original = Ping(
            source="node-1",
            source_incarnation=5,
            sequence=42,
            piggyback=(entry,),
        )

        encoded = SwimCodec.encode(original)
        decoded, remaining = SwimCodec.decode(encoded)

        assert isinstance(decoded, Ping)
        assert len(decoded.piggyback) == 1
        assert decoded.piggyback[0].node_id == "node-2"
        assert decoded.piggyback[0].type == GossipType.ALIVE

    def test_encode_decode_ack(self):
        original = Ack(
            source="node-1",
            source_incarnation=5,
            sequence=42,
            piggyback=(),
        )

        encoded = SwimCodec.encode(original)
        decoded, remaining = SwimCodec.decode(encoded)

        assert isinstance(decoded, Ack)
        assert decoded.source == original.source
        assert decoded.sequence == original.sequence

    def test_encode_decode_ping_req(self):
        original = PingReq(
            source="node-1",
            target="node-2",
            sequence=42,
        )

        encoded = SwimCodec.encode(original)
        decoded, remaining = SwimCodec.decode(encoded)

        assert isinstance(decoded, PingReq)
        assert decoded.source == original.source
        assert decoded.target == original.target
        assert decoded.sequence == original.sequence

    def test_encode_decode_indirect_ack(self):
        original = IndirectAck(
            original_source="node-1",
            target="node-2",
            sequence=42,
            target_incarnation=3,
        )

        encoded = SwimCodec.encode(original)
        decoded, remaining = SwimCodec.decode(encoded)

        assert isinstance(decoded, IndirectAck)
        assert decoded.original_source == original.original_source
        assert decoded.target_incarnation == original.target_incarnation

    def test_encode_decode_nack(self):
        original = Nack(
            suspected="node-2",
            reporters=("node-1", "node-3"),
            first_suspected_at=1234567890.0,
        )

        encoded = SwimCodec.encode(original)
        decoded, remaining = SwimCodec.decode(encoded)

        assert isinstance(decoded, Nack)
        assert decoded.suspected == original.suspected
        assert decoded.reporters == original.reporters

    def test_decode_incomplete_message(self):
        """Test decoding incomplete data."""
        original = Ping(
            source="node-1",
            source_incarnation=5,
            sequence=42,
            piggyback=(),
        )

        encoded = SwimCodec.encode(original)

        # Try to decode partial data
        decoded, remaining = SwimCodec.decode(encoded[:3])
        assert decoded is None
        assert remaining == encoded[:3]

    def test_decode_multiple_messages(self):
        """Test decoding multiple messages in buffer."""
        msg1 = Ping(source="node-1", source_incarnation=1, sequence=1, piggyback=())
        msg2 = Ack(source="node-2", source_incarnation=2, sequence=1, piggyback=())
        msg3 = PingReq(source="node-1", target="node-3", sequence=2)

        buffer = SwimCodec.encode(msg1) + SwimCodec.encode(msg2) + SwimCodec.encode(msg3)

        messages, remaining = SwimCodec.decode_all(buffer)

        assert len(messages) == 3
        assert isinstance(messages[0], Ping)
        assert isinstance(messages[1], Ack)
        assert isinstance(messages[2], PingReq)
        assert remaining == b""

    def test_message_type_values(self):
        """Test message type enum values."""
        assert SwimMessageType.PING == 1
        assert SwimMessageType.ACK == 2
        assert SwimMessageType.PING_REQ == 3
        assert SwimMessageType.INDIRECT_ACK == 4
        assert SwimMessageType.NACK == 5
