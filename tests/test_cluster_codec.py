"""Tests for ClusterCodec with protocol multiplexing."""

import pytest

from casty.cluster.codec import ClusterCodec, ProtocolType
from casty.cluster.messages import (
    ActorMessage,
    AskRequest,
    AskResponse,
    NodeHandshake,
    NodeHandshakeAck,
)


class TestClusterCodecActorMessages:
    """Test encoding/decoding of ACTOR protocol messages."""

    def test_encode_decode_actor_message(self):
        """ActorMessage roundtrip."""
        msg = ActorMessage(
            target_actor="worker-1",
            payload_type="myapp.messages.DoWork",
            payload=b"\x82\xa3foo\x01\xa3bar\x02",
        )

        data = ClusterCodec.encode(msg)
        protocol, decoded, remaining = ClusterCodec.decode(data)

        assert protocol == ProtocolType.ACTOR
        assert isinstance(decoded, ActorMessage)
        assert decoded.target_actor == "worker-1"
        assert decoded.payload_type == "myapp.messages.DoWork"
        assert decoded.payload == b"\x82\xa3foo\x01\xa3bar\x02"
        assert remaining == b""

    def test_encode_decode_ask_request(self):
        """AskRequest roundtrip."""
        msg = AskRequest(
            request_id="node-1-42",
            target_actor="counter",
            payload_type="myapp.messages.GetCount",
            payload=b"\x80",
        )

        data = ClusterCodec.encode(msg)
        protocol, decoded, remaining = ClusterCodec.decode(data)

        assert protocol == ProtocolType.ACTOR
        assert isinstance(decoded, AskRequest)
        assert decoded.request_id == "node-1-42"
        assert decoded.target_actor == "counter"
        assert remaining == b""

    def test_encode_decode_ask_response_success(self):
        """AskResponse success roundtrip."""
        msg = AskResponse(
            request_id="node-1-42",
            success=True,
            payload_type="builtins.int",
            payload=b"\xcd\x00\x2a",
        )

        data = ClusterCodec.encode(msg)
        protocol, decoded, remaining = ClusterCodec.decode(data)

        assert protocol == ProtocolType.ACTOR
        assert isinstance(decoded, AskResponse)
        assert decoded.request_id == "node-1-42"
        assert decoded.success is True
        assert decoded.payload_type == "builtins.int"

    def test_encode_decode_ask_response_failure(self):
        """AskResponse failure roundtrip."""
        msg = AskResponse(
            request_id="node-1-42",
            success=False,
            payload_type=None,
            payload=b"Actor not found",
        )

        data = ClusterCodec.encode(msg)
        protocol, decoded, remaining = ClusterCodec.decode(data)

        assert protocol == ProtocolType.ACTOR
        assert isinstance(decoded, AskResponse)
        assert decoded.success is False
        assert decoded.payload_type is None
        assert decoded.payload == b"Actor not found"


class TestClusterCodecHandshakeMessages:
    """Test encoding/decoding of HANDSHAKE protocol messages."""

    def test_encode_decode_node_handshake(self):
        """NodeHandshake roundtrip."""
        msg = NodeHandshake(
            node_id="node-abc123",
            address=("192.168.1.10", 7946),
            protocol_version=1,
        )

        data = ClusterCodec.encode(msg)
        protocol, decoded, remaining = ClusterCodec.decode(data)

        assert protocol == ProtocolType.HANDSHAKE
        assert isinstance(decoded, NodeHandshake)
        assert decoded.node_id == "node-abc123"
        assert decoded.address == ("192.168.1.10", 7946)
        assert decoded.protocol_version == 1
        assert remaining == b""

    def test_encode_decode_node_handshake_ack_accepted(self):
        """NodeHandshakeAck accepted roundtrip."""
        msg = NodeHandshakeAck(
            node_id="node-def456",
            address=("192.168.1.20", 7946),
            accepted=True,
            reason=None,
        )

        data = ClusterCodec.encode(msg)
        protocol, decoded, remaining = ClusterCodec.decode(data)

        assert protocol == ProtocolType.HANDSHAKE
        assert isinstance(decoded, NodeHandshakeAck)
        assert decoded.node_id == "node-def456"
        assert decoded.accepted is True
        assert decoded.reason is None

    def test_encode_decode_node_handshake_ack_rejected(self):
        """NodeHandshakeAck rejected roundtrip."""
        msg = NodeHandshakeAck(
            node_id="node-def456",
            address=("192.168.1.20", 7946),
            accepted=False,
            reason="Protocol version mismatch",
        )

        data = ClusterCodec.encode(msg)
        protocol, decoded, remaining = ClusterCodec.decode(data)

        assert protocol == ProtocolType.HANDSHAKE
        assert isinstance(decoded, NodeHandshakeAck)
        assert decoded.accepted is False
        assert decoded.reason == "Protocol version mismatch"


class TestClusterCodecFraming:
    """Test framing and buffer handling."""

    def test_decode_incomplete_header(self):
        """Incomplete header returns None."""
        data = b"\x00\x00\x00"  # Only 3 bytes, need at least 5

        protocol, msg, remaining = ClusterCodec.decode(data)

        assert protocol is None
        assert msg is None
        assert remaining == data

    def test_decode_incomplete_payload(self):
        """Incomplete payload returns None."""
        msg = NodeHandshake("test", ("127.0.0.1", 8000))
        full_data = ClusterCodec.encode(msg)

        # Truncate the data
        partial = full_data[:10]

        protocol, decoded, remaining = ClusterCodec.decode(partial)

        assert protocol is None
        assert decoded is None
        assert remaining == partial

    def test_decode_all_multiple_messages(self):
        """Decode multiple messages from buffer."""
        msg1 = NodeHandshake("node-1", ("127.0.0.1", 8001))
        msg2 = NodeHandshakeAck("node-2", ("127.0.0.1", 8002), True)
        msg3 = ActorMessage("worker", "test.Msg", b"\x80")

        data = ClusterCodec.encode(msg1) + ClusterCodec.encode(msg2) + ClusterCodec.encode(msg3)

        messages, remaining = ClusterCodec.decode_all(data)

        assert len(messages) == 3
        assert messages[0][0] == ProtocolType.HANDSHAKE
        assert isinstance(messages[0][1], NodeHandshake)
        assert messages[1][0] == ProtocolType.HANDSHAKE
        assert isinstance(messages[1][1], NodeHandshakeAck)
        assert messages[2][0] == ProtocolType.ACTOR
        assert isinstance(messages[2][1], ActorMessage)
        assert remaining == b""

    def test_decode_all_with_remaining(self):
        """Decode all with incomplete trailing message."""
        msg1 = NodeHandshake("node-1", ("127.0.0.1", 8001))
        msg2 = ActorMessage("worker", "test.Msg", b"\x80")

        full_data = ClusterCodec.encode(msg1) + ClusterCodec.encode(msg2)
        # Add partial data at end
        data = full_data + b"\x00\x00\x00\x10"

        messages, remaining = ClusterCodec.decode_all(data)

        assert len(messages) == 2
        assert remaining == b"\x00\x00\x00\x10"

    def test_message_too_large(self):
        """Message exceeding max size raises error."""
        # Create a fake header with large length
        import struct
        data = struct.pack(">I", 100 * 1024 * 1024)  # 100MB
        data += b"\x02\x01"  # protocol + type

        with pytest.raises(ValueError, match="too large"):
            ClusterCodec.decode(data)


class TestClusterCodecSwimWrapping:
    """Test SWIM protocol wrapping."""

    def test_encode_swim_frame(self):
        """Wrap SWIM data with cluster framing."""
        swim_data = b"\x00\x00\x00\x05\x01\x82\xa1a\x01"  # Fake SWIM frame

        data = ClusterCodec.encode_swim_frame(swim_data)

        # Decode and verify protocol
        protocol, raw_swim, remaining = ClusterCodec.decode(data)

        assert protocol == ProtocolType.SWIM
        assert raw_swim == swim_data
        assert remaining == b""
