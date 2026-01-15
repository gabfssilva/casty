"""Tests for the declarative codec system."""

import pytest
from casty.codec import (
    serializable,
    encode,
    decode,
    decode_with_type,
    ProtocolCodec,
    get_message_id,
    is_serializable,
    _MESSAGE_REGISTRY,
    _CLASS_TO_ID,
)


# =============================================================================
# Test Messages (using fresh IDs to avoid conflicts)
# =============================================================================


@serializable(0xF0)
class SimpleMessage:
    name: str
    count: int


@serializable(0xF1)
class MessageWithBytes:
    data: bytes
    label: str


@serializable(0xF2)
class MessageWithTuple:
    address: tuple[str, int]
    name: str


@serializable(0xF3)
class MessageWithOptional:
    value: str
    extra: str | None = None


@serializable(0xF4)
class MessageWithList:
    items: list[str]
    count: int


@serializable(0xF5)
class MessageWithNestedTuples:
    addresses: list[tuple[str, int]]


# =============================================================================
# Basic Tests
# =============================================================================


class TestSerializableDecorator:
    def test_auto_dataclass(self):
        """@serializable should auto-apply @dataclass."""
        msg = SimpleMessage(name="test", count=42)
        assert msg.name == "test"
        assert msg.count == 42

    def test_frozen(self):
        """Auto-dataclass should be frozen by default."""
        msg = SimpleMessage(name="test", count=42)
        with pytest.raises(AttributeError):
            msg.name = "changed"  # type: ignore

    def test_registration(self):
        """Class should be registered in both directions."""
        assert _MESSAGE_REGISTRY[0xF0] is SimpleMessage
        assert _CLASS_TO_ID[SimpleMessage] == 0xF0

    def test_get_message_id(self):
        """get_message_id should return the ID."""
        assert get_message_id(SimpleMessage) == 0xF0
        assert get_message_id(str) is None  # Not registered

    def test_is_serializable(self):
        """is_serializable should check registration."""
        assert is_serializable(SimpleMessage)
        assert not is_serializable(str)

    def test_message_id_on_class(self):
        """Message ID should be stored on class."""
        assert SimpleMessage.__casty_message_id__ == 0xF0


class TestEncodeDecode:
    def test_simple_roundtrip(self):
        """Basic encode/decode should work."""
        original = SimpleMessage(name="hello", count=123)
        data = encode(original)
        decoded = decode(data)

        assert decoded == original
        assert isinstance(decoded, SimpleMessage)

    def test_bytes_field(self):
        """bytes fields should work."""
        original = MessageWithBytes(data=b"\x00\x01\x02", label="binary")
        data = encode(original)
        decoded = decode(data)

        assert decoded == original
        assert decoded.data == b"\x00\x01\x02"

    def test_tuple_field(self):
        """tuple fields should be converted to/from list."""
        original = MessageWithTuple(address=("localhost", 8080), name="server")
        data = encode(original)
        decoded = decode(data)

        assert decoded == original
        assert decoded.address == ("localhost", 8080)
        assert isinstance(decoded.address, tuple)

    def test_optional_field_with_value(self):
        """Optional field with value should work."""
        original = MessageWithOptional(value="main", extra="bonus")
        data = encode(original)
        decoded = decode(data)

        assert decoded == original

    def test_optional_field_none(self):
        """Optional field with None should work."""
        original = MessageWithOptional(value="main", extra=None)
        data = encode(original)
        decoded = decode(data)

        assert decoded == original
        assert decoded.extra is None

    def test_list_field(self):
        """list fields should work."""
        original = MessageWithList(items=["a", "b", "c"], count=3)
        data = encode(original)
        decoded = decode(data)

        assert decoded == original
        assert decoded.items == ["a", "b", "c"]

    def test_nested_tuples_in_list(self):
        """list[tuple] should work."""
        original = MessageWithNestedTuples(
            addresses=[("host1", 8001), ("host2", 8002)]
        )
        data = encode(original)
        decoded = decode(data)

        assert decoded == original
        assert decoded.addresses[0] == ("host1", 8001)
        assert isinstance(decoded.addresses[0], tuple)

    def test_decode_with_type(self):
        """decode_with_type should verify type."""
        original = SimpleMessage(name="test", count=1)
        data = encode(original)

        decoded = decode_with_type(data, SimpleMessage)
        assert decoded == original

    def test_decode_with_type_wrong_type(self):
        """decode_with_type should raise on wrong type."""
        original = SimpleMessage(name="test", count=1)
        data = encode(original)

        with pytest.raises(ValueError, match="Expected MessageWithBytes"):
            decode_with_type(data, MessageWithBytes)


class TestErrors:
    def test_encode_unregistered(self):
        """Encoding unregistered type should raise."""

        class NotRegistered:
            pass

        with pytest.raises(ValueError, match="not @serializable"):
            encode(NotRegistered())

    def test_decode_unknown_id(self):
        """Decoding unknown message ID should raise."""
        data = b"\xFF\x80"  # Unknown ID 0xFF
        with pytest.raises(ValueError, match="Unknown message_id"):
            decode(data)

    def test_decode_empty(self):
        """Decoding empty data should raise."""
        with pytest.raises(ValueError, match="too short"):
            decode(b"")

    def test_decode_no_payload(self):
        """Decoding with no payload should raise."""
        with pytest.raises(ValueError, match="Empty payload"):
            decode(b"\xF0")  # Just the message ID

    def test_duplicate_message_id(self):
        """Registering duplicate ID should raise."""
        with pytest.raises(ValueError, match="already registered"):

            @serializable(0xF0)  # Already used by SimpleMessage
            class Duplicate:
                value: int

    def test_message_id_out_of_range(self):
        """Message ID outside 0x00-0xFF should raise."""
        with pytest.raises(ValueError, match="must be 0x00-0xFF"):

            @serializable(0x100)
            class OutOfRange:
                value: int


# =============================================================================
# Protocol Codec Tests
# =============================================================================


class TestProtocolCodec:
    def test_isolated_namespace(self):
        """Each ProtocolCodec should have its own namespace."""
        proto1 = ProtocolCodec("proto1")
        proto2 = ProtocolCodec("proto2")

        @proto1.serializable(0x01)
        class Message1:
            value: str

        @proto2.serializable(0x01)  # Same ID, different protocol
        class Message2:
            count: int

        msg1 = Message1(value="hello")
        msg2 = Message2(count=42)

        # Each protocol can encode its own messages
        data1 = proto1.encode(msg1)
        data2 = proto2.encode(msg2)

        # And decode them
        assert proto1.decode(data1) == msg1
        assert proto2.decode(data2) == msg2

    def test_cross_protocol_error(self):
        """Encoding with wrong protocol should raise."""
        proto1 = ProtocolCodec("proto1")
        proto2 = ProtocolCodec("proto2")

        @proto1.serializable(0x01)
        class OnlyInProto1:
            value: str

        msg = OnlyInProto1(value="test")

        # Should work with proto1
        data = proto1.encode(msg)
        assert proto1.decode(data) == msg

        # Should fail with proto2
        with pytest.raises(ValueError, match="not registered with protocol"):
            proto2.encode(msg)

    def test_protocol_name_on_class(self):
        """Protocol name should be stored on class."""
        proto = ProtocolCodec("my_proto")

        @proto.serializable(0x42)
        class MyMessage:
            value: int

        assert MyMessage.__casty_protocol__ == "my_proto"

    def test_protocol_get_registry(self):
        """get_registry should return protocol's registry."""
        proto = ProtocolCodec("test_proto")

        @proto.serializable(0x01)
        class Msg1:
            a: int

        @proto.serializable(0x02)
        class Msg2:
            b: str

        registry = proto.get_registry()
        assert registry[0x01] is Msg1
        assert registry[0x02] is Msg2
        assert len(registry) == 2

    def test_protocol_tuple_handling(self):
        """Protocol codec should handle tuple conversion."""
        proto = ProtocolCodec("tuple_proto")

        @proto.serializable(0x01)
        class WithTuple:
            addr: tuple[str, int]

        original = WithTuple(addr=("host", 9000))
        data = proto.encode(original)
        decoded = proto.decode(data)

        assert decoded == original
        assert isinstance(decoded.addr, tuple)


# =============================================================================
# Wire Format Tests
# =============================================================================


class TestWireFormat:
    def test_first_byte_is_message_id(self):
        """First byte should be the message ID."""
        msg = SimpleMessage(name="x", count=1)
        data = encode(msg)

        assert data[0] == 0xF0

    def test_payload_is_msgpack(self):
        """Payload should be valid msgpack."""
        import msgpack

        msg = SimpleMessage(name="test", count=99)
        data = encode(msg)

        payload = data[1:]
        unpacked = msgpack.unpackb(payload, raw=False)

        assert unpacked["name"] == "test"
        assert unpacked["count"] == 99
