"""
Gossip Wire Protocol Codec.

Wire format:
    [length: 4 bytes BE][type: 1 byte][payload: msgpack]
"""

import struct
from dataclasses import asdict, fields, is_dataclass
from typing import Any

import msgpack

from .messages import (
    WireMessageType,
    Handshake,
    HandshakeAck,
    GossipPush,
    GossipPushAck,
    SyncRequest,
    SyncResponse,
    SyncData,
    Ping,
    Pong,
    WireMessage,
)
from .state import StateDigest, StateEntry, EntryStatus
from .versioning import VectorClock


# Message type to class mapping
_MESSAGE_CLASSES: dict[WireMessageType, type] = {
    WireMessageType.HANDSHAKE: Handshake,
    WireMessageType.HANDSHAKE_ACK: HandshakeAck,
    WireMessageType.PUSH: GossipPush,
    WireMessageType.PUSH_ACK: GossipPushAck,
    WireMessageType.SYNC_REQUEST: SyncRequest,
    WireMessageType.SYNC_RESPONSE: SyncResponse,
    WireMessageType.SYNC_DATA: SyncData,
    WireMessageType.PING: Ping,
    WireMessageType.PONG: Pong,
}

_CLASS_TO_TYPE: dict[type, WireMessageType] = {v: k for k, v in _MESSAGE_CLASSES.items()}


def _encode_value(obj: Any) -> Any:
    """Encode a value for msgpack serialization."""
    if isinstance(obj, VectorClock):
        return {"__type__": "VectorClock", "entries": list(obj.entries)}
    elif isinstance(obj, StateEntry):
        return {
            "__type__": "StateEntry",
            "key": obj.key,
            "value": obj.value,
            "version": _encode_value(obj.version),
            "status": obj.status.name,
            "ttl": obj.ttl,
            "origin": obj.origin,
            "timestamp": obj.timestamp,
        }
    elif isinstance(obj, StateDigest):
        return {
            "__type__": "StateDigest",
            "entries": [
                [key, _encode_value(version)] for key, version in obj.entries
            ],
            "node_id": obj.node_id,
            "generation": obj.generation,
        }
    elif isinstance(obj, EntryStatus):
        return obj.name
    elif isinstance(obj, (list, tuple)):
        return [_encode_value(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: _encode_value(v) for k, v in obj.items()}
    elif is_dataclass(obj) and not isinstance(obj, type):
        return {
            "__type__": type(obj).__name__,
            **{f.name: _encode_value(getattr(obj, f.name)) for f in fields(obj)},
        }
    return obj


def _decode_value(obj: Any) -> Any:
    """Decode a value from msgpack deserialization."""
    if isinstance(obj, dict) and "__type__" in obj:
        type_name = obj["__type__"]

        if type_name == "VectorClock":
            entries = tuple(tuple(e) for e in obj["entries"])
            return VectorClock(entries=entries)
        elif type_name == "StateEntry":
            return StateEntry(
                key=obj["key"],
                value=obj["value"],
                version=_decode_value(obj["version"]),
                status=EntryStatus[obj["status"]],
                ttl=obj["ttl"],
                origin=obj["origin"],
                timestamp=obj["timestamp"],
            )
        elif type_name == "StateDigest":
            entries = tuple(
                (key, _decode_value(version)) for key, version in obj["entries"]
            )
            return StateDigest(
                entries=entries,
                node_id=obj["node_id"],
                generation=obj["generation"],
            )
        elif type_name in _MESSAGE_CLASSES.values().__class__.__name__:
            # This is a wire message - handled separately
            pass

    elif isinstance(obj, list):
        return [_decode_value(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: _decode_value(v) for k, v in obj.items()}

    return obj


class GossipCodec:
    """Codec for gossip protocol messages.

    Wire format:
        [length: 4 bytes BE][type: 1 byte][payload: msgpack]

    Example:
        # Encoding
        data = GossipCodec.encode(Handshake("node-1", ("localhost", 9000)))

        # Decoding
        msg, remaining = GossipCodec.decode(data)
    """

    HEADER_SIZE = 5  # 4 bytes length + 1 byte type
    MAX_MESSAGE_SIZE = 64 * 1024

    @staticmethod
    def encode(msg: WireMessage) -> bytes:
        """Encode message with framing.

        Args:
            msg: Wire message to encode.

        Returns:
            Framed bytes ready for transmission.

        Raises:
            ValueError: If message type is unknown.
        """
        msg_type = _CLASS_TO_TYPE.get(type(msg))
        if msg_type is None:
            raise ValueError(f"Unknown message type: {type(msg)}")

        # Convert message to dict for serialization
        payload_dict = _encode_value(msg)
        payload = msgpack.packb(payload_dict, use_bin_type=True)

        length = 1 + len(payload)  # type byte + payload
        return struct.pack(">I", length) + bytes([msg_type.value]) + payload

    @staticmethod
    def decode(data: bytes) -> tuple[WireMessage | None, bytes]:
        """Decode one message from buffer.

        Args:
            data: Buffer containing framed message(s).

        Returns:
            Tuple of (decoded message or None if incomplete, remaining bytes).
        """
        if len(data) < GossipCodec.HEADER_SIZE:
            return None, data

        length = struct.unpack(">I", data[:4])[0]

        if length > GossipCodec.MAX_MESSAGE_SIZE:
            raise ValueError(f"Message too large: {length} > {GossipCodec.MAX_MESSAGE_SIZE}")

        if len(data) < 4 + length:
            return None, data

        msg_type_value = data[4]
        try:
            msg_type = WireMessageType(msg_type_value)
        except ValueError:
            raise ValueError(f"Unknown message type: {msg_type_value}")

        payload = data[5 : 4 + length]
        remaining = data[4 + length :]

        payload_dict = msgpack.unpackb(payload, raw=False)
        msg = _decode_message(msg_type, payload_dict)

        return msg, remaining

    @staticmethod
    def decode_all(data: bytes) -> tuple[list[WireMessage], bytes]:
        """Decode all complete messages from buffer.

        Args:
            data: Buffer containing framed message(s).

        Returns:
            Tuple of (list of decoded messages, remaining bytes).
        """
        messages = []
        while True:
            msg, data = GossipCodec.decode(data)
            if msg is None:
                break
            messages.append(msg)
        return messages, data


def _decode_message(msg_type: WireMessageType, data: dict) -> WireMessage:
    """Decode a message from its type and payload dict."""
    # Remove __type__ if present
    data = {k: v for k, v in data.items() if k != "__type__"}

    match msg_type:
        case WireMessageType.HANDSHAKE:
            return Handshake(
                node_id=data["node_id"],
                address=tuple(data["address"]),
            )

        case WireMessageType.HANDSHAKE_ACK:
            known_peers = tuple(tuple(p) for p in data.get("known_peers", []))
            return HandshakeAck(
                node_id=data["node_id"],
                address=tuple(data["address"]),
                known_peers=known_peers,
            )

        case WireMessageType.PUSH:
            updates = tuple(_decode_value(e) for e in data["updates"])
            return GossipPush(
                sender_id=data["sender_id"],
                sender_address=tuple(data["sender_address"]),
                updates=updates,
            )

        case WireMessageType.PUSH_ACK:
            digest = _decode_value(data["digest"]) if data.get("digest") else None
            return GossipPushAck(
                sender_id=data["sender_id"],
                received_count=data["received_count"],
                digest=digest,
            )

        case WireMessageType.SYNC_REQUEST:
            return SyncRequest(
                sender_id=data["sender_id"],
                sender_address=tuple(data["sender_address"]),
                digest=_decode_value(data["digest"]),
            )

        case WireMessageType.SYNC_RESPONSE:
            entries = tuple(_decode_value(e) for e in data["entries_for_you"])
            keys = tuple(data["keys_i_need"])
            return SyncResponse(
                sender_id=data["sender_id"],
                entries_for_you=entries,
                keys_i_need=keys,
            )

        case WireMessageType.SYNC_DATA:
            entries = tuple(_decode_value(e) for e in data["entries"])
            return SyncData(
                sender_id=data["sender_id"],
                entries=entries,
            )

        case WireMessageType.PING:
            return Ping(
                sender_id=data["sender_id"],
                sequence=data["sequence"],
            )

        case WireMessageType.PONG:
            return Pong(
                sender_id=data["sender_id"],
                sequence=data["sequence"],
            )

        case _:
            raise ValueError(f"Unknown message type: {msg_type}")
