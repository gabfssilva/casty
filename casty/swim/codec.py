"""SWIM+ Wire Protocol Codec.

Wire format:
    [length: 4 bytes BE][type: 1 byte][payload: msgpack]

This codec follows the same pattern as casty.gossip.codec for consistency.
"""

from __future__ import annotations

import struct
from enum import IntEnum
from typing import Any

import msgpack

from .messages import (
    Ping,
    Ack,
    PingReq,
    IndirectAck,
    Nack,
    GossipEntry,
    GossipType,
    WireMessage,
)


class SwimMessageType(IntEnum):
    """Wire protocol message types for SWIM."""

    PING = 1
    ACK = 2
    PING_REQ = 3
    INDIRECT_ACK = 4
    NACK = 5


# Message type to class mapping
_MESSAGE_CLASSES: dict[SwimMessageType, type] = {
    SwimMessageType.PING: Ping,
    SwimMessageType.ACK: Ack,
    SwimMessageType.PING_REQ: PingReq,
    SwimMessageType.INDIRECT_ACK: IndirectAck,
    SwimMessageType.NACK: Nack,
}

_CLASS_TO_TYPE: dict[type, SwimMessageType] = {v: k for k, v in _MESSAGE_CLASSES.items()}


def _encode_gossip_entry(entry: GossipEntry) -> dict[str, Any]:
    """Encode a GossipEntry for msgpack."""
    return {
        "type": entry.type.value,
        "node_id": entry.node_id,
        "incarnation": entry.incarnation,
        "address": list(entry.address) if entry.address else None,
        "timestamp": entry.timestamp,
        "local_seq": entry.local_seq,
    }


def _decode_gossip_entry(data: dict[str, Any]) -> GossipEntry:
    """Decode a GossipEntry from msgpack."""
    return GossipEntry(
        type=GossipType(data["type"]),
        node_id=data["node_id"],
        incarnation=data["incarnation"],
        address=tuple(data["address"]) if data["address"] else None,
        timestamp=data["timestamp"],
        local_seq=data["local_seq"],
    )


def _encode_message(msg: WireMessage) -> dict[str, Any]:
    """Encode a wire message to dict."""
    match msg:
        case Ping():
            return {
                "source": msg.source,
                "source_incarnation": msg.source_incarnation,
                "sequence": msg.sequence,
                "piggyback": [_encode_gossip_entry(e) for e in msg.piggyback],
            }
        case Ack():
            return {
                "source": msg.source,
                "source_incarnation": msg.source_incarnation,
                "sequence": msg.sequence,
                "piggyback": [_encode_gossip_entry(e) for e in msg.piggyback],
            }
        case PingReq():
            return {
                "source": msg.source,
                "target": msg.target,
                "sequence": msg.sequence,
            }
        case IndirectAck():
            return {
                "original_source": msg.original_source,
                "target": msg.target,
                "sequence": msg.sequence,
                "target_incarnation": msg.target_incarnation,
            }
        case Nack():
            return {
                "suspected": msg.suspected,
                "reporters": list(msg.reporters),
                "first_suspected_at": msg.first_suspected_at,
            }
        case _:
            raise ValueError(f"Unknown message type: {type(msg)}")


def _decode_message(msg_type: SwimMessageType, data: dict[str, Any]) -> WireMessage:
    """Decode a wire message from type and dict."""
    match msg_type:
        case SwimMessageType.PING:
            piggyback = tuple(_decode_gossip_entry(e) for e in data.get("piggyback", []))
            return Ping(
                source=data["source"],
                source_incarnation=data["source_incarnation"],
                sequence=data["sequence"],
                piggyback=piggyback,
            )
        case SwimMessageType.ACK:
            piggyback = tuple(_decode_gossip_entry(e) for e in data.get("piggyback", []))
            return Ack(
                source=data["source"],
                source_incarnation=data["source_incarnation"],
                sequence=data["sequence"],
                piggyback=piggyback,
            )
        case SwimMessageType.PING_REQ:
            return PingReq(
                source=data["source"],
                target=data["target"],
                sequence=data["sequence"],
            )
        case SwimMessageType.INDIRECT_ACK:
            return IndirectAck(
                original_source=data["original_source"],
                target=data["target"],
                sequence=data["sequence"],
                target_incarnation=data["target_incarnation"],
            )
        case SwimMessageType.NACK:
            return Nack(
                suspected=data["suspected"],
                reporters=tuple(data["reporters"]),
                first_suspected_at=data["first_suspected_at"],
            )
        case _:
            raise ValueError(f"Unknown message type: {msg_type}")


class SwimCodec:
    """Codec for SWIM+ protocol messages.

    Wire format:
        [length: 4 bytes BE][type: 1 byte][payload: msgpack]

    Example:
        # Encoding
        data = SwimCodec.encode(Ping("node-1", 1, 0, ()))

        # Decoding
        msg, remaining = SwimCodec.decode(data)
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

        payload_dict = _encode_message(msg)
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
        if len(data) < SwimCodec.HEADER_SIZE:
            return None, data

        length = struct.unpack(">I", data[:4])[0]

        if length > SwimCodec.MAX_MESSAGE_SIZE:
            raise ValueError(f"Message too large: {length} > {SwimCodec.MAX_MESSAGE_SIZE}")

        if len(data) < 4 + length:
            return None, data

        msg_type_value = data[4]
        try:
            msg_type = SwimMessageType(msg_type_value)
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
            msg, data = SwimCodec.decode(data)
            if msg is None:
                break
            messages.append(msg)
        return messages, data
