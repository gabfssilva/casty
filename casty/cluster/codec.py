"""Cluster Wire Protocol Codec with Protocol Multiplexing.

Wire format:
    [length: 4 bytes BE][protocol: 1 byte][msg_type: 1 byte][payload: msgpack]

Protocol Types:
    0x01 = SWIM (delegated to SwimCodec)
    0x02 = ACTOR (ActorMessage, AskRequest, AskResponse, etc.)
    0x03 = HANDSHAKE (NodeHandshake, NodeHandshakeAck)

This codec allows a single TCP port to handle multiple protocols.

The actual message encoding/decoding is handled by the declarative
@serializable system in wire.py. This module provides the ActorCodec
and HandshakeCodec wrappers that delegate to the protocol codecs.
"""

from __future__ import annotations

import logging

from .wire import (
    actor_protocol,
    handshake_protocol,
    # Re-export all wire messages for backwards compatibility
    ActorMessage,
    AskRequest,
    AskResponse,
    RegisterActor,
    LookupActor,
    LookupResponse,
    EntitySend,
    EntityAskRequest,
    EntityAskResponse,
    SingletonSend,
    SingletonAskRequest,
    SingletonAskResponse,
    ShardedTypeRegister,
    ShardedTypeAck,
    MergeRequest,
    MergeState,
    MergeComplete,
    ReplicateWrite,
    ReplicateWriteAck,
    HintedHandoff,
    ReplicateWALEntry,
    ReplicateWALAck,
    RequestCatchUp,
    CatchUpEntries,
    NodeHandshake,
    NodeHandshakeAck,
    WireActorMessage,
    WireHandshakeMessage,
)

# Keep importing ProtocolType from messages.py for backwards compatibility
from .messages import ProtocolType

log = logging.getLogger(__name__)


# Type alias for any wire message
WireMessage = WireActorMessage | WireHandshakeMessage


# =============================================================================
# ActorCodec - Thin wrapper around actor_protocol
# =============================================================================


class ActorCodec:
    """Simplified codec for actor protocol messages.

    Used by Cluster when receiving ACTOR protocol data from TransportMux.
    TransportMux handles the length framing and protocol byte, so this
    codec only handles the msg_type and payload.

    Wire format:
        [msg_type: 1 byte][payload: msgpack]

    This is now a thin wrapper around the declarative actor_protocol codec.
    """

    @staticmethod
    def encode(msg: WireActorMessage) -> bytes:
        """Encode an actor protocol message.

        Args:
            msg: Wire message to encode.

        Returns:
            Encoded bytes with msg_type and msgpack payload.

        Raises:
            ValueError: If message type is unknown.
        """
        return actor_protocol.encode(msg)

    @staticmethod
    def decode(data: bytes) -> WireActorMessage | None:
        """Decode an actor protocol message.

        Args:
            data: Raw bytes (msg_type + msgpack payload).

        Returns:
            Decoded wire message, or None if data is incomplete.

        Raises:
            ValueError: If message type is unknown.
        """
        if len(data) < 1:
            return None

        try:
            return actor_protocol.decode(data)
        except ValueError:
            return None


# =============================================================================
# HandshakeCodec - Thin wrapper around handshake_protocol
# =============================================================================


class HandshakeCodec:
    """Codec for handshake protocol messages.

    Wire format:
        [msg_type: 1 byte][payload: msgpack]
    """

    @staticmethod
    def encode(msg: WireHandshakeMessage) -> bytes:
        """Encode a handshake message."""
        return handshake_protocol.encode(msg)

    @staticmethod
    def decode(data: bytes) -> WireHandshakeMessage | None:
        """Decode a handshake message."""
        if len(data) < 1:
            return None

        try:
            return handshake_protocol.decode(data)
        except ValueError:
            return None


# =============================================================================
# Backwards Compatibility - Keep ActorMessageType and HandshakeMessageType
# =============================================================================

# These enums are kept for backwards compatibility with code that uses them
# for type checking or logging. The actual encoding now uses the message_id
# stored on each class by @serializable.

from .messages import ActorMessageType, HandshakeMessageType

__all__ = [
    # Codecs
    "ActorCodec",
    "HandshakeCodec",
    # Protocol types
    "ProtocolType",
    "ActorMessageType",
    "HandshakeMessageType",
    # Wire messages
    "ActorMessage",
    "AskRequest",
    "AskResponse",
    "RegisterActor",
    "LookupActor",
    "LookupResponse",
    "EntitySend",
    "EntityAskRequest",
    "EntityAskResponse",
    "SingletonSend",
    "SingletonAskRequest",
    "SingletonAskResponse",
    "ShardedTypeRegister",
    "ShardedTypeAck",
    "MergeRequest",
    "MergeState",
    "MergeComplete",
    "ReplicateWrite",
    "ReplicateWriteAck",
    "HintedHandoff",
    "ReplicateWALEntry",
    "ReplicateWALAck",
    "RequestCatchUp",
    "CatchUpEntries",
    "NodeHandshake",
    "NodeHandshakeAck",
    # Type aliases
    "WireActorMessage",
    "WireHandshakeMessage",
    "WireMessage",
]
