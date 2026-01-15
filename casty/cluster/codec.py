"""Cluster Wire Protocol Codec with Protocol Multiplexing.

Wire format:
    [length: 4 bytes BE][protocol: 1 byte][msg_type: 1 byte][payload: msgpack]

Protocol Types:
    0x01 = SWIM (delegated to SwimCodec)
    0x02 = ACTOR (ActorMessage, AskRequest, AskResponse, etc.)
    0x03 = HANDSHAKE (NodeHandshake, NodeHandshakeAck)

This codec allows a single TCP port to handle multiple protocols.
"""

from __future__ import annotations

import struct
from typing import Any

import msgpack

from .messages import (
    # Protocol types
    ProtocolType,
    ActorMessageType,
    HandshakeMessageType,
    # Actor messages
    ActorMessage,
    AskRequest,
    AskResponse,
    RegisterActor,
    LookupActor,
    LookupResponse,
    # Entity messages
    EntitySend,
    EntityAskRequest,
    EntityAskResponse,
    # Singleton messages
    SingletonSend,
    SingletonAskRequest,
    SingletonAskResponse,
    # Sharded type registration
    ShardedTypeRegister,
    ShardedTypeAck,
    # Merge messages
    MergeRequest,
    MergeState,
    MergeComplete,
    WireActorMessage,
    # Handshake messages
    NodeHandshake,
    NodeHandshakeAck,
    WireHandshakeMessage,
)


# Type alias for any wire message
WireMessage = WireActorMessage | WireHandshakeMessage


# =============================================================================
# Actor Message Encoding/Decoding
# =============================================================================


_ACTOR_MESSAGE_CLASSES: dict[ActorMessageType, type] = {
    ActorMessageType.ACTOR_MESSAGE: ActorMessage,
    ActorMessageType.ASK_REQUEST: AskRequest,
    ActorMessageType.ASK_RESPONSE: AskResponse,
    ActorMessageType.REGISTER_ACTOR: RegisterActor,
    ActorMessageType.LOOKUP_ACTOR: LookupActor,
    ActorMessageType.LOOKUP_RESPONSE: LookupResponse,
    ActorMessageType.ENTITY_SEND: EntitySend,
    ActorMessageType.ENTITY_ASK_REQUEST: EntityAskRequest,
    ActorMessageType.ENTITY_ASK_RESPONSE: EntityAskResponse,
    ActorMessageType.SINGLETON_SEND: SingletonSend,
    ActorMessageType.SINGLETON_ASK_REQUEST: SingletonAskRequest,
    ActorMessageType.SINGLETON_ASK_RESPONSE: SingletonAskResponse,
    ActorMessageType.SHARDED_TYPE_REGISTER: ShardedTypeRegister,
    ActorMessageType.SHARDED_TYPE_ACK: ShardedTypeAck,
    ActorMessageType.MERGE_REQUEST: MergeRequest,
    ActorMessageType.MERGE_STATE: MergeState,
    ActorMessageType.MERGE_COMPLETE: MergeComplete,
}

_ACTOR_CLASS_TO_TYPE: dict[type, ActorMessageType] = {
    v: k for k, v in _ACTOR_MESSAGE_CLASSES.items()
}


def _encode_actor_message(msg: WireActorMessage) -> dict[str, Any]:
    """Encode an actor protocol message to dict."""
    match msg:
        case ActorMessage():
            return {
                "target_actor": msg.target_actor,
                "payload_type": msg.payload_type,
                "payload": msg.payload,
            }
        case AskRequest():
            return {
                "request_id": msg.request_id,
                "target_actor": msg.target_actor,
                "payload_type": msg.payload_type,
                "payload": msg.payload,
            }
        case AskResponse():
            return {
                "request_id": msg.request_id,
                "success": msg.success,
                "payload_type": msg.payload_type,
                "payload": msg.payload,
            }
        case RegisterActor():
            return {
                "actor_name": msg.actor_name,
                "node_id": msg.node_id,
            }
        case LookupActor():
            return {
                "actor_name": msg.actor_name,
                "request_id": msg.request_id,
            }
        case LookupResponse():
            return {
                "actor_name": msg.actor_name,
                "request_id": msg.request_id,
                "node_id": msg.node_id,
            }
        case EntitySend():
            return {
                "entity_type": msg.entity_type,
                "entity_id": msg.entity_id,
                "payload_type": msg.payload_type,
                "payload": msg.payload,
                "actor_cls_fqn": msg.actor_cls_fqn,
            }
        case EntityAskRequest():
            return {
                "request_id": msg.request_id,
                "entity_type": msg.entity_type,
                "entity_id": msg.entity_id,
                "payload_type": msg.payload_type,
                "payload": msg.payload,
                "actor_cls_fqn": msg.actor_cls_fqn,
            }
        case EntityAskResponse():
            return {
                "request_id": msg.request_id,
                "success": msg.success,
                "payload_type": msg.payload_type,
                "payload": msg.payload,
            }
        case SingletonSend():
            return {
                "singleton_name": msg.singleton_name,
                "payload_type": msg.payload_type,
                "payload": msg.payload,
                "actor_cls_fqn": msg.actor_cls_fqn,
            }
        case SingletonAskRequest():
            return {
                "request_id": msg.request_id,
                "singleton_name": msg.singleton_name,
                "payload_type": msg.payload_type,
                "payload": msg.payload,
                "actor_cls_fqn": msg.actor_cls_fqn,
            }
        case SingletonAskResponse():
            return {
                "request_id": msg.request_id,
                "success": msg.success,
                "payload_type": msg.payload_type,
                "payload": msg.payload,
            }
        case ShardedTypeRegister():
            return {
                "request_id": msg.request_id,
                "entity_type": msg.entity_type,
                "actor_cls_fqn": msg.actor_cls_fqn,
            }
        case ShardedTypeAck():
            return {
                "request_id": msg.request_id,
                "entity_type": msg.entity_type,
                "success": msg.success,
                "error": msg.error,
            }
        case MergeRequest():
            return {
                "request_id": msg.request_id,
                "entity_type": msg.entity_type,
                "entity_id": msg.entity_id,
                "my_version": msg.my_version,
                "my_base_version": msg.my_base_version,
            }
        case MergeState():
            return {
                "request_id": msg.request_id,
                "entity_type": msg.entity_type,
                "entity_id": msg.entity_id,
                "version": msg.version,
                "base_version": msg.base_version,
                "state": msg.state,
                "base_state": msg.base_state,
            }
        case MergeComplete():
            return {
                "request_id": msg.request_id,
                "entity_type": msg.entity_type,
                "entity_id": msg.entity_id,
                "new_version": msg.new_version,
            }
        case _:
            raise ValueError(f"Unknown actor message type: {type(msg)}")


def _decode_actor_message(
    msg_type: ActorMessageType, data: dict[str, Any]
) -> WireActorMessage:
    """Decode an actor protocol message from type and dict."""
    match msg_type:
        case ActorMessageType.ACTOR_MESSAGE:
            return ActorMessage(
                target_actor=data["target_actor"],
                payload_type=data["payload_type"],
                payload=data["payload"],
            )
        case ActorMessageType.ASK_REQUEST:
            return AskRequest(
                request_id=data["request_id"],
                target_actor=data["target_actor"],
                payload_type=data["payload_type"],
                payload=data["payload"],
            )
        case ActorMessageType.ASK_RESPONSE:
            return AskResponse(
                request_id=data["request_id"],
                success=data["success"],
                payload_type=data["payload_type"],
                payload=data["payload"],
            )
        case ActorMessageType.REGISTER_ACTOR:
            return RegisterActor(
                actor_name=data["actor_name"],
                node_id=data["node_id"],
            )
        case ActorMessageType.LOOKUP_ACTOR:
            return LookupActor(
                actor_name=data["actor_name"],
                request_id=data["request_id"],
            )
        case ActorMessageType.LOOKUP_RESPONSE:
            return LookupResponse(
                actor_name=data["actor_name"],
                request_id=data["request_id"],
                node_id=data["node_id"],
            )
        case ActorMessageType.ENTITY_SEND:
            return EntitySend(
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                payload_type=data["payload_type"],
                payload=data["payload"],
                actor_cls_fqn=data.get("actor_cls_fqn", ""),
            )
        case ActorMessageType.ENTITY_ASK_REQUEST:
            return EntityAskRequest(
                request_id=data["request_id"],
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                payload_type=data["payload_type"],
                payload=data["payload"],
                actor_cls_fqn=data.get("actor_cls_fqn", ""),
            )
        case ActorMessageType.ENTITY_ASK_RESPONSE:
            return EntityAskResponse(
                request_id=data["request_id"],
                success=data["success"],
                payload_type=data["payload_type"],
                payload=data["payload"],
            )
        case ActorMessageType.SINGLETON_SEND:
            return SingletonSend(
                singleton_name=data["singleton_name"],
                payload_type=data["payload_type"],
                payload=data["payload"],
                actor_cls_fqn=data.get("actor_cls_fqn", ""),
            )
        case ActorMessageType.SINGLETON_ASK_REQUEST:
            return SingletonAskRequest(
                request_id=data["request_id"],
                singleton_name=data["singleton_name"],
                payload_type=data["payload_type"],
                payload=data["payload"],
                actor_cls_fqn=data.get("actor_cls_fqn", ""),
            )
        case ActorMessageType.SINGLETON_ASK_RESPONSE:
            return SingletonAskResponse(
                request_id=data["request_id"],
                success=data["success"],
                payload_type=data["payload_type"],
                payload=data["payload"],
            )
        case ActorMessageType.SHARDED_TYPE_REGISTER:
            return ShardedTypeRegister(
                request_id=data["request_id"],
                entity_type=data["entity_type"],
                actor_cls_fqn=data["actor_cls_fqn"],
            )
        case ActorMessageType.SHARDED_TYPE_ACK:
            return ShardedTypeAck(
                request_id=data["request_id"],
                entity_type=data["entity_type"],
                success=data["success"],
                error=data.get("error"),
            )
        case ActorMessageType.MERGE_REQUEST:
            return MergeRequest(
                request_id=data["request_id"],
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                my_version=data["my_version"],
                my_base_version=data["my_base_version"],
            )
        case ActorMessageType.MERGE_STATE:
            return MergeState(
                request_id=data["request_id"],
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                version=data["version"],
                base_version=data["base_version"],
                state=data["state"],
                base_state=data["base_state"],
            )
        case ActorMessageType.MERGE_COMPLETE:
            return MergeComplete(
                request_id=data["request_id"],
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                new_version=data["new_version"],
            )
        case _:
            raise ValueError(f"Unknown actor message type: {msg_type}")


# =============================================================================
# Handshake Message Encoding/Decoding
# =============================================================================


_HANDSHAKE_MESSAGE_CLASSES: dict[HandshakeMessageType, type] = {
    HandshakeMessageType.NODE_HANDSHAKE: NodeHandshake,
    HandshakeMessageType.NODE_HANDSHAKE_ACK: NodeHandshakeAck,
}

_HANDSHAKE_CLASS_TO_TYPE: dict[type, HandshakeMessageType] = {
    v: k for k, v in _HANDSHAKE_MESSAGE_CLASSES.items()
}


def _encode_handshake_message(msg: WireHandshakeMessage) -> dict[str, Any]:
    """Encode a handshake protocol message to dict."""
    match msg:
        case NodeHandshake():
            return {
                "node_id": msg.node_id,
                "address": list(msg.address),
                "protocol_version": msg.protocol_version,
            }
        case NodeHandshakeAck():
            return {
                "node_id": msg.node_id,
                "address": list(msg.address),
                "accepted": msg.accepted,
                "reason": msg.reason,
            }
        case _:
            raise ValueError(f"Unknown handshake message type: {type(msg)}")


def _decode_handshake_message(
    msg_type: HandshakeMessageType, data: dict[str, Any]
) -> WireHandshakeMessage:
    """Decode a handshake protocol message from type and dict."""
    match msg_type:
        case HandshakeMessageType.NODE_HANDSHAKE:
            return NodeHandshake(
                node_id=data["node_id"],
                address=tuple(data["address"]),
                protocol_version=data.get("protocol_version", 1),
            )
        case HandshakeMessageType.NODE_HANDSHAKE_ACK:
            return NodeHandshakeAck(
                node_id=data["node_id"],
                address=tuple(data["address"]),
                accepted=data["accepted"],
                reason=data.get("reason"),
            )
        case _:
            raise ValueError(f"Unknown handshake message type: {msg_type}")


# =============================================================================
# ClusterCodec
# =============================================================================


class ClusterCodec:
    """Codec for cluster protocol messages with protocol multiplexing.

    Wire format:
        [length: 4 bytes BE][protocol: 1 byte][msg_type: 1 byte][payload: msgpack]

    The protocol byte allows multiplexing SWIM, Actor, and Handshake messages
    on the same TCP connection.

    Example:
        # Encoding
        data = ClusterCodec.encode(NodeHandshake("node-1", ("127.0.0.1", 8000)))

        # Decoding
        protocol, msg, remaining = ClusterCodec.decode(data)
        # protocol = ProtocolType.HANDSHAKE
        # msg = NodeHandshake(...)
    """

    HEADER_SIZE = 6  # 4 bytes length + 1 byte protocol + 1 byte msg_type
    MAX_MESSAGE_SIZE = 64 * 1024

    @staticmethod
    def encode(msg: WireMessage) -> bytes:
        """Encode message with protocol multiplexing.

        Args:
            msg: Wire message to encode.

        Returns:
            Framed bytes ready for transmission.

        Raises:
            ValueError: If message type is unknown.
        """
        # Determine protocol and message type
        if isinstance(msg, (ActorMessage, AskRequest, AskResponse, RegisterActor, LookupActor, LookupResponse, EntitySend, EntityAskRequest, EntityAskResponse, SingletonSend, SingletonAskRequest, SingletonAskResponse, ShardedTypeRegister, ShardedTypeAck, MergeRequest, MergeState, MergeComplete)):
            protocol = ProtocolType.ACTOR
            msg_type = _ACTOR_CLASS_TO_TYPE.get(type(msg))
            if msg_type is None:
                raise ValueError(f"Unknown actor message type: {type(msg)}")
            payload_dict = _encode_actor_message(msg)
        elif isinstance(msg, (NodeHandshake, NodeHandshakeAck)):
            protocol = ProtocolType.HANDSHAKE
            msg_type = _HANDSHAKE_CLASS_TO_TYPE.get(type(msg))
            if msg_type is None:
                raise ValueError(f"Unknown handshake message type: {type(msg)}")
            payload_dict = _encode_handshake_message(msg)
        else:
            raise ValueError(f"Unknown message type: {type(msg)}")

        payload = msgpack.packb(payload_dict, use_bin_type=True)

        # Frame: [length][protocol][msg_type][payload]
        length = 2 + len(payload)  # protocol + msg_type + payload
        return struct.pack(">IBB", length, protocol.value, msg_type.value) + payload

    @staticmethod
    def encode_swim_frame(swim_data: bytes) -> bytes:
        """Wrap SWIM codec output with cluster framing.

        Use this to wrap data from SwimCodec.encode() with the cluster
        protocol header so it can be sent on the shared port.

        Args:
            swim_data: Data from SwimCodec.encode() (already has SWIM framing).

        Returns:
            Cluster-framed bytes with SWIM protocol marker.
        """
        # SWIM data already has [length][type][payload] format
        # We wrap it as [length][SWIM_PROTOCOL][swim_data]
        length = 1 + len(swim_data)  # protocol byte + swim data
        return struct.pack(">IB", length, ProtocolType.SWIM.value) + swim_data

    @staticmethod
    def encode_gossip_frame(gossip_data: bytes) -> bytes:
        """Wrap GossipCodec output with cluster framing.

        Use this to wrap data from GossipCodec.encode() with the cluster
        protocol header so it can be sent on the shared port.

        Args:
            gossip_data: Data from GossipCodec.encode() (already has Gossip framing).

        Returns:
            Cluster-framed bytes with GOSSIP protocol marker.
        """
        length = 1 + len(gossip_data)  # protocol byte + gossip data
        return struct.pack(">IB", length, ProtocolType.GOSSIP.value) + gossip_data

    @staticmethod
    def decode(data: bytes) -> tuple[ProtocolType | None, WireMessage | bytes | None, bytes]:
        """Decode one message from buffer.

        Args:
            data: Buffer containing framed message(s).

        Returns:
            Tuple of:
                - Protocol type (or None if incomplete)
                - Decoded message for ACTOR/HANDSHAKE, raw bytes for SWIM (or None if incomplete)
                - Remaining bytes
        """
        if len(data) < 5:  # Minimum: 4 bytes length + 1 byte protocol
            return None, None, data

        length = struct.unpack(">I", data[:4])[0]

        if length > ClusterCodec.MAX_MESSAGE_SIZE:
            raise ValueError(f"Message too large: {length} > {ClusterCodec.MAX_MESSAGE_SIZE}")

        if len(data) < 4 + length:
            return None, None, data

        protocol_value = data[4]
        try:
            protocol = ProtocolType(protocol_value)
        except ValueError:
            raise ValueError(f"Unknown protocol type: {protocol_value}")

        frame_end = 4 + length
        remaining = data[frame_end:]

        if protocol == ProtocolType.SWIM:
            # Return raw SWIM data (without protocol byte) for SwimCodec to decode
            swim_data = data[5:frame_end]
            return protocol, swim_data, remaining

        if protocol == ProtocolType.GOSSIP:
            # Return raw Gossip data (without protocol byte) for GossipCodec to decode
            gossip_data = data[5:frame_end]
            return protocol, gossip_data, remaining

        # For ACTOR and HANDSHAKE, decode fully
        if len(data) < ClusterCodec.HEADER_SIZE:
            return None, None, data

        msg_type_value = data[5]
        payload = data[6:frame_end]

        payload_dict = msgpack.unpackb(payload, raw=False)

        if protocol == ProtocolType.ACTOR:
            try:
                msg_type = ActorMessageType(msg_type_value)
            except ValueError:
                raise ValueError(f"Unknown actor message type: {msg_type_value}")
            msg = _decode_actor_message(msg_type, payload_dict)
        elif protocol == ProtocolType.HANDSHAKE:
            try:
                msg_type = HandshakeMessageType(msg_type_value)
            except ValueError:
                raise ValueError(f"Unknown handshake message type: {msg_type_value}")
            msg = _decode_handshake_message(msg_type, payload_dict)
        else:
            raise ValueError(f"Unsupported protocol for full decode: {protocol}")

        return protocol, msg, remaining

    @staticmethod
    def decode_all(
        data: bytes,
    ) -> tuple[list[tuple[ProtocolType, WireMessage | bytes]], bytes]:
        """Decode all complete messages from buffer.

        Args:
            data: Buffer containing framed message(s).

        Returns:
            Tuple of:
                - List of (protocol, message) tuples
                - Remaining bytes
        """
        messages = []
        while True:
            protocol, msg, data = ClusterCodec.decode(data)
            if protocol is None:
                break
            messages.append((protocol, msg))
        return messages, data


# =============================================================================
# ActorCodec (simplified codec for TransportMux)
# =============================================================================


class ActorCodec:
    """Simplified codec for actor protocol messages.

    Used by Cluster when receiving ACTOR protocol data from TransportMux.
    TransportMux handles the length framing and protocol byte, so this
    codec only handles the msg_type and payload.

    Wire format:
        [msg_type: 1 byte][payload: msgpack]

    Example:
        # Encoding
        data = ActorCodec.encode(AskRequest(...))

        # Decoding
        msg = ActorCodec.decode(data)
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
        msg_type = _ACTOR_CLASS_TO_TYPE.get(type(msg))
        if msg_type is None:
            raise ValueError(f"Unknown actor message type: {type(msg)}")

        payload_dict = _encode_actor_message(msg)
        payload = msgpack.packb(payload_dict, use_bin_type=True)

        return struct.pack("B", msg_type.value) + payload

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

        msg_type_value = data[0]
        try:
            msg_type = ActorMessageType(msg_type_value)
        except ValueError:
            raise ValueError(f"Unknown actor message type: {msg_type_value}")

        payload = data[1:]
        if not payload:
            return None

        payload_dict = msgpack.unpackb(payload, raw=False)
        return _decode_actor_message(msg_type, payload_dict)
