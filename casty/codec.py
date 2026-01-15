"""Declarative codec system for Casty wire protocol.

This module provides the @serializable decorator that:
1. Auto-applies @dataclass (frozen=True, slots=True) if not already a dataclass
2. Registers the class in a global registry with its message ID
3. Enables automatic encode/decode via dataclass introspection

Usage:
    from casty.codec import serializable, encode, decode

    @serializable(0x01)
    class ActorMessage:
        target_actor: str
        payload_type: str
        payload: bytes

    # Encode
    data = encode(ActorMessage("target", "type", b"payload"))

    # Decode
    msg = decode(data)  # Returns ActorMessage instance
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, fields, is_dataclass, asdict
from typing import Any, TypeVar, get_type_hints, get_origin, get_args

import msgpack

T = TypeVar("T")


# =============================================================================
# Global Registry
# =============================================================================

# Maps message_id -> class
_MESSAGE_REGISTRY: dict[int, type] = {}

# Maps class -> message_id
_CLASS_TO_ID: dict[type, int] = {}

# Maps class -> custom encoder function (for special types like tuple)
_CUSTOM_ENCODERS: dict[type, dict[str, Any]] = {}

# Maps class -> custom decoder function (for special types like tuple)
_CUSTOM_DECODERS: dict[type, dict[str, Any]] = {}


def get_registry() -> dict[int, type]:
    """Get a copy of the message registry (for debugging)."""
    return _MESSAGE_REGISTRY.copy()


def get_message_id(cls: type) -> int | None:
    """Get the message ID for a class, or None if not registered."""
    return _CLASS_TO_ID.get(cls)


def is_serializable(cls: type) -> bool:
    """Check if a class is registered as serializable."""
    return cls in _CLASS_TO_ID


# =============================================================================
# Type Conversion Helpers
# =============================================================================


def _get_field_types(cls: type) -> dict[str, type]:
    """Extract field name -> type mapping from a dataclass."""
    try:
        return get_type_hints(cls)
    except Exception:
        # Fallback for forward references
        return {f.name: f.type for f in fields(cls)}


def _needs_conversion(field_type: type) -> bool:
    """Check if a field type needs special conversion (tuple anywhere in the type)."""
    origin = get_origin(field_type)

    # Direct tuple
    if origin is tuple or field_type is tuple:
        return True

    # list[T] - check if T needs conversion
    if origin is list:
        args = get_args(field_type)
        if args:
            return _needs_conversion(args[0])

    return False


def _encode_value(value: Any, field_type: type) -> Any:
    """Encode a value, handling special types like tuple."""
    if value is None:
        return None

    origin = get_origin(field_type)

    # tuple -> list for msgpack
    if origin is tuple or field_type is tuple:
        return list(value) if value is not None else None

    # list[T] - recursively encode elements
    if origin is list:
        args = get_args(field_type)
        if args:
            elem_type = args[0]
            return [_encode_value(v, elem_type) for v in value]
        return value

    return value


def _decode_value(value: Any, field_type: type) -> Any:
    """Decode a value, handling special types like tuple."""
    if value is None:
        return None

    origin = get_origin(field_type)

    # list -> tuple for msgpack
    if origin is tuple or field_type is tuple:
        if value is None:
            return None
        # Also recursively decode tuple elements
        args = get_args(field_type)
        if args:
            return tuple(_decode_value(v, t) for v, t in zip(value, args))
        return tuple(value)

    # list[T] - recursively decode elements
    if origin is list:
        args = get_args(field_type)
        if args:
            elem_type = args[0]
            return [_decode_value(v, elem_type) for v in value]
        return value

    return value


# =============================================================================
# The @serializable Decorator
# =============================================================================


def serializable(
    message_id: int,
    *,
    frozen: bool = True,
    slots: bool = True,
):
    """Decorator that makes a class serializable for wire protocol.

    This decorator:
    1. Applies @dataclass(frozen=True, slots=True) if not already a dataclass
    2. Registers the class in the global message registry
    3. Enables automatic encode/decode

    Args:
        message_id: Unique integer ID for wire protocol (1 byte, 0x01-0xFF)
        frozen: Whether dataclass should be frozen (default: True)
        slots: Whether dataclass should use __slots__ (default: True)

    Example:
        @serializable(0x01)
        class ActorMessage:
            target_actor: str
            payload_type: str
            payload: bytes

        # Encoding
        msg = ActorMessage("target", "type", b"data")
        data = encode(msg)

        # Decoding
        decoded = decode(data)
        assert decoded == msg

    Raises:
        ValueError: If message_id is already registered or out of range
    """
    if not (0x00 <= message_id <= 0xFF):
        raise ValueError(f"message_id must be 0x00-0xFF, got {hex(message_id)}")

    if message_id in _MESSAGE_REGISTRY:
        existing = _MESSAGE_REGISTRY[message_id]
        raise ValueError(
            f"message_id {hex(message_id)} already registered to {existing.__name__}"
        )

    def decorator(cls: type[T]) -> type[T]:
        # Apply @dataclass if not already one
        if not is_dataclass(cls):
            cls = dataclass(frozen=frozen, slots=slots)(cls)

        # Register in both directions
        _MESSAGE_REGISTRY[message_id] = cls
        _CLASS_TO_ID[cls] = message_id

        # Pre-compute field types for encode/decode
        field_types = _get_field_types(cls)

        # Check which fields need special handling
        tuple_fields = {
            name: ftype
            for name, ftype in field_types.items()
            if _needs_conversion(ftype)
        }

        if tuple_fields:
            _CUSTOM_ENCODERS[cls] = tuple_fields
            _CUSTOM_DECODERS[cls] = tuple_fields

        # Store message_id on class for introspection
        cls.__casty_message_id__ = message_id  # type: ignore

        return cls

    return decorator


# =============================================================================
# Encode / Decode Functions
# =============================================================================


def encode(msg: Any) -> bytes:
    """Encode a @serializable message to bytes.

    Wire format: [message_id: 1 byte][payload: msgpack]

    Args:
        msg: Instance of a @serializable class

    Returns:
        Encoded bytes

    Raises:
        ValueError: If message type is not registered
    """
    cls = type(msg)
    message_id = _CLASS_TO_ID.get(cls)

    if message_id is None:
        raise ValueError(f"Type {cls.__name__} is not @serializable")

    # Convert to dict
    data = asdict(msg)

    # Apply custom encoding for special types (tuple -> list)
    if cls in _CUSTOM_ENCODERS:
        field_types = _CUSTOM_ENCODERS[cls]
        for field_name, field_type in field_types.items():
            if field_name in data:
                data[field_name] = _encode_value(data[field_name], field_type)

    # Pack with msgpack
    payload = msgpack.packb(data, use_bin_type=True)

    return struct.pack("B", message_id) + payload


def decode(data: bytes) -> Any:
    """Decode bytes to a @serializable message.

    Args:
        data: Encoded bytes (message_id + msgpack payload)

    Returns:
        Instance of the registered class

    Raises:
        ValueError: If message_id is not registered or data is invalid
    """
    if len(data) < 1:
        raise ValueError("Data too short: need at least 1 byte for message_id")

    message_id = data[0]
    cls = _MESSAGE_REGISTRY.get(message_id)

    if cls is None:
        raise ValueError(f"Unknown message_id: {hex(message_id)}")

    # Unpack payload
    payload = data[1:]
    if not payload:
        raise ValueError("Empty payload")

    fields_dict = msgpack.unpackb(payload, raw=False)

    # Apply custom decoding for special types (list -> tuple)
    if cls in _CUSTOM_DECODERS:
        field_types = _CUSTOM_DECODERS[cls]
        for field_name, field_type in field_types.items():
            if field_name in fields_dict:
                fields_dict[field_name] = _decode_value(
                    fields_dict[field_name], field_type
                )

    return cls(**fields_dict)


def decode_with_type(data: bytes, expected_type: type[T]) -> T:
    """Decode bytes and verify the type matches.

    Args:
        data: Encoded bytes
        expected_type: Expected class type

    Returns:
        Instance of expected_type

    Raises:
        ValueError: If decoded type doesn't match expected_type
    """
    result = decode(data)
    if not isinstance(result, expected_type):
        raise ValueError(
            f"Expected {expected_type.__name__}, got {type(result).__name__}"
        )
    return result


# =============================================================================
# Protocol-Scoped Registries (for isolated protocol codecs)
# =============================================================================


class ProtocolCodec:
    """Scoped codec for a specific protocol.

    Allows multiple protocols to have their own message ID namespaces.

    Example:
        actor_codec = ProtocolCodec("actor")

        @actor_codec.serializable(0x01)
        class ActorMessage:
            target: str

        @actor_codec.serializable(0x02)
        class AskRequest:
            request_id: str

        # Encode/decode within this protocol
        data = actor_codec.encode(ActorMessage("foo"))
        msg = actor_codec.decode(data)
    """

    def __init__(self, name: str):
        self.name = name
        self._registry: dict[int, type] = {}
        self._class_to_id: dict[type, int] = {}
        self._custom_encoders: dict[type, dict[str, type]] = {}
        self._custom_decoders: dict[type, dict[str, type]] = {}

    def serializable(
        self,
        message_id: int,
        *,
        frozen: bool = True,
        slots: bool = True,
    ):
        """Register a message type within this protocol's namespace."""
        if not (0x00 <= message_id <= 0xFF):
            raise ValueError(f"message_id must be 0x00-0xFF, got {hex(message_id)}")

        if message_id in self._registry:
            existing = self._registry[message_id]
            raise ValueError(
                f"[{self.name}] message_id {hex(message_id)} already registered "
                f"to {existing.__name__}"
            )

        def decorator(cls: type[T]) -> type[T]:
            if not is_dataclass(cls):
                cls = dataclass(frozen=frozen, slots=slots)(cls)

            self._registry[message_id] = cls
            self._class_to_id[cls] = message_id

            # Pre-compute field types
            field_types = _get_field_types(cls)
            tuple_fields = {
                name: ftype
                for name, ftype in field_types.items()
                if _needs_conversion(ftype)
            }

            if tuple_fields:
                self._custom_encoders[cls] = tuple_fields
                self._custom_decoders[cls] = tuple_fields

            cls.__casty_message_id__ = message_id  # type: ignore
            cls.__casty_protocol__ = self.name  # type: ignore

            return cls

        return decorator

    def encode(self, msg: Any) -> bytes:
        """Encode a message registered with this protocol."""
        cls = type(msg)
        message_id = self._class_to_id.get(cls)

        if message_id is None:
            raise ValueError(
                f"Type {cls.__name__} is not registered with protocol '{self.name}'"
            )

        data = asdict(msg)

        if cls in self._custom_encoders:
            field_types = self._custom_encoders[cls]
            for field_name, field_type in field_types.items():
                if field_name in data:
                    data[field_name] = _encode_value(data[field_name], field_type)

        payload = msgpack.packb(data, use_bin_type=True)
        return struct.pack("B", message_id) + payload

    def decode(self, data: bytes) -> Any:
        """Decode bytes to a message registered with this protocol."""
        if len(data) < 1:
            raise ValueError("Data too short")

        message_id = data[0]
        cls = self._registry.get(message_id)

        if cls is None:
            raise ValueError(
                f"[{self.name}] Unknown message_id: {hex(message_id)}"
            )

        payload = data[1:]
        if not payload:
            raise ValueError("Empty payload")

        fields_dict = msgpack.unpackb(payload, raw=False)

        if cls in self._custom_decoders:
            field_types = self._custom_decoders[cls]
            for field_name, field_type in field_types.items():
                if field_name in fields_dict:
                    fields_dict[field_name] = _decode_value(
                        fields_dict[field_name], field_type
                    )

        return cls(**fields_dict)

    def get_registry(self) -> dict[int, type]:
        """Get a copy of this protocol's registry."""
        return self._registry.copy()

    def get_message_id(self, cls: type) -> int | None:
        """Get message ID for a class in this protocol."""
        return self._class_to_id.get(cls)
