"""Serialization for cluster communication.

Uses msgpack for efficient binary serialization of messages,
commands, and state.
"""

from __future__ import annotations

import importlib
from dataclasses import asdict, is_dataclass, fields
from typing import Any, TypeVar, get_type_hints

import msgpack

T = TypeVar("T")


# Type registry for deserialization
_type_registry: dict[str, type] = {}


def register_type(cls: type) -> type:
    """Register a type for serialization.

    Registered types can be serialized and deserialized by name.

    Args:
        cls: The class to register

    Returns:
        The same class (for use as decorator)

    Usage:
        @register_type
        @dataclass
        class MyMessage:
            value: int
    """
    _type_registry[f"{cls.__module__}.{cls.__qualname__}"] = cls
    return cls


def get_registered_type(name: str) -> type | None:
    """Get a registered type by name.

    Args:
        name: Fully qualified type name

    Returns:
        The type or None if not found
    """
    return _type_registry.get(name)


def _resolve_type(name: str) -> type | None:
    """Resolve a type by fully qualified name.

    First checks the registry, then tries to import dynamically.

    Args:
        name: Fully qualified type name (e.g., "myapp.MyClass")

    Returns:
        The type or None if not found
    """
    # Check registry first
    if name in _type_registry:
        return _type_registry[name]

    # Try to import dynamically
    try:
        parts = name.rsplit(".", 1)
        if len(parts) == 2:
            module_name, class_name = parts
            module = importlib.import_module(module_name)
            return getattr(module, class_name, None)
    except (ImportError, AttributeError):
        pass

    return None


def _serialize_value(value: Any) -> Any:
    """Serialize a single value to msgpack-compatible form.

    Args:
        value: Value to serialize

    Returns:
        Serializable form of the value
    """
    from enum import Enum

    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value

    # Handle Enums specially - serialize as type + value
    if isinstance(value, Enum):
        type_name = f"{type(value).__module__}.{type(value).__qualname__}"
        return {"__enum__": type_name, "__value__": value.name}

    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]

    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}

    if isinstance(value, set):
        return {"__set__": [_serialize_value(v) for v in value]}

    if is_dataclass(value) and not isinstance(value, type):
        # Serialize dataclass
        type_name = f"{type(value).__module__}.{type(value).__qualname__}"
        data = {}
        for field in fields(value):
            field_value = getattr(value, field.name)
            data[field.name] = _serialize_value(field_value)
        return {"__type__": type_name, "__data__": data}

    # Try to serialize as dict if possible
    if hasattr(value, "__dict__"):
        type_name = f"{type(value).__module__}.{type(value).__qualname__}"
        return {
            "__type__": type_name,
            "__data__": {k: _serialize_value(v) for k, v in value.__dict__.items()},
        }

    # Fallback: convert to string
    return str(value)


def _deserialize_value(value: Any) -> Any:
    """Deserialize a value from msgpack form.

    Args:
        value: Serialized value

    Returns:
        Deserialized value
    """
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value

    if isinstance(value, list):
        return [_deserialize_value(v) for v in value]

    if isinstance(value, dict):
        # Check for special markers
        if "__set__" in value:
            return set(_deserialize_value(v) for v in value["__set__"])

        if "__enum__" in value and "__value__" in value:
            # Deserialize enum
            type_name = value["__enum__"]
            enum_value = value["__value__"]
            cls = _resolve_type(type_name)
            if cls is not None:
                return cls[enum_value]  # Get enum member by name
            return enum_value  # Fallback to string

        if "__type__" in value and "__data__" in value:
            type_name = value["__type__"]
            data = value["__data__"]

            # Resolve the type
            cls = _resolve_type(type_name)
            if cls is None:
                # Can't resolve type, return raw dict
                return {k: _deserialize_value(v) for k, v in data.items()}

            # Deserialize field values
            deserialized_data = {k: _deserialize_value(v) for k, v in data.items()}

            # Create instance
            if is_dataclass(cls):
                return cls(**deserialized_data)
            else:
                # Try to create instance with kwargs
                try:
                    return cls(**deserialized_data)
                except TypeError:
                    # Fallback: create instance and set attributes
                    obj = object.__new__(cls)
                    for k, v in deserialized_data.items():
                        setattr(obj, k, v)
                    return obj

        # Regular dict
        return {k: _deserialize_value(v) for k, v in value.items()}

    return value


def serialize(obj: Any) -> bytes:
    """Serialize an object to bytes.

    Args:
        obj: Object to serialize

    Returns:
        Serialized bytes
    """
    serialized = _serialize_value(obj)
    return msgpack.packb(serialized, use_bin_type=True)


def deserialize(data: bytes) -> Any:
    """Deserialize bytes to an object.

    Args:
        data: Serialized bytes

    Returns:
        Deserialized object
    """
    unpacked = msgpack.unpackb(data, raw=False)
    return _deserialize_value(unpacked)


def serialize_to_dict(obj: Any) -> dict:
    """Serialize an object to a dictionary.

    Useful for debugging or JSON export.

    Args:
        obj: Object to serialize

    Returns:
        Dictionary representation
    """
    return _serialize_value(obj)


def deserialize_from_dict(data: dict) -> Any:
    """Deserialize a dictionary to an object.

    Args:
        data: Dictionary representation

    Returns:
        Deserialized object
    """
    return _deserialize_value(data)


# Register built-in types from control plane
def _register_builtin_types() -> None:
    """Register built-in types for serialization."""
    # Import and register control plane commands
    from .control_plane.commands import (
        JoinCluster,
        LeaveCluster,
        NodeFailed,
        NodeRecovered,
        RegisterSingleton,
        UnregisterSingleton,
        TransferSingleton,
        RegisterEntityType,
        UnregisterEntityType,
        UpdateEntityTypeConfig,
        RegisterNamedActor,
        UnregisterNamedActor,
    )

    for cls in [
        JoinCluster,
        LeaveCluster,
        NodeFailed,
        NodeRecovered,
        RegisterSingleton,
        UnregisterSingleton,
        TransferSingleton,
        RegisterEntityType,
        UnregisterEntityType,
        UpdateEntityTypeConfig,
        RegisterNamedActor,
        UnregisterNamedActor,
    ]:
        register_type(cls)

    # Import and register raft messages
    from .control_plane.raft import (
        LogEntry,
        VoteRequest,
        VoteResponse,
        AppendEntriesRequest,
        AppendEntriesResponse,
    )

    for cls in [
        LogEntry,
        VoteRequest,
        VoteResponse,
        AppendEntriesRequest,
        AppendEntriesResponse,
    ]:
        register_type(cls)

    # Import and register gossip messages
    from .data_plane.gossip import (
        Ping,
        PingReq,
        Ack,
        MembershipUpdate,
        MemberInfo,
    )

    for cls in [Ping, PingReq, Ack, MembershipUpdate, MemberInfo]:
        register_type(cls)


# Initialize on import
try:
    _register_builtin_types()
except ImportError:
    # Modules may not be available yet during initial setup
    pass
