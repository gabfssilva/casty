"""Message serialization for Casty cluster using msgpack."""

import importlib
from dataclasses import asdict, is_dataclass
from typing import Any, TypeVar

import msgpack


def _import_type(type_name: str) -> type:
    """Import a type from its fully qualified name.

    Args:
        type_name: Full type name like 'myapp.messages.Increment'

    Returns:
        The imported class
    """
    module_name, class_name = type_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)

# Primitive types that can be serialized directly (excluding containers)
_SCALAR_PRIMITIVES = (int, float, str, bool, type(None), bytes)


def _convert_for_msgpack(obj: Any) -> Any:
    """Recursively convert objects for msgpack serialization.

    Converts dataclasses to dicts with type information preserved.
    """
    if obj is None or isinstance(obj, _SCALAR_PRIMITIVES):
        return obj
    elif isinstance(obj, dict):
        return {k: _convert_for_msgpack(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_convert_for_msgpack(item) for item in obj]
    elif is_dataclass(obj) and not isinstance(obj, type):
        # Convert dataclass to dict with type info
        return {
            "__type__": f"{type(obj).__module__}.{type(obj).__qualname__}",
            "__data__": {k: _convert_for_msgpack(v) for k, v in asdict(obj).items()},
        }
    else:
        raise TypeError(f"Cannot serialize {type(obj)}")


def serialize(obj: Any) -> bytes:
    """Serialize a value to msgpack bytes.

    Supports primitives (int, float, str, bool, None, bytes, list, dict)
    and dataclass instances. Type information is encoded for dataclasses.
    Handles nested dataclasses within dicts and lists.
    """
    if obj is None or isinstance(obj, _SCALAR_PRIMITIVES):
        data = {"__primitive__": True, "__value__": obj}
    elif isinstance(obj, (dict, list, tuple)):
        data = {"__primitive__": True, "__value__": _convert_for_msgpack(obj)}
    elif is_dataclass(obj) and not isinstance(obj, type):
        data = {
            "__type__": f"{type(obj).__module__}.{type(obj).__qualname__}",
            "__data__": {k: _convert_for_msgpack(v) for k, v in asdict(obj).items()},
        }
    else:
        raise TypeError(f"Cannot serialize {type(obj)}")

    return msgpack.packb(data, use_bin_type=True)


def _convert_from_msgpack(obj: Any, type_registry: dict[str, type]) -> Any:
    """Recursively convert deserialized msgpack data back to Python objects.

    Reconstructs dataclasses from dicts with __type__ markers.
    """
    if obj is None or isinstance(obj, _SCALAR_PRIMITIVES):
        return obj
    elif isinstance(obj, dict):
        # Check if this is a serialized dataclass
        if "__type__" in obj and "__data__" in obj:
            type_name = obj["__type__"]
            obj_data = obj["__data__"]

            # Try registry first, then dynamic import
            if type_name in type_registry:
                cls = type_registry[type_name]
            else:
                cls = _import_type(type_name)
                type_registry[type_name] = cls

            # Recursively convert nested data
            converted_data = {k: _convert_from_msgpack(v, type_registry) for k, v in obj_data.items()}
            return cls(**converted_data)
        else:
            return {k: _convert_from_msgpack(v, type_registry) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_from_msgpack(item, type_registry) for item in obj]
    else:
        return obj


def deserialize[T](data: bytes, type_registry: dict[str, type[T]]) -> T:
    """Deserialize msgpack bytes to original value.

    Types are automatically imported if not in the registry.

    Args:
        data: The msgpack bytes to deserialize
        type_registry: A mapping from type names to type classes (used as cache)

    Returns:
        The deserialized value (primitive or dataclass instance)
    """
    unpacked = msgpack.unpackb(data, raw=False)

    # Handle primitives (may contain nested dataclasses)
    if unpacked.get("__primitive__"):
        value = unpacked["__value__"]
        return _convert_from_msgpack(value, type_registry)  # type: ignore

    # Handle top-level dataclasses
    type_name = unpacked["__type__"]
    obj_data = unpacked["__data__"]

    # Try registry first, then dynamic import
    if type_name in type_registry:
        cls = type_registry[type_name]
    else:
        cls = _import_type(type_name)
        type_registry[type_name] = cls  # Cache for next time

    # Recursively convert nested data
    converted_data = {k: _convert_from_msgpack(v, type_registry) for k, v in obj_data.items()}
    return cls(**converted_data)


def get_type_name(cls: type) -> str:
    """Get the full type name for registration."""
    return f"{cls.__module__}.{cls.__qualname__}"
