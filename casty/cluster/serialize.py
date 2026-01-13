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

# Primitive types that can be serialized directly
_PRIMITIVES = (int, float, str, bool, type(None), bytes, list, dict)


def serialize(obj: Any) -> bytes:
    """Serialize a value to msgpack bytes.

    Supports primitives (int, float, str, bool, None, bytes, list, dict)
    and dataclass instances. Type information is encoded for dataclasses.
    """
    if isinstance(obj, _PRIMITIVES):
        data = {"__primitive__": True, "__value__": obj}
    elif is_dataclass(obj) and not isinstance(obj, type):
        data = {
            "__type__": f"{type(obj).__module__}.{type(obj).__qualname__}",
            "__data__": asdict(obj),
        }
    else:
        raise TypeError(f"Cannot serialize {type(obj)}")

    return msgpack.packb(data, use_bin_type=True)


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

    # Handle primitives
    if unpacked.get("__primitive__"):
        return unpacked["__value__"]  # type: ignore

    # Handle dataclasses
    type_name = unpacked["__type__"]
    obj_data = unpacked["__data__"]

    # Try registry first, then dynamic import
    if type_name in type_registry:
        cls = type_registry[type_name]
    else:
        cls = _import_type(type_name)
        type_registry[type_name] = cls  # Cache for next time

    return cls(**obj_data)


def get_type_name(cls: type) -> str:
    """Get the full type name for registration."""
    return f"{cls.__module__}.{cls.__qualname__}"
