from __future__ import annotations

import asyncio
import msgpack
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, TypeVar

T = TypeVar("T")

_registry: dict[str, type] = {}


def serializable(cls: type[T]) -> type[T]:
    if not is_dataclass(cls):
        raise TypeError(f"{cls.__name__} must be a dataclass")

    type_name = f"{cls.__module__}.{cls.__qualname__}"
    _registry[type_name] = cls
    cls.__serializable_type__ = type_name

    return cls


def serialize(obj: Any) -> bytes:
    return msgpack.packb(_to_dict(obj), use_bin_type=True)


def deserialize(data: bytes) -> Any:
    return _from_dict(msgpack.unpackb(data, raw=False))


def _is_serializable_value(value: Any) -> bool:
    if isinstance(value, asyncio.Future):
        return False
    return True


def _to_dict(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str, bytes)):
        return obj

    if isinstance(obj, Enum):
        return {
            "__enum__": f"{obj.__class__.__module__}.{obj.__class__.__qualname__}",
            "value": obj.value,
        }

    if isinstance(obj, (list, tuple)):
        return [_to_dict(item) for item in obj]

    if isinstance(obj, dict):
        return {k: _to_dict(v) for k, v in obj.items()}

    if is_dataclass(obj) and not isinstance(obj, type):
        type_name = getattr(obj.__class__, "__serializable_type__", None)
        if type_name is None:
            raise TypeError(f"{obj.__class__.__name__} is not @serializable")

        return {
            "__type__": type_name,
            **{
                f.name: _to_dict(getattr(obj, f.name))
                for f in fields(obj)
                if _is_serializable_value(getattr(obj, f.name))
            },
        }

    raise TypeError(f"Cannot serialize {type(obj)}")


def _from_dict(data: Any) -> Any:
    if data is None or isinstance(data, (bool, int, float, str, bytes)):
        return data

    if isinstance(data, list):
        return [_from_dict(item) for item in data]

    if isinstance(data, dict):
        if "__type__" in data:
            type_name = data["__type__"]
            cls = _registry.get(type_name)
            if cls is None:
                raise TypeError(f"Unknown type: {type_name}")

            field_values = {k: _from_dict(v) for k, v in data.items() if k != "__type__"}
            return cls(**field_values)

        if "__enum__" in data:
            enum_path = data["__enum__"]
            module_name, enum_name = enum_path.rsplit(".", 1)
            import importlib
            module = importlib.import_module(module_name)
            enum_cls = getattr(module, enum_name)
            return enum_cls(data["value"])

        return {k: _from_dict(v) for k, v in data.items()}

    return data
