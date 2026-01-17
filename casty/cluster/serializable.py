from __future__ import annotations

import hashlib
import struct
from dataclasses import fields, is_dataclass
from typing import Any, cast, get_args, get_origin, get_type_hints

import msgpack

_registry: dict[int, type] = {}


def _compute_type_id(cls: type) -> int:
    fqn = f"{cls.__module__}.{cls.__qualname__}"
    digest = hashlib.sha256(fqn.encode("utf-8")).digest()
    return struct.unpack(">I", digest[:4])[0]


def _get_field_types(cls: type) -> dict[str, Any]:
    try:
        return get_type_hints(cls)
    except Exception:
        return {f.name: f.type if isinstance(f.type, type) else Any for f in fields(cls)}




def _encode_value(value: Any, field_type: type) -> Any:
    if value is None:
        return None

    origin = get_origin(field_type)
    args = get_args(field_type)

    if origin is tuple:
        if args:
            if len(args) == 2 and args[1] is ...:
                return [_encode_value(v, args[0]) for v in value]
            return [_encode_value(v, t) for v, t in zip(value, args)]
        return list(value)

    if field_type is tuple:
        return list(value)

    if origin is list:
        if args:
            return [_encode_value(v, args[0]) for v in value]
        return list(value)

    if origin is dict:
        if args and len(args) >= 2:
            return {_encode_value(k, args[0]): _encode_value(v, args[1]) for k, v in value.items()}
        return dict(value)

    if origin is set or origin is frozenset:
        if args:
            return [_encode_value(v, args[0]) for v in value]
        return list(value)

    if isinstance(value, frozenset):
        return list(value)

    if isinstance(value, set):
        return list(value)

    if isinstance(value, tuple):
        return list(value)

    if is_dataclass(type(value)) and hasattr(type(value), "Codec"):
        return {"_type": type(value).__qualname__, **_serialize_dataclass(value)}

    if is_dataclass(type(value)):
        return _serialize_dataclass(value)

    return value


def _serialize_dataclass(obj: Any) -> dict[str, Any]:
    cls = type(obj)
    field_types = _get_field_types(cls)
    result = {}
    for f in fields(cls):
        value = getattr(obj, f.name)
        field_type = field_types.get(f.name, type(value))
        result[f.name] = _encode_value(value, field_type)
    return result


def _decode_value(value: Any, field_type: type) -> Any:
    if value is None:
        return None

    origin = get_origin(field_type)
    args = get_args(field_type)

    if origin is tuple:
        if args:
            if len(args) == 2 and args[1] is ...:
                return tuple(_decode_value(v, args[0]) for v in value)
            return tuple(_decode_value(v, t) for v, t in zip(value, args))
        return tuple(value)

    if field_type is tuple:
        return tuple(value)

    if origin is list:
        if args:
            return [_decode_value(v, args[0]) for v in value]
        return list(value)

    if origin is dict:
        if args and len(args) >= 2:
            return {_decode_value(k, args[0]): _decode_value(v, args[1]) for k, v in value.items()}
        return dict(value)

    if origin is set:
        if args:
            return set(_decode_value(v, args[0]) for v in value)
        return set(value)

    if origin is frozenset:
        if args:
            return frozenset(_decode_value(v, args[0]) for v in value)
        return frozenset(value)

    if _is_union_type(field_type):
        return _decode_union_value(value, field_type)

    if is_dataclass(field_type) and hasattr(field_type, "Codec"):
        return _deserialize_dataclass(value, field_type)

    if is_dataclass(field_type):
        return _deserialize_dataclass(value, field_type)

    return value


def _is_union_type(tp: type) -> bool:
    import types
    origin = get_origin(tp)
    return origin is types.UnionType


def _decode_union_value(value: Any, union_type: type) -> Any:
    args = get_args(union_type)

    if isinstance(value, dict) and "_type" in value:
        type_name = value["_type"]
        for arg in args:
            if is_dataclass(arg) and hasattr(arg, "__qualname__") and arg.__qualname__ == type_name:
                data = {k: v for k, v in value.items() if k != "_type"}
                return _deserialize_dataclass(data, cast(type, arg))

    for arg in args:
        if arg is type(None) and value is None:
            return None
        if arg is int and isinstance(value, int) and not isinstance(value, bool):
            return value
        if arg is float and isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if arg is str and isinstance(value, str):
            return value
        if arg is bool and isinstance(value, bool):
            return value
        if arg is bytes and isinstance(value, bytes):
            return value

    for arg in args:
        if is_dataclass(arg) and isinstance(value, dict):
            try:
                return _deserialize_dataclass(value, cast(type, arg))
            except (TypeError, KeyError):
                continue

    return value


def _deserialize_dataclass(data: dict[str, Any], cls: type) -> Any:
    field_types = _get_field_types(cls)
    kwargs = {}
    for f in fields(cls):
        if f.name in data:
            value = data[f.name]
            field_type = field_types.get(f.name, type(value) if value is not None else object)
            kwargs[f.name] = _decode_value(value, field_type)
    return cls(**kwargs)


class Codec[T]:
    _cls: type[T]
    _type_id: int

    def __init__(self, cls: type[T], type_id: int):
        self._cls = cls
        self._type_id = type_id

    @property
    def type_id(self) -> int:
        return self._type_id

    def serialize(self, obj: T) -> bytes:
        data = _serialize_dataclass(obj)
        payload = msgpack.packb(data, use_bin_type=True)
        return struct.pack(">I", self._type_id) + payload

    def deserialize(self, data: bytes) -> T:
        fields_dict = msgpack.unpackb(data, raw=False)
        return _deserialize_dataclass(fields_dict, self._cls)


def serializable[T](cls: type[T]) -> type[T]:
    if not is_dataclass(cls):
        raise TypeError(f"{cls.__name__} must be a dataclass")

    type_id = _compute_type_id(cls)

    if type_id in _registry:
        existing = _registry[type_id]
        if existing is not cls:
            raise ValueError(
                f"type_id collision: {cls.__module__}.{cls.__qualname__} "
                f"conflicts with {existing.__module__}.{existing.__qualname__}"
            )

    _registry[type_id] = cls

    codec_instance: Codec[T] = Codec(cls, type_id)
    computed_type_id = type_id

    class CodecWrapper:
        @staticmethod
        def serialize(obj: Any) -> bytes:
            return codec_instance.serialize(obj)

        @staticmethod
        def deserialize(data: bytes) -> Any:
            return codec_instance.deserialize(data)

        type_id: int = computed_type_id

    cls.Codec = CodecWrapper  # type: ignore[attr-defined]

    return cls


def deserialize(data: bytes) -> Any:
    if len(data) < 4:
        raise ValueError("data too short: need at least 4 bytes for type_id")

    type_id = struct.unpack(">I", data[:4])[0]
    cls = _registry.get(type_id)

    if cls is None:
        raise ValueError(f"unknown type_id: {hex(type_id)}")

    return cls.Codec.deserialize(data[4:])
