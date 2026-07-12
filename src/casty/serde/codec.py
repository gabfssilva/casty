from __future__ import annotations

import dataclasses
import datetime
import enum
import types
import typing
import uuid
from collections.abc import Callable, Mapping

import msgpack

from casty.errors import SerializationError
from casty.serde import registry

_EXT_DATETIME = 1
_EXT_UUID = 2

_MISSING = object()

type _Coercer = Callable[[object], object]

# Compiled plans: typing introspection and path strings are resolved once per
# class/annotation instead of once per message.
_field_plans: dict[type, tuple[tuple[str, _Coercer, bool], ...]] = {}
_encode_fields: dict[type, tuple[str, tuple[str, ...]]] = {}
_raw_coercers: dict[tuple[object, str], _Coercer] = {}


def encode(obj: object) -> bytes:
    cls = type(obj)
    if not registry.is_registered(cls):
        raise SerializationError(f"{cls!r} is not a registered @casty.message")
    packed: bytes = msgpack.packb(_encode_value(obj), default=_ext_default)
    return packed


def encode_raw(value: object) -> bytes:
    """Encode any supported value (not only registered messages). The receiver
    must know the expected type: decode with `decode_raw(data, annotation)`."""
    packed: bytes = msgpack.packb(_encode_value(value), default=_ext_default)
    return packed


def decode_raw(data: bytes, annotation: object, path: str = "value") -> object:
    try:
        raw: object = msgpack.unpackb(data, ext_hook=_ext_hook, strict_map_key=False)
    except Exception as exc:
        raise SerializationError(f"malformed payload: {exc}") from exc
    key = (annotation, path)
    coerce = _raw_coercers.get(key)
    if coerce is None:
        coerce = _compile(annotation, path)
        _raw_coercers[key] = coerce
    return coerce(raw)


def decode_any(data: bytes) -> object:
    """Structural decode without a type annotation: message-shaped lists
    (`[wire_name, fields]` with a registered wire name) become messages, other
    values come back as decoded msgpack."""
    try:
        raw: object = msgpack.unpackb(data, ext_hook=_ext_hook, strict_map_key=False)
    except Exception as exc:
        raise SerializationError(f"malformed payload: {exc}") from exc
    return _decode_free(raw)


def _decode_free(value: object) -> object:
    if _looks_like_message(value):
        raw = typing.cast(list[object], value)
        if registry.lookup_by_name(typing.cast(str, raw[0])) is not None:
            return _decode_message(raw)
    if isinstance(value, list):
        return [_decode_free(item) for item in value]
    if isinstance(value, dict):
        typed = typing.cast(dict[object, object], value)
        return {key: _decode_free(item) for key, item in typed.items()}
    return value


def decode(data: bytes) -> object:
    try:
        raw: object = msgpack.unpackb(data, ext_hook=_ext_hook, strict_map_key=False)
    except Exception as exc:
        raise SerializationError(f"malformed payload: {exc}") from exc
    if not _looks_like_message(raw):
        raise SerializationError("payload is not an encoded message")
    return _decode_message(typing.cast(list[object], raw))


def _encode_value(value: object) -> object:
    match value:
        case enum.Enum():
            return _encode_value(value.value)
        case None | bool() | int() | float() | str() | bytes():
            return value
        case datetime.datetime() | uuid.UUID():
            return value  # handled by _ext_default at pack time
        case list() | tuple() | set() | frozenset():
            return [_encode_value(item) for item in value]
        case Mapping():  # dict, and the paged-state wrapper around one (spec 09 §4)
            return {key: _encode_value(item) for key, item in value.items()}
        case _:
            cls = type(value)
            plan = _encode_fields.get(cls)
            if plan is None:
                wire_name = registry.wire_name_of(cls)
                if wire_name is None:
                    raise SerializationError(f"cannot serialize value of type {cls!r}")
                plan = (
                    wire_name,
                    tuple(f.name for f in dataclasses.fields(value)),  # type: ignore[arg-type]
                )
                _encode_fields[cls] = plan
            name, field_names = plan
            return [name, {f: _encode_value(getattr(value, f)) for f in field_names}]


def _ext_default(value: object) -> msgpack.ExtType:
    if isinstance(value, datetime.datetime):
        return msgpack.ExtType(_EXT_DATETIME, value.isoformat().encode())
    if isinstance(value, uuid.UUID):
        return msgpack.ExtType(_EXT_UUID, value.bytes)
    raise SerializationError(f"cannot serialize value of type {type(value)!r}")


def _ext_hook(code: int, data: bytes) -> object:
    if code == _EXT_DATETIME:
        return datetime.datetime.fromisoformat(data.decode())
    if code == _EXT_UUID:
        return uuid.UUID(bytes=data)
    return msgpack.ExtType(code, data)


def _looks_like_message(raw: object) -> bool:
    return (
        isinstance(raw, list)
        and len(raw) == 2
        and isinstance(raw[0], str)
        and isinstance(raw[1], dict)
    )


def _decode_message(raw: list[object]) -> object:
    wire_name = typing.cast(str, raw[0])
    payload = typing.cast(dict[str, object], raw[1])
    cls = registry.lookup_by_name(wire_name)
    if cls is None:
        raise SerializationError(f"unknown wire name {wire_name!r}")
    kwargs: dict[str, object] = {}
    for name, coerce, required in _field_plan(cls, wire_name):
        if name in payload:
            kwargs[name] = coerce(payload[name])
        elif required:
            raise SerializationError(f"{wire_name}.{name}: field missing and has no default")
        # unknown keys in payload are ignored (forward compat)
    return cls(**kwargs)


def _field_plan(cls: type, wire_name: str) -> tuple[tuple[str, _Coercer, bool], ...]:
    plan = _field_plans.get(cls)
    if plan is None:
        hints = registry.fields_of(cls)
        plan = tuple(
            (
                f.name,
                _compile(hints[f.name], f"{wire_name}.{f.name}"),
                f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING,
            )
            for f in dataclasses.fields(cls)
        )
        _field_plans[cls] = plan
    return plan


def _compile(tp: object, path: str) -> _Coercer:
    if tp is None or tp is types.NoneType:

        def coerce_none(value: object) -> object:
            if value is None:
                return None
            raise SerializationError(f"{path}: expected None, got {type(value)!r}")

        return coerce_none
    origin = typing.get_origin(tp)
    if origin is None:
        return _compile_plain(typing.cast(type, tp), path)
    args = typing.get_args(tp)
    if origin in (types.UnionType, typing.Union):
        return _compile_union(args, path)
    if origin is tuple:
        return _compile_tuple(args, path)
    if origin in (list, set, frozenset):
        item = _compile(args[0], f"{path}[]")
        make: Callable[[list[object]], object] = (
            (lambda items: items) if origin is list else origin
        )

        def coerce_sequence(value: object) -> object:
            if not isinstance(value, list):
                raise SerializationError(f"{path}: expected sequence, got {type(value)!r}")
            return make([item(v) for v in value])

        return coerce_sequence
    if origin is dict:
        item = _compile(args[1], f"{path}[]")

        def coerce_dict(value: object) -> object:
            if not isinstance(value, dict):
                raise SerializationError(f"{path}: expected map, got {type(value)!r}")
            return {key: item(v) for key, v in value.items()}

        return coerce_dict
    raise SerializationError(f"{path}: unsupported annotation {tp!r}")


def _compile_plain(tp: type, path: str) -> _Coercer:
    if registry.is_registered(tp):

        def coerce_message(value: object) -> object:
            if not _looks_like_message(value):
                raise SerializationError(f"{path}: expected encoded message, got {type(value)!r}")
            decoded = _decode_message(typing.cast(list[object], value))
            if isinstance(decoded, tp):
                return decoded
            raise SerializationError(
                f"{path}: expected {tp.__qualname__}, got {type(decoded).__qualname__}"
            )

        return coerce_message
    if issubclass(tp, enum.Enum):

        def coerce_enum(value: object) -> object:
            try:
                return tp(value)
            except ValueError as exc:
                raise SerializationError(f"{path}: {exc}") from exc

        return coerce_enum
    if tp is float:

        def coerce_float(value: object) -> object:
            if isinstance(value, float):
                return value
            if isinstance(value, int) and not isinstance(value, bool):
                return float(value)
            raise SerializationError(f"{path}: expected float, got {type(value).__qualname__}")

        return coerce_float
    if tp is int:

        def coerce_int(value: object) -> object:
            if isinstance(value, int) and not isinstance(value, bool):
                return value
            raise SerializationError(f"{path}: expected int, got {type(value).__qualname__}")

        return coerce_int

    def coerce_plain(value: object) -> object:
        if isinstance(value, tp):
            return value
        raise SerializationError(
            f"{path}: expected {tp.__qualname__}, got {type(value).__qualname__}"
        )

    return coerce_plain


def _compile_union(args: tuple[object, ...], path: str) -> _Coercer:
    allows_none = types.NoneType in args
    members = tuple(_compile(arg, path) for arg in args if arg is not types.NoneType)

    def coerce_union(value: object) -> object:
        if value is None and allows_none:
            return None
        if _looks_like_message(value):
            # disambiguate by embedded wire name
            raw = typing.cast(list[object], value)
            cls = registry.lookup_by_name(typing.cast(str, raw[0]))
            if cls is not None and cls in args:
                return _decode_message(raw)
        for member in members:
            try:
                return member(value)
            except SerializationError:
                continue
        raise SerializationError(f"{path}: value does not match any union member")

    return coerce_union


def _compile_tuple(args: tuple[object, ...], path: str) -> _Coercer:
    if len(args) == 2 and args[1] is Ellipsis:
        item = _compile(args[0], f"{path}[]")

        def coerce_var_tuple(value: object) -> object:
            if not isinstance(value, list):
                raise SerializationError(f"{path}: expected sequence, got {type(value)!r}")
            return tuple(item(v) for v in value)

        return coerce_var_tuple
    items = tuple(_compile(arg, f"{path}[{i}]") for i, arg in enumerate(args))

    def coerce_tuple(value: object) -> object:
        if not isinstance(value, list):
            raise SerializationError(f"{path}: expected sequence, got {type(value)!r}")
        if len(value) != len(items):
            raise SerializationError(
                f"{path}: expected tuple of {len(items)} items, got {len(value)}"
            )
        return tuple(coerce(v) for coerce, v in zip(items, value, strict=True))

    return coerce_tuple
