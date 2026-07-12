from __future__ import annotations

import dataclasses
import datetime
import enum
import sys
import types
import typing
import uuid
from collections.abc import Callable

from casty.errors import SerializationSchemaError

_PRIMITIVES: tuple[type, ...] = (int, float, bool, str, bytes)
_EXT_TYPES: tuple[type, ...] = (datetime.datetime, uuid.UUID)
_DICT_KEY_TYPES: tuple[type, ...] = (str, int, bytes)

_by_name: dict[str, type] = {}
_by_type: dict[type, str] = {}
_hints: dict[type, dict[str, object]] = {}


def lookup_by_name(wire_name: str) -> type | None:
    return _by_name.get(wire_name)


def wire_name_of(cls: type) -> str | None:
    return _by_type.get(cls)


def is_registered(cls: type) -> bool:
    return cls in _by_type


def fields_of(cls: type) -> dict[str, object]:
    """Resolved field annotations of a registered message (cached at registration)."""
    return _hints[cls]


@typing.overload
def message[T](cls: type[T], /) -> type[T]: ...


@typing.overload
def message[T](*, name: str) -> Callable[[type[T]], type[T]]: ...


@typing.dataclass_transform(eq_default=True, field_specifiers=(dataclasses.field,))
def message(
    cls: type | None = None, /, *, name: str | None = None
) -> type | Callable[[type], type]:
    """Declare a wire message: a class whose instances cross the network.

    The class is dataclass-ified (`slots=True, eq=True`, mutable) unless it
    already is one, registered globally under its wire name, and every field
    annotation is validated at decoration time. Serializable annotations:
    primitives (int, float, bool, str, bytes), datetime, uuid, enums with
    primitive values, other registered messages, and list / set / frozenset /
    tuple / dict (str, int or bytes keys) / unions of those. Schema evolution
    is tolerant both ways: new fields with defaults are accepted by old
    receivers, unknown fields are ignored.

    Parameters
    ----------
    name : str | None
        Stable wire name. Defaults to `module.QualName` — set it explicitly
        to move or rename the class later without breaking the cluster.

    Raises
    ------
    SerializationSchemaError
        At import time: an unserializable field annotation or a duplicate
        wire name.

    Examples
    --------
    >>> @casty.message
    ... class Order:
    ...     sku: str
    ...     quantity: int = 1
    """
    if cls is None:

        def apply(inner: type) -> type:
            return _register(inner, name)

        return apply
    return _register(cls, name)


def _register(cls: type, name: str | None) -> type:
    if not dataclasses.is_dataclass(cls):
        cls = dataclasses.dataclass(slots=True, eq=True)(cls)
    wire_name = name or f"{cls.__module__}.{cls.__qualname__}"
    existing = _by_name.get(wire_name)
    if existing is not None:
        raise SerializationSchemaError(
            f"wire name {wire_name!r} already registered by {existing!r}"
        )
    # Register before validating so self-referential fields (`Node | None`) resolve;
    # roll back if validation fails.
    _by_name[wire_name] = cls
    _by_type[cls] = wire_name
    try:
        hints = _resolve_hints(cls, wire_name)
        for field_name, annotation in hints.items():
            _validate(annotation, f"{wire_name}.{field_name}")
        _hints[cls] = hints
    except Exception:
        del _by_name[wire_name]
        del _by_type[cls]
        raise
    return cls


def _resolve_hints(cls: type, wire_name: str) -> dict[str, object]:
    """Resolve field annotations per field, with the class itself in scope
    (self-references work at decoration time) and errors carrying the field path."""
    module = sys.modules.get(cls.__module__)
    module_ns = vars(module) if module is not None else {}
    local_ns = {cls.__name__: cls}
    hints: dict[str, object] = {}
    for f in dataclasses.fields(cls):
        annotation: object = f.type
        if isinstance(annotation, str):
            try:
                annotation = eval(annotation, module_ns, local_ns)
            except NameError as exc:
                raise SerializationSchemaError(
                    f"{wire_name}.{f.name}: cannot resolve annotation {f.type!r} ({exc}); "
                    f"types must be importable or registered before use"
                ) from exc
        hints[f.name] = annotation
    return hints


def _validate(tp: object, path: str) -> None:
    if tp is None or tp is types.NoneType:
        return
    origin = typing.get_origin(tp)
    if origin is None:
        if isinstance(tp, type):
            if tp in _PRIMITIVES or tp in _EXT_TYPES:
                return
            if issubclass(tp, enum.Enum):
                for member in tp:
                    if not isinstance(member.value, _PRIMITIVES):
                        raise SerializationSchemaError(
                            f"{path}: enum {tp.__qualname__}.{member.name} "
                            f"has non-primitive value {member.value!r}"
                        )
                return
            if tp in _by_type:
                return
            raise SerializationSchemaError(
                f"{path}: {tp!r} is not serializable (not a primitive, "
                f"registered @casty.message, enum, datetime or uuid)"
            )
        raise SerializationSchemaError(f"{path}: unsupported annotation {tp!r}")
    args = typing.get_args(tp)
    if origin in (list, set, frozenset):
        _validate(args[0], f"{path}[]")
        return
    if origin is dict:
        key_tp = args[0]
        if key_tp not in _DICT_KEY_TYPES:
            raise SerializationSchemaError(
                f"{path}: dict keys must be str, int or bytes, got {key_tp!r}"
            )
        _validate(args[1], f"{path}[value]")
        return
    if origin is tuple:
        for i, arg in enumerate(args):
            if arg is Ellipsis:
                continue
            _validate(arg, f"{path}[{i}]")
        return
    if origin in (types.UnionType, typing.Union):
        for arg in args:
            _validate(arg, path)
        return
    raise SerializationSchemaError(f"{path}: unsupported annotation {tp!r}")
