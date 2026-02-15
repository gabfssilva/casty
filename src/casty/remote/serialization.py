"""Message serialization for actor communication.

Provides the ``Serializer`` protocol and concrete implementations
(``JsonSerializer``, ``PickleSerializer``), plus ``TypeRegistry`` for
mapping between type names and Python classes.
"""

from __future__ import annotations

import dataclasses
import enum
import importlib
import json
import logging
import pickle
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from casty.core.address import ActorAddress

if TYPE_CHECKING:
    from casty.remote.ref import RemoteActorRef

logger = logging.getLogger("casty.serialization")


class TypeRegistry:
    """Bidirectional mapping between fully-qualified type names and Python classes.

    Supports both explicit registration and auto-resolution via
    ``importlib``.

    Examples
    --------
    >>> from dataclasses import dataclass
    >>> @dataclass(frozen=True)
    ... class Ping:
    ...     value: int
    >>> registry = TypeRegistry()
    >>> registry.register(Ping)
    >>> registry.resolve(registry.type_name(Ping)) is Ping
    True
    """

    def __init__(self) -> None:
        self._name_to_type: dict[str, type] = {}
        self._type_to_name: dict[type, str] = {}

    def register(self, cls: type) -> None:
        """Register a type for serialization.

        Parameters
        ----------
        cls : type
            The class to register.
        """
        name = f"{cls.__module__}.{cls.__qualname__}"
        self._name_to_type[name] = cls
        self._type_to_name[cls] = name

    def register_all(self, *types: type) -> None:
        """Register multiple types at once.

        Parameters
        ----------
        *types : type
            Classes to register.

        Examples
        --------
        >>> registry = TypeRegistry()
        >>> registry.register_all(int, str)
        >>> registry.is_registered(int)
        True
        """
        for cls in types:
            self.register(cls)

    def resolve(self, name: str) -> type:
        """Resolve a fully-qualified type name to a Python class.

        Falls back to auto-resolution via ``importlib`` if the name is
        not explicitly registered.

        Parameters
        ----------
        name : str
            Fully-qualified type name (e.g. ``mymod.MyClass``).

        Returns
        -------
        type

        Raises
        ------
        KeyError
            If the type cannot be resolved.
        """
        if name in self._name_to_type:
            return self._name_to_type[name]
        cls = self._auto_resolve(name)
        self.register(cls)
        return cls

    def type_name(self, cls: type) -> str:
        """Return the fully-qualified name for a type, registering it if needed.

        Parameters
        ----------
        cls : type
            The class to look up.

        Returns
        -------
        str
            Fully-qualified name (e.g. ``mymod.MyClass``).
        """
        if cls not in self._type_to_name:
            self.register(cls)
        return self._type_to_name[cls]

    def _auto_resolve(self, name: str) -> type:
        parts = name.split(".")
        for i in range(len(parts) - 1, 0, -1):
            module_path = ".".join(parts[:i])
            attr_path = parts[i:]
            try:
                mod = importlib.import_module(module_path)
                obj: Any = mod
                for attr in attr_path:
                    obj = getattr(obj, attr)
                if isinstance(obj, type):
                    return obj
            except (ImportError, AttributeError):
                continue
        logger.warning("Failed to resolve type: %s", name)
        msg = f"Cannot resolve type: {name}"
        raise KeyError(msg)

    def is_registered(self, cls: type) -> bool:
        """Return ``True`` if the type is already registered.

        Parameters
        ----------
        cls : type
            The class to check.

        Returns
        -------
        bool

        Examples
        --------
        >>> registry = TypeRegistry()
        >>> registry.is_registered(int)
        False
        >>> registry.register(int)
        >>> registry.is_registered(int)
        True
        """
        return cls in self._type_to_name


@runtime_checkable
class Serializer(Protocol):
    """Protocol for message serialization and deserialization.

    Implementations must handle ``ActorRef`` round-tripping via the
    ``ref_factory`` keyword argument passed to ``deserialize()``.

    Examples
    --------
    Minimal implementation:

    >>> class MySerializer:
    ...     def serialize(self, obj: object) -> bytes: ...
    ...     def deserialize(self, data: bytes, *, ref_factory=None) -> object: ...
    """

    def serialize[M](self, obj: M) -> bytes:
        """Serialize an object to bytes."""
        ...

    def deserialize[M, R](
        self,
        data: bytes,
        *,
        ref_factory: Callable[[ActorAddress], RemoteActorRef[R]] | None = None,
    ) -> M:
        """Deserialize bytes back to an object."""
        ...


class JsonSerializer:
    """JSON-based serializer with recursive handling of Casty types.

    Handles ``ActorRef``, ``frozenset``, ``Enum``, ``dict``, ``tuple``,
    and frozen dataclasses registered in the ``TypeRegistry``.

    Parameters
    ----------
    registry : TypeRegistry
        Registry for resolving type names.

    Examples
    --------
    >>> registry = TypeRegistry()
    >>> ser = JsonSerializer(registry)
    >>> ser.deserialize(ser.serialize({"key": "value"}))
    {'key': 'value'}
    """

    def __init__(self, registry: TypeRegistry) -> None:
        self._registry = registry

    @property
    def registry(self) -> TypeRegistry:
        """Return the underlying type registry."""
        return self._registry

    def serialize[M](self, obj: M) -> bytes:
        """Serialize an object to JSON bytes.

        Parameters
        ----------
        obj : M
            Object to serialize.

        Returns
        -------
        bytes
            UTF-8 encoded JSON.
        """
        payload = self._to_dict(obj)
        return json.dumps(payload).encode("utf-8")

    def deserialize[M, R](
        self,
        data: bytes,
        *,
        ref_factory: Callable[[ActorAddress], RemoteActorRef[R]] | None = None,
    ) -> M:
        """Deserialize JSON bytes back to a Python object.

        Parameters
        ----------
        data : bytes
            UTF-8 encoded JSON from ``serialize``.
        ref_factory : Callable or None
            Factory for reconstructing ``ActorRef`` instances from addresses.

        Returns
        -------
        Any
        """
        payload: object = json.loads(data.decode("utf-8"))
        return self._from_dict(payload, ref_factory=ref_factory)

    def _to_dict(self, value: Any) -> object:
        from casty.remote.ref import RemoteActorRef

        match value:
            case RemoteActorRef():
                return {"__ref__": value.address.to_uri()}
            case enum.Enum():
                enum_cls = type(value)
                return {
                    "__enum__": f"{enum_cls.__module__}.{enum_cls.__qualname__}",
                    "value": value.name,
                }
            case frozenset():
                return {
                    "__frozenset__": [
                        self._to_dict(item) for item in cast(frozenset[object], value)
                    ]
                }
            case _ if dataclasses.is_dataclass(value) and not isinstance(value, type):
                type_name = self._registry.type_name(type(value))
                result: dict[str, object] = {"_type": type_name}
                for f in dataclasses.fields(value):
                    field_value: object = getattr(value, f.name)
                    result[f.name] = self._to_dict(field_value)
                return result
            case dict():
                return {
                    "__dict__": [
                        [self._to_dict(k), self._to_dict(v)]
                        for k, v in cast(dict[object, object], value).items()
                    ]
                }
            case list():
                return [self._to_dict(item) for item in cast(list[object], value)]
            case tuple():
                return {
                    "__tuple__": [
                        self._to_dict(item) for item in cast(tuple[object, ...], value)
                    ]
                }
            case _:
                return value

    def _from_dict[R](
        self,
        value: object,
        *,
        ref_factory: Callable[[ActorAddress], RemoteActorRef[R]] | None = None,
    ) -> Any:
        match value:
            case {"__ref__": str() as uri}:
                addr = ActorAddress.from_uri(uri)
                if ref_factory is not None:
                    return ref_factory(addr)
                msg = "Cannot deserialize ActorRef without ref_factory"
                raise ValueError(msg)
            case {"__enum__": str() as enum_type_name, "value": str() as member_name}:
                enum_cls = self._registry.resolve(enum_type_name)
                return enum_cls[member_name]  # type: ignore[index]
            case {"__frozenset__": list() as _fs}:  # pyright: ignore[reportUnknownVariableType]
                items = cast(list[object], _fs)
                return frozenset(
                    self._from_dict(item, ref_factory=ref_factory) for item in items
                )
            case {"__dict__": list() as _pairs}:  # pyright: ignore[reportUnknownVariableType]
                pairs = cast(list[list[object]], _pairs)
                return {
                    self._from_dict(pair[0], ref_factory=ref_factory): self._from_dict(
                        pair[1], ref_factory=ref_factory
                    )
                    for pair in pairs
                }
            case {"__tuple__": list() as _tup}:  # pyright: ignore[reportUnknownVariableType]
                items = cast(list[object], _tup)
                return tuple(
                    self._from_dict(item, ref_factory=ref_factory) for item in items
                )
            case {"_type": str() as type_name}:
                cls = self._registry.resolve(type_name)
                fields = dataclasses.fields(cls)
                str_dict = cast(dict[str, object], value)
                kwargs: dict[str, Any] = {}
                for f in fields:
                    if f.name in str_dict:
                        kwargs[f.name] = self._from_dict(
                            str_dict[f.name], ref_factory=ref_factory
                        )
                return cls(**kwargs)
            case dict():
                return cast(dict[str, object], value)
            case list():
                return [
                    self._from_dict(item, ref_factory=ref_factory)
                    for item in cast(list[object], value)
                ]
            case _:
                return value


class PickleSerializer:
    """Pickle-based serializer using protocol 5.

    Faster than JSON but not human-readable. Suitable for trusted
    environments where all nodes run the same code.

    Examples
    --------
    >>> ser = PickleSerializer()
    >>> ser.deserialize(ser.serialize({"key": "value"}))
    {'key': 'value'}
    """

    def serialize[M](self, obj: M) -> bytes:
        """Serialize an object to pickle bytes (protocol 5).

        Parameters
        ----------
        obj : M
            Object to serialize.

        Returns
        -------
        bytes
        """
        return pickle.dumps(obj, protocol=5)

    def deserialize[M, R](
        self,
        data: bytes,
        *,
        ref_factory: Callable[[ActorAddress], RemoteActorRef[R]] | None = None,
    ) -> M:
        """Deserialize pickle bytes back to a Python object.

        Temporarily installs ``ref_factory`` as the module-level restore
        hook for the duration of ``pickle.loads()``, then restores the
        previous value.

        Parameters
        ----------
        data : bytes
            Bytes produced by ``serialize``.
        ref_factory : Callable or None
            Factory for reconstructing ``ActorRef`` instances from addresses.

        Returns
        -------
        Any
        """
        from casty.remote import ref as _ref_module

        if ref_factory is not None:
            previous = _ref_module.ref_restore_hook
            _ref_module.ref_restore_hook = lambda uri: ref_factory(
                ActorAddress.from_uri(uri)
            )
            try:
                return pickle.loads(data)  # noqa: S301
            finally:
                _ref_module.ref_restore_hook = previous
        return pickle.loads(data)  # noqa: S301
