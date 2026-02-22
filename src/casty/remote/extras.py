"""Optional serializers requiring external packages.

Each class lazily imports its dependency so users only need to install
what they actually use.

- ``MsgpackSerializer`` -- requires ``msgpack`` (``pip install casty[msgpack]``)
- ``CloudPickleSerializer`` -- requires ``cloudpickle`` (``pip install casty[cloudpickle]``)
- ``Lz4CompressedSerializer`` -- requires ``lz4`` (``pip install casty[lz4]``)
"""

from __future__ import annotations

import importlib
import pickle
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from casty.core.address import ActorAddress
from casty.remote.serialization import JsonSerializer, TypeRegistry

if TYPE_CHECKING:
    from casty.remote.ref import RemoteActorRef


def _lazy_import(module_name: str, extra: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        msg = f"'{module_name}' is required. Install with: pip install casty[{extra}]"
        raise ModuleNotFoundError(msg) from None


class MsgpackSerializer:
    """MessagePack serializer with structured type handling.

    Reuses the same recursive dict-conversion as ``JsonSerializer`` but
    encodes to MessagePack binary format, giving compact output with
    fast encode/decode.

    Requires the optional ``msgpack`` package.

    Parameters
    ----------
    registry : TypeRegistry
        Registry for resolving type names.

    Examples
    --------
    >>> registry = TypeRegistry()
    >>> ser = MsgpackSerializer(registry)
    >>> ser.deserialize(ser.serialize({"key": "value"}))
    {'key': 'value'}
    """

    def __init__(self, registry: TypeRegistry | None = None) -> None:
        self._registry = registry or TypeRegistry()
        self._converter = JsonSerializer(self._registry)

    @property
    def registry(self) -> TypeRegistry:
        """Return the underlying type registry."""
        return self._registry

    def serialize[M](self, obj: M) -> bytes:
        """Serialize an object to MessagePack bytes.

        Parameters
        ----------
        obj : M
            Object to serialize.

        Returns
        -------
        bytes
        """
        msgpack = _lazy_import("msgpack", "msgpack")
        payload = self._converter.to_dict(obj)
        return msgpack.packb(payload, use_bin_type=True)  # type: ignore[no-any-return]

    def deserialize[M, R](
        self,
        data: bytes,
        *,
        ref_factory: Callable[[ActorAddress], RemoteActorRef[R]] | None = None,
    ) -> M:
        """Deserialize MessagePack bytes back to a Python object.

        Parameters
        ----------
        data : bytes
            Bytes from ``serialize``.
        ref_factory : Callable or None
            Factory for reconstructing ``ActorRef`` instances from addresses.

        Returns
        -------
        Any
        """
        msgpack = _lazy_import("msgpack", "msgpack")
        payload: object = msgpack.unpackb(data, raw=False)
        return self._converter.from_dict(payload, ref_factory=ref_factory)


class CloudPickleSerializer:
    """CloudPickle-based serializer for lambdas and closures.

    Drop-in replacement for ``PickleSerializer`` that handles lambdas,
    closures, and dynamically defined classes via ``cloudpickle``.
    Deserialization uses standard ``pickle.loads``.

    Requires the optional ``cloudpickle`` package.

    Examples
    --------
    >>> ser = CloudPickleSerializer()
    >>> ser.deserialize(ser.serialize({"key": "value"}))
    {'key': 'value'}
    """

    def serialize[M](self, obj: M) -> bytes:
        """Serialize an object using cloudpickle (protocol 5).

        Parameters
        ----------
        obj : M
            Object to serialize.

        Returns
        -------
        bytes
        """
        cloudpickle = _lazy_import("cloudpickle", "cloudpickle")
        return cloudpickle.dumps(obj, protocol=5)  # type: ignore[no-any-return]

    def deserialize[M, R](
        self,
        data: bytes,
        *,
        ref_factory: Callable[[ActorAddress], RemoteActorRef[R]] | None = None,
    ) -> M:
        """Deserialize bytes back to a Python object.

        Uses standard ``pickle.loads`` since cloudpickle only customizes
        the dump side.

        Parameters
        ----------
        data : bytes
            Bytes from ``serialize``.
        ref_factory : Callable or None
            Factory for reconstructing ``ActorRef`` instances from addresses.

        Returns
        -------
        Any
        """
        from casty.remote import ref as _ref_module

        if ref_factory is not None:
            previous = getattr(_ref_module.thread_local, "ref_restore_hook", None)
            _ref_module.thread_local.ref_restore_hook = lambda uri: ref_factory(  # pyright: ignore[reportUnknownLambdaType]
                ActorAddress.from_uri(uri)  # pyright: ignore[reportUnknownArgumentType]
            )
            try:
                return pickle.loads(data)  # noqa: S301
            finally:
                _ref_module.thread_local.ref_restore_hook = previous
        return pickle.loads(data)  # noqa: S301


class Lz4CompressedSerializer:
    """Wraps any serializer with LZ4 frame compression.

    Applies LZ4 compression on ``serialize()`` and decompression on
    ``deserialize()``, delegating actual serialization to the wrapped
    *inner* serializer.

    Requires the optional ``lz4`` package.

    Parameters
    ----------
    inner : Serializer
        The underlying serializer to wrap.
    level : int
        LZ4 compression level (0 = default, higher = more compression).

    Examples
    --------
    >>> from casty.remote.serialization import PickleSerializer
    >>> ser = Lz4CompressedSerializer(PickleSerializer())
    >>> ser.deserialize(ser.serialize({"key": "value"}))
    {'key': 'value'}
    """

    def __init__(
        self,
        inner: Any,
        *,
        level: int = 0,
    ) -> None:
        self._inner = inner
        self._level = level

    def serialize[M](self, obj: M) -> bytes:
        """Serialize and compress an object to bytes.

        Parameters
        ----------
        obj : M
            Object to serialize.

        Returns
        -------
        bytes
            LZ4-compressed serialized bytes.
        """
        lz4_frame = _lazy_import("lz4.frame", "lz4")
        raw: bytes = self._inner.serialize(obj)
        return lz4_frame.compress(raw, compression_level=self._level)  # type: ignore[no-any-return]

    def deserialize[M, R](
        self,
        data: bytes,
        *,
        ref_factory: Callable[[ActorAddress], RemoteActorRef[R]] | None = None,
    ) -> M:
        """Decompress and deserialize bytes back to an object.

        Parameters
        ----------
        data : bytes
            LZ4-compressed bytes from ``serialize``.
        ref_factory : Callable or None
            Factory for reconstructing ``ActorRef`` instances.

        Returns
        -------
        Any
        """
        lz4_frame = _lazy_import("lz4.frame", "lz4")
        raw: bytes = lz4_frame.decompress(data)
        return self._inner.deserialize(raw, ref_factory=ref_factory)  # type: ignore[no-any-return]
