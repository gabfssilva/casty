from __future__ import annotations

import typing
from collections.abc import AsyncIterator, Callable

from casty.actors.context import context_or_none
from casty.actors.registry import ActorInfo, MethodInfo


class Caller(typing.Protocol):
    """Implemented by Node and Client: route one actor call to its owner."""

    async def _call_actor(
        self, info: ActorInfo, key: str, method: MethodInfo, args: list[object], chain: list[str]
    ) -> object: ...

    def _stream_out(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[object]: ...

    async def _stream_in(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> object: ...


class ActorProxy:
    """Runtime side of `node.actor(Cls, key)`. Statically the caller sees `Cls`
    (cast); at runtime attribute access resolves to routed async calls. Creating
    a proxy costs nothing — activation happens at the owner, on first call.

    Streaming methods (spec 07) resolve to a routed `AsyncIterator` (server/duplex,
    consumed with `async for`) or an awaitable of the single result (client-
    streaming); the annotations on `Cls` already type both correctly."""

    def __init__(self, caller: Caller, info: ActorInfo, key: str) -> None:
        self._caller = caller
        self._info = info
        self._key = key

    def __getattr__(self, name: str) -> Callable[..., object]:
        method = self._info.methods.get(name)
        if method is None:
            raise AttributeError(f"{self._info.wire_name} has no actor method {name!r}")

        if method.stream_out is not None:
            def call_stream_out(*args: object, **kwargs: object) -> AsyncIterator[object]:
                unary, in_iter = _split_stream_args(self._info, method, args, kwargs)
                return self._caller._stream_out(
                    self._info, self._key, method, unary, in_iter, _chain()
                )

            return call_stream_out

        if method.stream_in is not None:
            async def call_stream_in(*args: object, **kwargs: object) -> object:
                unary, in_iter = _split_stream_args(self._info, method, args, kwargs)
                return await self._caller._stream_in(
                    self._info, self._key, method, unary, in_iter, _chain()
                )

            return call_stream_in

        async def call(*args: object, **kwargs: object) -> object:
            ordered = _order_args(self._info, method, args, kwargs)
            return await self._caller._call_actor(
                self._info, self._key, method, ordered, _chain()
            )

        return call

    def __repr__(self) -> str:
        return f"<actor {self._info.wire_name}/{self._key}>"


def _chain() -> list[str]:
    ctx = context_or_none()
    return ctx.chain if ctx is not None else []


def _order_args(
    info: ActorInfo, method: MethodInfo, args: tuple[object, ...], kwargs: dict[str, object]
) -> list[object]:
    """Flatten positional + keyword arguments into the declared parameter order.
    Every parameter must be present (defaults on actor methods would need to be
    applied owner-side; requiring all args keeps the wire unambiguous)."""
    names = [name for name, _ in method.params]
    if len(args) > len(names):
        raise TypeError(f"{info.wire_name}.{method.name} takes {len(names)} arguments")
    ordered: dict[str, object] = dict(zip(names, args, strict=False))
    for name, value in kwargs.items():
        if name not in names:
            raise TypeError(f"{info.wire_name}.{method.name} has no parameter {name!r}")
        if name in ordered:
            raise TypeError(f"{info.wire_name}.{method.name}: duplicate argument {name!r}")
        ordered[name] = value
    missing = [name for name in names if name not in ordered]
    if missing:
        raise TypeError(f"{info.wire_name}.{method.name} missing arguments: {missing}")
    return [ordered[name] for name in names]


def _split_stream_args(
    info: ActorInfo, method: MethodInfo, args: tuple[object, ...], kwargs: dict[str, object]
) -> tuple[dict[str, object], AsyncIterator[object] | None]:
    """Bind a streaming call's arguments, then split off the AsyncIterator param
    (if any) from the unary ones."""
    names = method.param_order
    if len(args) > len(names):
        raise TypeError(f"{info.wire_name}.{method.name} takes {len(names)} arguments")
    bound: dict[str, object] = dict(zip(names, args, strict=False))
    for name, value in kwargs.items():
        if name not in names:
            raise TypeError(f"{info.wire_name}.{method.name} has no parameter {name!r}")
        if name in bound:
            raise TypeError(f"{info.wire_name}.{method.name}: duplicate argument {name!r}")
        bound[name] = value
    missing = [name for name in names if name not in bound]
    if missing:
        raise TypeError(f"{info.wire_name}.{method.name} missing arguments: {missing}")
    in_iter: AsyncIterator[object] | None = None
    if method.stream_in is not None:
        value = bound.pop(method.stream_in[0])
        if not hasattr(value, "__aiter__"):
            raise TypeError(
                f"{info.wire_name}.{method.name}({method.stream_in[0]}) expects an async iterable"
            )
        in_iter = typing.cast(AsyncIterator[object], value)
    return bound, in_iter
