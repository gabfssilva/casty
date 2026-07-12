from __future__ import annotations

import asyncio
import contextlib
import typing
from collections.abc import AsyncIterator

from casty.actors.registry import ActorInfo, MethodInfo
from casty.errors import CastyError

if typing.TYPE_CHECKING:
    from casty.actors.host import ActorHost


class StreamSink(typing.Protocol):
    """Output side of a streaming handler (spec 07). The handler emits elements
    (server/duplex) or a single terminal value (client-streaming) via `send`,
    then `close` on normal completion or `fail` on error. Implemented in-process
    by `QueuePipe` and over the wire by the node's bulk-stream sink."""

    async def send(self, elem: object) -> None: ...

    async def close(self) -> None: ...

    async def fail(self, exc: CastyError) -> None: ...


_ITEM = "item"
_END = "end"
_ERR = "err"


# Bounded so a producer that outpaces the consumer suspends on `send` instead of
# flooding: this both throttles (local backpressure) and yields to the loop, so a
# non-awaiting handler stays cancellable when the consumer abandons the stream.
_LOCAL_BUFFER = 16


class QueuePipe:
    """In-process `StreamSink` that is also an `AsyncIterator`: the handler sends
    on one end, the consumer iterates the other. Bounded buffer (see above)."""

    def __init__(self, buffer: int = _LOCAL_BUFFER) -> None:
        self._q: asyncio.Queue[tuple[str, object]] = asyncio.Queue(maxsize=buffer)

    async def send(self, elem: object) -> None:
        await self._q.put((_ITEM, elem))

    async def close(self) -> None:
        await self._q.put((_END, None))

    async def fail(self, exc: CastyError) -> None:
        await self._q.put((_ERR, exc))

    def __aiter__(self) -> AsyncIterator[object]:
        return self

    async def __anext__(self) -> object:
        kind, value = await self._q.get()
        if kind == _ITEM:
            return value
        if kind == _END:
            raise StopAsyncIteration
        raise typing.cast(CastyError, value)


async def local_stream_out(
    host: ActorHost,
    info: ActorInfo,
    key: str,
    method: MethodInfo,
    kwargs: dict[str, object],
    in_iter: AsyncIterator[object] | None,
    chain: list[str],
) -> AsyncIterator[object]:
    """Drive a server/duplex streaming handler on `host` in-process and yield its
    (already-decoded) elements. Cancels the handler if the consumer abandons."""
    pipe = QueuePipe()
    driver = asyncio.ensure_future(
        host.dispatch_stream(info, key, method, kwargs, in_iter, pipe, chain)
    )
    try:
        async for elem in pipe:
            yield elem
    finally:
        if not driver.done():
            driver.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await driver


async def local_stream_in(
    host: ActorHost,
    info: ActorInfo,
    key: str,
    method: MethodInfo,
    kwargs: dict[str, object],
    in_iter: AsyncIterator[object] | None,
    chain: list[str],
) -> object:
    """Drive a client-streaming handler in-process and return its single result."""
    pipe = QueuePipe()
    await host.dispatch_stream(info, key, method, kwargs, in_iter, pipe, chain)
    return await anext(pipe)
