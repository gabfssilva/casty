"""Inspection (spec 11): the spy event messages, the per-node hub that fans
events out to subscriptions, and the `SpyAgent` wire surface.

The hub is passive and best-effort by contract: every buffer between an actor
and an observer drops on overflow and accounts for the loss in-band (`Lag`).
Nothing here reads a clock for protocol decisions — the host stamps `ts` in;
the wall-clock stamps on synthetic `Lag`/`Gap` exist for humans to merge,
never to decide anything (spec 11 §3).
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import fnmatch
import re
import typing
import uuid
from collections import deque
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime

from casty.actors.context import current_context
from casty.actors.registry import info_by_name, info_of, register_service
from casty.errors import UnknownActorTypeError
from casty.serde import codec
from casty.serde.registry import message


@message(name="casty.spy.Selector")
class Selector:
    """What a subscription matches, as data (spec 11 §2): `fnmatch` globs,
    matched at the emitting node so only matching events cross the wire."""

    actor: str = "*"
    key: str = "*"
    payloads: bool = False


@message(name="casty.spy.Activated")
class Activated:
    actor: str
    key: str
    node: uuid.UUID
    seq: int
    ts: datetime


@message(name="casty.spy.Received")
class Received:
    actor: str
    key: str
    node: uuid.UUID
    seq: int
    ts: datetime
    method: str
    streaming: bool
    chain: list[str]
    args: list[bytes] | None = None


@message(name="casty.spy.Completed")
class Completed:
    actor: str
    key: str
    node: uuid.UUID
    seq: int
    ts: datetime
    method: str
    streaming: bool
    duration: float
    changed: list[str] | None = None
    result: bytes | None = None
    commit_error: str | None = None


@message(name="casty.spy.Failed")
class Failed:
    actor: str
    key: str
    node: uuid.UUID
    seq: int
    ts: datetime
    method: str
    error: str
    directive: str


@message(name="casty.spy.Deactivated")
class Deactivated:
    actor: str
    key: str
    node: uuid.UUID
    seq: int
    ts: datetime
    reason: str


@message(name="casty.spy.Lag")
class Lag:
    """Events were dropped between the emitting node and this observer. The
    header carries the emitting node; actor/key/seq are zeroed."""

    actor: str
    key: str
    node: uuid.UUID
    seq: int
    ts: datetime
    dropped: int


@message(name="casty.spy.Gap")
class Gap:
    """A member's event stream ended (cluster scope only): `lost` while it is
    still in the view (the dial retries), `left` when it departed."""

    actor: str
    key: str
    node: uuid.UUID
    seq: int
    ts: datetime
    reason: str


type SpyEvent = Activated | Received | Completed | Failed | Deactivated | Lag | Gap
"""Everything a subscription or interceptor can see; made for `match`."""

type Interceptor = Callable[[SpyEvent], None]
"""Inline per-node hook (spec 11 §2): sync, called in the worker, guaranteed
and ordered. Contract: return fast, never block, never raise (raises are
logged, not propagated)."""


def selector_of(actor: type | str | None, key: str, payloads: bool) -> Selector:
    """Build a `Selector` from the `spy()` arguments."""
    match actor:
        case None:
            pattern = "*"
        case str():
            pattern = actor
        case type():
            info = info_of(actor)
            if info is None:
                raise TypeError(f"{actor.__qualname__} is not a @casty.actor")
            pattern = info.wire_name
    return Selector(actor=pattern, key=key, payloads=payloads)


@typing.overload
def decode(event: Received) -> list[object]: ...


@typing.overload
def decode(event: Completed) -> object: ...


def decode(event: Received | Completed) -> list[object] | object:
    """Decode an event's payload bytes against the local registry.

    Parameters
    ----------
    event : Received | Completed
        An event whose payloads were armed (`payloads=True` on the
        subscription).

    Returns
    -------
    list[object] | object
        The handler's arguments (`Received`) or its result (`Completed`).

    Raises
    ------
    UnknownActorTypeError
        If the event's actor class is not registered on this process.
    ValueError
        If the event carries no payload bytes.
    """
    info = info_by_name(event.actor)
    if info is None:
        raise UnknownActorTypeError(f"actor type {event.actor!r} is not registered here")
    method = info.methods.get(event.method)
    if method is None:
        raise UnknownActorTypeError(f"{event.actor} has no method {event.method!r}")
    match event:
        case Received(args=args):
            if args is None:
                raise ValueError("event carries no payloads (subscription not armed)")
            return [
                codec.decode_raw(raw, annotation)
                for raw, (_, annotation) in zip(args, method.params, strict=True)
            ]
        case Completed(result=result):
            if result is None:
                raise ValueError("event carries no payloads (subscription not armed)")
            return codec.decode_raw(result, method.returns)


class Subscription(abc.ABC):
    """A live spy subscription: async context manager and async iterator of
    `SpyEvent`. `async with` is the recommended form (deterministic teardown);
    bare `async for` works and cleans up on `aclose`."""

    async def __aenter__(self) -> Subscription:
        self._open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def __aiter__(self) -> Subscription:
        self._open()
        return self

    @abc.abstractmethod
    def _open(self) -> None: ...

    @abc.abstractmethod
    async def __anext__(self) -> SpyEvent: ...

    @abc.abstractmethod
    async def aclose(self) -> None:
        """Stop the subscription and release its resources. Idempotent."""


class _Sub:
    """One registered subscription inside the hub: compiled selector, bounded
    buffer, `dropped` counter. `Lag` is emitted in-band before the next event
    whenever the counter is nonzero."""

    __slots__ = (
        "_actor_re", "_closed", "_events", "_key_re", "_wake", "dropped", "node", "payloads",
    )

    def __init__(
        self, node: uuid.UUID, selector: Selector, buffer_events: int
    ) -> None:
        self.node = node
        self.payloads = selector.payloads
        self.dropped = 0
        self._actor_re = re.compile(fnmatch.translate(selector.actor))
        self._key_re = re.compile(fnmatch.translate(selector.key))
        self._events: deque[SpyEvent] = deque(maxlen=buffer_events)
        self._wake = asyncio.Event()
        self._closed = False

    def offer(self, event: SpyEvent) -> None:
        if not (self._actor_re.match(event.actor) and self._key_re.match(event.key)):
            return
        if len(self._events) == self._events.maxlen:
            self.dropped += 1
        self._events.append(event)
        self._wake.set()

    def lag(self, n: int) -> None:
        self.dropped += n
        self._wake.set()

    def close(self) -> None:
        self._closed = True
        self._wake.set()

    def __aiter__(self) -> _Sub:
        return self

    async def __anext__(self) -> SpyEvent:
        while True:
            if self.dropped:
                dropped, self.dropped = self.dropped, 0
                return Lag(
                    actor="", key="", node=self.node, seq=0,
                    ts=datetime.now(UTC), dropped=dropped,
                )
            if self._events:
                return self._events.popleft()
            if self._closed:
                raise StopAsyncIteration
            self._wake.clear()
            await self._wake.wait()


class Hub:
    """Per-node fan-out from workers to spy subscriptions (spec 11 §4).

    Workers `publish` (O(1): `put_nowait`, move on); one drain task matches
    selectors and appends to subscription buffers. Hub-level overflow drops
    oldest and folds the loss into every live subscription's counter. Payload
    arming is global: a count of `payloads=True` subscriptions, checked by
    workers before encoding args/results.
    """

    def __init__(
        self, node: uuid.UUID, hub_events: int = 4096, buffer_events: int = 1024
    ) -> None:
        self.node = node
        self._buffer_events = buffer_events
        self._events: deque[SpyEvent] = deque(maxlen=hub_events)
        self._wake = asyncio.Event()
        self._subs: list[_Sub] = []
        self._payloads = 0
        self._lost = 0
        self._task: asyncio.Task[None] | None = None

    @property
    def active(self) -> bool:
        return bool(self._subs)

    @property
    def payloads_armed(self) -> bool:
        return self._payloads > 0

    def publish(self, event: SpyEvent) -> None:
        # SpyAgent is excluded: a wildcard subscription observing its own
        # delivery actor would feed back into itself (spec 11 §4)
        if not self._subs or event.actor == SPY_AGENT_NAME:
            return
        if len(self._events) == self._events.maxlen:
            self._lost += 1
        self._events.append(event)
        self._wake.set()

    def register(self, selector: Selector) -> _Sub:
        sub = _Sub(self.node, selector, self._buffer_events)
        self._subs.append(sub)
        if sub.payloads:
            self._payloads += 1
        if self._task is None:
            self._task = asyncio.create_task(self._drain())
        return sub

    def unregister(self, sub: _Sub) -> None:
        with contextlib.suppress(ValueError):
            self._subs.remove(sub)
        if sub.payloads:
            self._payloads -= 1
        sub.close()

    def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None
        for sub in list(self._subs):
            self.unregister(sub)

    async def _drain(self) -> None:
        while True:
            await self._wake.wait()
            self._wake.clear()
            if self._lost:
                lost, self._lost = self._lost, 0
                for sub in self._subs:
                    sub.lag(lost)
            while self._events:
                event = self._events.popleft()
                for sub in self._subs:
                    sub.offer(event)


class LocalSubscription(Subscription):
    """`scope="local"`: a direct view on this node's hub, zero wire."""

    def __init__(self, hub: Hub, selector: Selector) -> None:
        self._hub = hub
        self._selector = selector
        self._sub: _Sub | None = None

    def _open(self) -> None:
        if self._sub is None:
            self._sub = self._hub.register(self._selector)

    async def __anext__(self) -> SpyEvent:
        self._open()
        assert self._sub is not None
        return await self._sub.__anext__()

    async def aclose(self) -> None:
        if self._sub is not None:
            self._hub.unregister(self._sub)


class SpyAgent:
    """Internal wire surface of `spy(scope="cluster")` and `state_of`
    (spec 11 §6, §8). Registered as an unplaced service by hand — bypassing
    the `@casty.service` builder on purpose: "no streaming in a service" is a
    builder rule, not a host one, and `subscribe` is exactly a server-streaming
    method. One activation per subscription: the caller keys each subscribe
    with a fresh uuid, so N subscriptions run concurrently."""

    async def subscribe(
        self, selector: Selector
    ) -> AsyncIterator[Activated | Received | Completed | Failed | Deactivated | Lag | Gap]:
        hub = current_context()._host.hub
        sub = hub.register(selector)
        try:
            async for event in sub:
                yield event
        finally:
            hub.unregister(sub)

    async def state(self, actor: str, key: str) -> dict[str, bytes] | None:
        """Point-in-time read of a live activation's serializable state, or
        None when `(actor, key)` is not active on this host (spec 11 §8)."""
        values = current_context()._host.state_values(actor, key)
        if values is None:
            return None
        return {name: codec.encode_raw(value) for name, value in values.items()}


SPY_AGENT_NAME = "casty.SpyAgent"
register_service(SpyAgent, name=SPY_AGENT_NAME)
