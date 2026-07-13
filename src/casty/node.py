from __future__ import annotations

import asyncio
import contextlib
import logging
import typing
import uuid
from collections.abc import AsyncIterator, Callable, Coroutine, Generator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from casty.actors import inspection, reactivation
from casty.actors import messages as actor_messages
from casty.actors import registry as actor_registry
from casty.actors import replication as actor_replication
from casty.actors.host import ActorHost
from casty.actors.proxy import Caller
from casty.actors.registry import ActorInfo, MethodInfo, Supervisor
from casty.actors.replication import Replication
from casty.actors.streaming import local_stream_in, local_stream_out
from casty.collections._sharded import ensure as ensure_collection
from casty.config import TLS, MembershipConfig, TransportConfig
from casty.errors import (
    ActorFailedError,
    ActorUnavailableError,
    CastyError,
    QuorumUnavailableError,
    RangeMovingError,
    ReentrancyError,
    RemoteError,
    UnknownActorTypeError,
)
from casty.membership import messages as membership_messages
from casty.membership.service import Membership
from casty.membership.table import Member, Status, ViewEvent
from casty.placement.ring import Ring
from casty.serde import codec
from casty.serde import registry as serde
from casty.system import ActorSystem
from casty.transport.connection import ActorStream, Connection, LocalNode
from casty.transport.pool import Pool
from casty.transport.server import Server

logger = logging.getLogger("casty.node")


@dataclass(frozen=True)
class Config:
    """Every knob of a node or client. Defaults are production-oriented.

    Parameters
    ----------
    transport : TransportConfig
        Framing, flow control, keepalive and reconnection.
    membership : MembershipConfig
        Overlay, broadcast and failure detection.
    call_timeout : float
        Seconds a remote actor call waits for its reply.
    default_idle_timeout : float
        Seconds without messages before an activation deactivates, for actor
        classes that don't set their own `idle_timeout`.
    drain_timeout : float
        Seconds `close()` waits for in-flight handlers before giving up.
    client_sync_interval : float
        Seconds between a `Client`'s member-table refreshes.
    replication_timeout : float
        Seconds the owner waits for replica acks on a commit.
    handoff_timeout : float
        Seconds a leaving node spends handing its ranges off.
    replication_batch_bytes : int
        Page bytes batched into one replication message.
    reactivation_concurrency : int
        Reactivation pokes and marked-handoff pushes in flight at once after
        a ring change.
    reactivation_retry : float
        Seconds between retries of a failed reactivation poke or mark clear.
    spy_hub_events : int
        Events the node's spy hub buffers between workers and the drain task.
    spy_buffer_events : int
        Events buffered per spy subscription (and per cluster-scope merge
        queue); overflow drops oldest and surfaces as `Lag`.
    """

    transport: TransportConfig = field(default_factory=TransportConfig)
    membership: MembershipConfig = field(default_factory=MembershipConfig)
    call_timeout: float = 10.0
    default_idle_timeout: float = 300.0
    drain_timeout: float = 10.0
    client_sync_interval: float = 5.0
    replication_timeout: float = 5.0
    handoff_timeout: float = 10.0
    replication_batch_bytes: int = 256 * 1024
    reactivation_concurrency: int = 4
    reactivation_retry: float = 5.0
    spy_hub_events: int = 4096
    spy_buffer_events: int = 1024


class _Router(ActorSystem):
    """Shared routing: ring lookup, remote ask, WRONG_OWNER retry. Node adds the
    local-dispatch path; Client is remote-only."""

    _pool: Pool  # assigned by subclass __init__

    def __init__(self, config: Config) -> None:
        self._config = config
        self._ring: Ring | None = None
        self._addrs: dict[uuid.UUID, str] = {}
        self._members: frozenset[Member] = frozenset()
        self._stopping = False
        self._next_member = 0  # round-robin cursor for service calls
        self._spy_subs: set[_ClusterSubscription] = set()

    @property
    def ring(self) -> Ring | None:
        return self._ring

    def addr_of(self, node_id: uuid.UUID) -> str | None:
        return self._addrs.get(node_id)

    def members(self) -> frozenset[Member]:
        """The full members currently in this system's view. A `Member` is
        what `service(cls, at=...)` pins a call to."""
        return self._members

    def _rebuild_ring(self, members: frozenset[Member]) -> None:
        placed = [m for m in members if m.role == "member"]
        self._members = frozenset(placed)
        self._addrs = {m.node_id: m.addr for m in placed}
        self._ring = Ring.build([m.node_id for m in placed]) if placed else None
        for sub in list(self._spy_subs):  # cluster spies follow the view (spec 11 §7)
            sub.sync(self._addrs)

    async def _call_actor(
        self, info: ActorInfo, key: str, method: MethodInfo, args: list[object], chain: list[str]
    ) -> object:
        if self._stopping:
            raise ActorUnavailableError("this node is stopping")
        if info.kind == "service":
            return await self._call_service(info, key, method, args, chain)
        ring_key = f"{info.wire_name}/{key}"
        for attempt in range(2):
            ring = self._ring
            if ring is None:
                raise ActorUnavailableError("no cluster members known")
            owner = ring.owner(ring_key)
            try:
                local = await self._dispatch_local(owner, info, key, method, args, chain)
            except RangeMovingError:
                # stale activation was discarded; retrying re-handshakes (safe:
                # the rejected write was applied nowhere)
                if attempt == 0:
                    continue
                raise
            if local is not None:
                return local.value
            addr = self._addrs.get(owner)
            if addr is None:
                raise ActorUnavailableError(f"owner {owner} has no known address")
            try:
                conn = await self._pool.get(addr)
            except (CastyError, OSError) as exc:
                # dial failure: the call never left, retrying is safe
                if attempt == 0:
                    await self._refresh_view()
                    continue
                raise ActorUnavailableError(f"cannot reach owner at {addr}: {exc}") from exc
            call = actor_messages.ActorCall(
                actor=info.wire_name,
                key=key,
                method=method.name,
                args=[codec.encode_raw(a) for a in args],
                chain=chain,
            )
            try:
                body = await conn.ask(
                    actor_messages.ACTOR_CALL,
                    codec.encode(call),
                    timeout=self._config.call_timeout,
                )
            except RemoteError as exc:
                if exc.code == actor_messages.CODE_WRONG_OWNER:
                    if attempt == 0:
                        await self._refresh_view()
                        continue
                    raise ActorUnavailableError(
                        f"{ring_key}: ownership still disagrees after view refresh"
                    ) from exc
                if exc.code == actor_messages.CODE_RANGE_MOVING and attempt == 0:
                    continue  # stale activation discarded owner-side; retry is safe
                raise _from_remote(exc, info.wire_name, key) from exc
            return codec.decode_raw(body, method.returns)
        raise AssertionError("unreachable")  # both attempts either return or raise

    async def _call_service(
        self, info: ActorInfo, key: str, method: MethodInfo, args: list[object], chain: list[str]
    ) -> object:
        """A service has no owner on the ring — every member's activation is
        interchangeable (spec 08 §5). Client: pick one, round-robin. Node
        overrides to dispatch on its own host."""
        for attempt in range(2):
            addrs = list(self._addrs.values())
            if not addrs:
                raise ActorUnavailableError("no cluster members known")
            addr = addrs[self._next_member % len(addrs)]
            self._next_member += 1
            try:
                conn = await self._pool.get(addr)
            except (CastyError, OSError) as exc:
                # dial failure: the call never left, another member is safe to try
                if attempt == 0:
                    await self._refresh_view()
                    continue
                raise ActorUnavailableError(f"cannot reach member at {addr}: {exc}") from exc
            call = actor_messages.ActorCall(
                actor=info.wire_name,
                key=key,
                method=method.name,
                args=[codec.encode_raw(a) for a in args],
                chain=chain,
            )
            try:
                body = await conn.ask(
                    actor_messages.ACTOR_CALL,
                    codec.encode(call),
                    timeout=self._config.call_timeout,
                )
            except RemoteError as exc:
                raise _from_remote(exc, info.wire_name, key) from exc
            return codec.decode_raw(body, method.returns)
        raise AssertionError("unreachable")  # both attempts either return or raise

    async def _dispatch_local(
        self,
        owner: uuid.UUID,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        args: list[object],
        chain: list[str],
    ) -> _Local | None:
        return None  # Client: never local

    async def _refresh_view(self) -> None:
        await asyncio.sleep(0.05)  # Node: the ring converges via membership events

    # --- inspection (spec 11) ---------------------------------------------------------

    async def state_of(self, actor: type, key: str) -> dict[str, object] | None:
        info = actor_registry.info_of(actor)
        if info is None or info.kind != "actor":
            raise TypeError(f"{actor.__qualname__} is not a @casty.actor")
        ring = self._ring
        if ring is None:
            raise ActorUnavailableError("no cluster members known")
        owner = ring.owner(f"{info.wire_name}/{key}")
        local = self._state_local(owner, info, key)
        if local is not None:
            return typing.cast(dict[str, object] | None, local.value)
        addr = self._addrs.get(owner)
        if addr is None:
            raise ActorUnavailableError(f"owner {owner} has no known address")
        spy_info = actor_registry.info_by_name(inspection.SPY_AGENT_NAME)
        assert spy_info is not None
        method = spy_info.methods["state"]
        body = await self._call_service_at(addr, spy_info, method, [info.wire_name, key])
        raw = codec.decode_raw(body, method.returns)
        if raw is None:
            return None
        hints = serde.fields_of(info.state_cls)
        return {
            name: codec.decode_raw(data, hints[name])
            for name, data in typing.cast(dict[str, bytes], raw).items()
            if name in hints
        }

    def _state_local(self, owner: uuid.UUID, info: ActorInfo, key: str) -> _Local | None:
        return None  # Client: never local

    def _spy_local(
        self, node_id: uuid.UUID, selector: inspection.Selector
    ) -> inspection.Subscription | None:
        return None  # Client: never local

    async def _call_service_at(
        self,
        addr: str,
        info: ActorInfo,
        method: MethodInfo,
        args: list[object],
        chain: list[str] | None = None,
    ) -> bytes:
        """Node-directed unary service call (spec 11 §6): explicit address, no
        ring, no WRONG_OWNER retry — the target serves its own unplaced
        activation."""
        try:
            conn = await self._pool.get(addr)
        except (CastyError, OSError) as exc:
            raise ActorUnavailableError(f"cannot reach member at {addr}: {exc}") from exc
        call = actor_messages.ActorCall(
            actor=info.wire_name,
            key=actor_registry.SERVICE_KEY,
            method=method.name,
            args=[codec.encode_raw(a) for a in args],
            chain=chain or [],
        )
        try:
            return await conn.ask(
                actor_messages.ACTOR_CALL,
                codec.encode(call),
                timeout=self._config.call_timeout,
            )
        except RemoteError as exc:
            raise _from_remote(exc, info.wire_name, actor_registry.SERVICE_KEY) from exc

    async def _stream_at(self, addr: str, descriptor: bytes) -> AsyncIterator[bytes]:
        """Node-directed actor stream (spec 11 §6): `_remote_stream`'s loop body
        with an explicit address and no WRONG_OWNER retry."""
        conn = await self._pool.get(addr)
        stream = await conn.open_actor_stream(descriptor)
        await stream.finish()  # no input half
        completed = False
        try:
            async for raw in stream:
                yield raw
            completed = True
        finally:
            if not completed:
                with contextlib.suppress(Exception):
                    await stream.reset()

    # --- pinned service dispatch ------------------------------------------------------

    def _service_caller(self, info: ActorInfo, at: Member) -> Caller:
        return _Pinned(self, at)

    def _service_local_host(self, node_id: uuid.UUID) -> ActorHost | None:
        return None  # Client: never local

    async def _service_stream_at(
        self,
        addr: str,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[bytes]:
        """Member-directed actor stream with an input half: `_remote_stream`'s
        loop body against an explicit address — a service is unplaced, so there
        is no WRONG_OWNER to retry on."""
        try:
            conn = await self._pool.get(addr)
        except (CastyError, OSError) as exc:
            raise ActorUnavailableError(f"cannot reach member at {addr}: {exc}") from exc
        descriptor = codec.encode(
            actor_messages.StreamOpen(
                actor=info.wire_name,
                key=key,
                method=method.name,
                args=[codec.encode_raw(kwargs[name]) for name, _ in method.params],
                chain=chain,
            )
        )
        stream = await conn.open_actor_stream(descriptor)
        uploader: asyncio.Task[None] | None = None
        if in_iter is not None:
            uploader = asyncio.ensure_future(_upload_stream(stream, in_iter))
        else:
            await stream.finish()  # no input half
        completed = False
        try:
            async for raw in stream:
                yield raw
            completed = True
        except RemoteError as exc:
            raise _from_remote(exc, info.wire_name, key) from exc
        finally:
            await _cancel(uploader)
            if not completed:
                with contextlib.suppress(Exception):
                    await stream.reset()

    # --- streaming (spec 07) --------------------------------------------------------

    def _stream_out(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[object]:
        return self._route_stream(info, key, method, kwargs, in_iter, chain, method.stream_out)

    async def _stream_in(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> object:
        result: object = _MISSING
        async for elem in self._route_stream(
            info, key, method, kwargs, in_iter, chain, method.returns
        ):
            result = elem
        if result is _MISSING:
            raise ActorUnavailableError(f"{info.wire_name}/{key}: stream produced no result")
        return result

    async def _route_stream(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
        decode_as: object,
    ) -> AsyncIterator[object]:
        local = self._local_stream(info, key, method, kwargs, in_iter, chain)
        if local is not None:
            async for elem in local:
                yield elem
            return
        async for raw in self._remote_stream(info, key, method, kwargs, in_iter, chain):
            yield codec.decode_raw(raw, decode_as)

    def _local_stream(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[object] | None:
        return None  # Client: never local

    async def _remote_stream(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[bytes]:
        ring_key = f"{info.wire_name}/{key}"
        for attempt in range(2):
            ring = self._ring
            if ring is None:
                raise ActorUnavailableError("no cluster members known")
            owner = ring.owner(ring_key)
            addr = self._addrs.get(owner)
            if addr is None:
                raise ActorUnavailableError(f"owner {owner} has no known address")
            try:
                conn = await self._pool.get(addr)
            except (CastyError, OSError) as exc:
                if attempt == 0:
                    await self._refresh_view()
                    continue
                raise ActorUnavailableError(f"cannot reach owner at {addr}: {exc}") from exc
            descriptor = codec.encode(
                actor_messages.StreamOpen(
                    actor=info.wire_name,
                    key=key,
                    method=method.name,
                    args=[codec.encode_raw(kwargs[name]) for name, _ in method.params],
                    chain=chain,
                )
            )
            stream = await conn.open_actor_stream(descriptor)
            uploader: asyncio.Task[None] | None = None
            if in_iter is not None:
                uploader = asyncio.ensure_future(_upload_stream(stream, in_iter))
            else:
                await stream.finish()  # no input half
            yielded = False
            completed = False
            try:
                async for raw in stream:
                    yielded = True
                    yield raw
                completed = True
            except RemoteError as exc:
                if exc.code == actor_messages.CODE_WRONG_OWNER and not yielded and attempt == 0:
                    await _cancel(uploader)
                    with contextlib.suppress(Exception):
                        await stream.reset()
                    await self._refresh_view()
                    continue
                await _cancel(uploader)
                raise _from_remote(exc, info.wire_name, key) from exc
            finally:
                await _cancel(uploader)
                if not completed:
                    with contextlib.suppress(Exception):
                        await stream.reset()
            return


_MISSING = object()


class _Pinned:
    """`Caller` behind `service(cls, at=member)`: every call lands on the chosen
    member instead of the default placement (Node: local; Client: round-robin).
    A service receiver dispatches unconditionally — unplaced, no wrong owner —
    so pinning is purely the caller's choice (spec 08 §5)."""

    def __init__(self, router: _Router, member: Member) -> None:
        self._router = router
        self._member = member

    async def _call_actor(
        self, info: ActorInfo, key: str, method: MethodInfo, args: list[object], chain: list[str]
    ) -> object:
        host = self._router._service_local_host(self._member.node_id)
        if host is not None:
            return await host.dispatch(info, key, method.name, args, chain)
        body = await self._router._call_service_at(
            self._member.addr, info, method, args, chain
        )
        return codec.decode_raw(body, method.returns)

    def _stream_out(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[object]:
        return self._route_stream(info, key, method, kwargs, in_iter, chain, method.stream_out)

    async def _stream_in(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> object:
        result: object = _MISSING
        async for elem in self._route_stream(
            info, key, method, kwargs, in_iter, chain, method.returns
        ):
            result = elem
        if result is _MISSING:
            raise ActorUnavailableError(f"{info.wire_name}/{key}: stream produced no result")
        return result

    async def _route_stream(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
        decode_as: object,
    ) -> AsyncIterator[object]:
        host = self._router._service_local_host(self._member.node_id)
        if host is not None:
            if method.stream_out is not None:
                async for elem in local_stream_out(host, info, key, method, kwargs, in_iter, chain):
                    yield elem
            else:
                yield await local_stream_in(host, info, key, method, kwargs, in_iter, chain)
            return
        async for raw in self._router._service_stream_at(
            self._member.addr, info, key, method, kwargs, in_iter, chain
        ):
            yield codec.decode_raw(raw, decode_as)


async def _upload_stream(stream: ActorStream, in_iter: AsyncIterator[object]) -> None:
    """Pump the caller's input iterator up as STREAM_ITEM frames, then FIN. On
    iterator failure, tell the owner via STREAM_ERROR so its input raises."""
    try:
        async for item in in_iter:
            await stream.send_item(codec.encode_raw(item))
        await stream.finish()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        with contextlib.suppress(Exception):
            await stream.send_error(0, f"input iterator failed: {exc}")
            await stream.finish()


async def _one(coro: Coroutine[object, object, object]) -> AsyncIterator[object]:
    """Adapt a client-streaming result coroutine to the single-element iterator
    `_route_stream` consumes."""
    yield await coro


async def _cancel(task: asyncio.Task[None] | None) -> None:
    if task is not None and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _stream_input(stream: ActorStream, elem_type: object) -> AsyncIterator[object]:
    """Owner-side input iterator: decode each inbound STREAM_ITEM against the
    handler's parameter element type. Raises on STREAM_ERROR / connection loss."""
    async for raw in stream:
        yield codec.decode_raw(raw, elem_type, path="stream input")


class _WireSink:
    """`StreamSink` that puts a handler's output onto an actor stream (spec 07)."""

    def __init__(self, stream: ActorStream) -> None:
        self._stream = stream

    async def send(self, elem: object) -> None:
        await self._stream.send_item(codec.encode_raw(elem))

    async def close(self) -> None:
        await self._stream.finish()

    async def fail(self, exc: CastyError) -> None:
        await self._stream.send_error(_stream_code(exc), str(exc))
        await self._stream.finish()


def _stream_code(exc: Exception) -> int:
    if isinstance(exc, UnknownActorTypeError):
        return actor_messages.CODE_UNKNOWN_ACTOR_TYPE
    if isinstance(exc, ActorFailedError):
        return actor_messages.CODE_ACTOR_FAILED
    if isinstance(exc, ReentrancyError):
        return actor_messages.CODE_REENTRANCY
    if isinstance(exc, RangeMovingError):
        return actor_messages.CODE_RANGE_MOVING
    if isinstance(exc, QuorumUnavailableError):
        return actor_messages.CODE_QUORUM_UNAVAILABLE
    if isinstance(exc, ActorUnavailableError):
        return actor_messages.CODE_UNAVAILABLE
    if isinstance(exc, RemoteError):
        return exc.code
    return 0


@dataclass(frozen=True, slots=True)
class _Local:
    value: object


def _from_remote(exc: RemoteError, wire_name: str, key: str) -> CastyError:
    code = exc.code
    if code == actor_messages.CODE_UNKNOWN_ACTOR_TYPE:
        return UnknownActorTypeError(exc.remote_message)
    if code == actor_messages.CODE_ACTOR_FAILED:
        return ActorFailedError(wire_name, key, exc.remote_message)
    if code == actor_messages.CODE_REENTRANCY:
        return ReentrancyError(exc.remote_message)
    if code == actor_messages.CODE_UNAVAILABLE:
        return ActorUnavailableError(exc.remote_message)
    if code == actor_messages.CODE_RANGE_MOVING:
        return RangeMovingError(exc.remote_message)
    if code == actor_messages.CODE_QUORUM_UNAVAILABLE:
        return QuorumUnavailableError(exc.remote_message)
    return exc


_SPY_DONE = object()


class _ClusterSubscription(inspection.Subscription):
    """`scope="cluster"`: the subscription IS the open streams (spec 11 §7).
    One dial task per member merged into one bounded queue; the dial set
    follows the view via `_Router._rebuild_ring`. `Gap` is synthesized locally
    when a member's stream ends."""

    def __init__(self, router: _Router, selector: inspection.Selector) -> None:
        self._router = router
        self._selector = selector
        self._key = uuid.uuid4().hex  # one SpyAgent activation per subscription
        self._queue: asyncio.Queue[object] = asyncio.Queue(
            maxsize=router._config.spy_buffer_events
        )
        self._tasks: dict[uuid.UUID, asyncio.Task[None]] = {}
        self._started = False
        self._closed = False

    def _open(self) -> None:
        if self._started:
            return
        self._started = True
        self._router._spy_subs.add(self)
        self.sync(self._router._addrs)

    def sync(self, addrs: dict[uuid.UUID, str]) -> None:
        if self._closed:
            return
        for node_id, addr in addrs.items():
            if node_id not in self._tasks:
                self._tasks[node_id] = asyncio.create_task(self._dial(node_id, addr))
        for node_id in list(self._tasks):
            if node_id not in addrs:
                self._tasks.pop(node_id).cancel()
                self._gap(node_id, "left")

    async def __anext__(self) -> inspection.SpyEvent:
        self._open()
        if self._closed:
            raise StopAsyncIteration
        item = await self._queue.get()
        if item is _SPY_DONE:
            raise StopAsyncIteration
        return typing.cast(inspection.SpyEvent, item)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._router._spy_subs.discard(self)
        for task in self._tasks.values():
            task.cancel()
        for task in self._tasks.values():
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(_SPY_DONE)

    async def _dial(self, node_id: uuid.UUID, addr: str) -> None:
        local = self._router._spy_local(node_id, self._selector)
        if local is not None:
            # this member is us: a node cannot dial itself — pump the hub directly
            try:
                async for event in local:
                    await self._queue.put(event)
            finally:
                await local.aclose()
            return
        spy_info = actor_registry.info_by_name(inspection.SPY_AGENT_NAME)
        assert spy_info is not None
        elem = spy_info.methods["subscribe"].stream_out
        descriptor = codec.encode(
            actor_messages.StreamOpen(
                actor=inspection.SPY_AGENT_NAME,
                key=self._key,
                method="subscribe",
                args=[codec.encode_raw(self._selector)],
                chain=[],
            )
        )
        while True:
            try:
                async for raw in self._router._stream_at(addr, descriptor):
                    await self._queue.put(codec.decode_raw(raw, elem))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass  # accounted below via Gap; the pool owns reconnect backoff
            if self._closed or node_id not in self._router._addrs:
                return
            self._gap(node_id, "lost")
            await asyncio.sleep(self._router._config.transport.reconnect_base)

    def _gap(self, node_id: uuid.UUID, reason: str) -> None:
        gap = inspection.Gap(
            actor="", key="", node=node_id, seq=0, ts=datetime.now(UTC), reason=reason
        )
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(gap)


class Node(_Router):
    """A full cluster member: hosts actors for the token ranges it owns.

    Created by `start`, never directly. The `ActorSystem` surface (`actor`,
    `service`, the collection factories) is the API; everything else on the
    instance is cluster machinery.

    Attributes
    ----------
    node_id : uuid.UUID
        This node's identity on the ring, minted at start.
    """

    def __init__(
        self,
        *,
        local: LocalNode,
        bind_addr: str,
        config: Config,
        tls: TLS | None,
        supervisor: Supervisor | None,
        interceptor: inspection.Interceptor | None = None,
    ) -> None:
        super().__init__(config)
        self._local = local
        self._bind_addr = bind_addr
        self._tls = tls
        assert local.listen_addr is not None
        self.member = Member(node_id=local.node_id, addr=local.listen_addr)
        self.membership = Membership(self.member, config.membership)
        self._pool = Pool(
            local=local,
            config=config.transport,
            tls=tls,
            on_request=self._on_request,
            on_control=self.membership.handle_control,
            on_actor_stream=self._on_actor_stream,
            on_close=self.membership.handle_close,
        )
        self.membership.pool = self._pool
        self._replication = Replication(
            node_id=local.node_id,
            pool=self._pool,
            view=self,
            replication_timeout=config.replication_timeout,
            handoff_timeout=config.handoff_timeout,
            batch_bytes=config.replication_batch_bytes,
            max_message_bytes=config.transport.max_message_bytes,
        )
        self._hub = inspection.Hub(
            node=local.node_id,
            hub_events=config.spy_hub_events,
            buffer_events=config.spy_buffer_events,
        )
        self._host = ActorHost(
            router=self,
            replication=self._replication,
            supervisor=supervisor,
            default_idle_timeout=config.default_idle_timeout,
            reactivation_retry=config.reactivation_retry,
            hub=self._hub,
            interceptor=interceptor,
        )
        self._replication.on_active = self._on_active_arrival
        self._reactivation: asyncio.Queue[tuple[str, str, str]] = asyncio.Queue()
        self._reactivation_workers: list[asyncio.Task[None]] = []
        self._repokes: set[asyncio.Task[None]] = set()
        self._swept: frozenset[uuid.UUID] | None = None
        self._server: Server | None = None
        self.membership.subscribe(self._on_view_event)

    @property
    def node_id(self) -> uuid.UUID:
        return self._local.node_id

    async def _start(self, seeds: Sequence[str]) -> None:
        host, _, port = self._bind_addr.rpartition(":")
        self._server = await Server.start(
            host,
            int(port),
            local=self._local,
            config=self._config.transport,
            tls=self._tls,
            on_connection=self.membership.handle_connection,
            on_request=self._on_request,
            on_control=self.membership.handle_control,
            on_actor_stream=self._on_actor_stream,
            on_close=self.membership.handle_close,
        )
        self.membership.start()
        others = [s for s in seeds if s != self._local.listen_addr]
        if others:
            await self.membership.join(others)
        self._rebuild_ring(self.membership.alive_members())
        self._reactivation_workers = [
            asyncio.create_task(self._reactivation_worker())
            for _ in range(self._config.reactivation_concurrency)
        ]
        self._maybe_sweep()

    async def close(self) -> None:
        """Leave the cluster gracefully.

        Rejects new work, drains in-flight handlers, runs deactivate hooks,
        hands token ranges off, broadcasts a clean leave and closes every
        connection. Calls after this raise `ActorUnavailableError`.
        """
        self._stopping = True
        for sub in list(self._spy_subs):
            await sub.aclose()
        for task in [*self._reactivation_workers, *self._repokes]:
            task.cancel()
        for task in [*self._reactivation_workers, *self._repokes]:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self._host.stop(self._config.drain_timeout)
        self._hub.close()
        await self._replication.handoff()
        await self.membership.leave()
        if self._server is not None:
            self._server.stop_accepting()
        await self._pool.close()
        if self._server is not None:
            await self._server.close()

    # --- routing ---------------------------------------------------------------

    async def _call_service(
        self, info: ActorInfo, key: str, method: MethodInfo, args: list[object], chain: list[str]
    ) -> object:
        """Local-first: the coordinator handler is O(1) and the real work runs as
        a task on this node, so there is nothing to gain from a hop."""
        return await self._host.dispatch(info, key, method.name, args, chain)

    def _service_local_host(self, node_id: uuid.UUID) -> ActorHost | None:
        return self._host if node_id == self.node_id else None

    async def _dispatch_local(
        self,
        owner: uuid.UUID,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        args: list[object],
        chain: list[str],
    ) -> _Local | None:
        if owner != self.node_id:
            return None
        result = await self._host.dispatch(info, key, method.name, args, chain)
        return _Local(result)

    def _local_stream(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[object] | None:
        ring = self._ring
        if ring is None or ring.owner(f"{info.wire_name}/{key}") != self.node_id:
            return None  # not the owner: go over the wire
        if method.stream_out is not None:
            return local_stream_out(self._host, info, key, method, kwargs, in_iter, chain)
        return _one(local_stream_in(self._host, info, key, method, kwargs, in_iter, chain))

    def spy(
        self,
        actor: type | str | None = None,
        *,
        key: str = "*",
        scope: typing.Literal["local", "cluster"] = "local",
        payloads: bool = False,
    ) -> inspection.Subscription:
        selector = inspection.selector_of(actor, key, payloads)
        if scope == "local":
            return inspection.LocalSubscription(self._hub, selector)
        return _ClusterSubscription(self, selector)

    def _state_local(self, owner: uuid.UUID, info: ActorInfo, key: str) -> _Local | None:
        if owner != self.node_id:
            return None
        return _Local(self._host.state_values(info.wire_name, key))

    def _spy_local(
        self, node_id: uuid.UUID, selector: inspection.Selector
    ) -> inspection.Subscription | None:
        if node_id != self.node_id:
            return None
        return inspection.LocalSubscription(self._hub, selector)

    def _on_view_event(self, event: ViewEvent) -> None:
        self._rebuild_ring(self.membership.alive_members())
        self._maybe_sweep()

    # --- failover reactivation --------------------------------------------------------

    def _maybe_sweep(self) -> None:
        """Run the scans only when the ring *content* changed — view events
        fire on gossip far more often than membership actually moves."""
        nodes = self._ring.nodes if self._ring is not None else frozenset()
        if nodes == self._swept:
            return
        self._swept = nodes
        self._sweep()

    def _sweep(self) -> None:
        ring = self._ring
        if ring is None:
            return
        marked: list[reactivation.Marked] = []
        for actor, key, stored in self._replication.store.items():
            if reactivation.ACTIVE_PAGE not in stored.pages:
                continue
            info = actor_registry.info_by_name(actor) or ensure_collection(actor)
            if info is None or not info.durable_activity:
                continue
            marked.append(
                reactivation.Marked(
                    wire_name=actor,
                    key=key,
                    owner=ring.owner(f"{actor}/{key}"),
                    replicas=self._replication.replica_set(info, key),
                )
            )
        placed = [
            (wire, key, ring.owner(f"{wire}/{key}"))
            for wire, key in self._host.placed_activations()
        ]
        decision = reactivation.sweep(
            marked=marked,
            placed=placed,
            active={(wire, key) for wire, key, _ in placed},
            pending_clears=self._host.pending_clears,
            node_id=self.node_id,
        )
        for wire, key in decision.discards:
            self._host.discard(wire, key)
        for wire, key in decision.pokes:
            self._reactivation.put_nowait(("poke", wire, key))
        for wire, key in decision.handoffs:
            self._reactivation.put_nowait(("push", wire, key))

    def _poke_needed(self, wire: str, key: str) -> bool:
        ring = self._ring
        if ring is None or ring.owner(f"{wire}/{key}") != self.node_id:
            return False
        if self._host.is_active(wire, key) or (wire, key) in self._host.pending_clears:
            return False
        stored = self._replication.store.get(wire, key)
        return stored is not None and reactivation.ACTIVE_PAGE in stored.pages

    def _on_active_arrival(self, wire: str, key: str) -> None:
        if not self._stopping and self._poke_needed(wire, key):
            self._reactivation.put_nowait(("poke", wire, key))

    async def _reactivation_worker(self) -> None:
        while True:
            action, wire, key = await self._reactivation.get()
            info = actor_registry.info_by_name(wire) or ensure_collection(wire)
            if info is None:
                continue
            if action == "push":
                await self._replication.push_marked(info, key)
                continue
            if not self._poke_needed(wire, key):
                continue
            self._host.ensure_active(info, key)
            task = asyncio.create_task(self._repoke(wire, key))
            self._repokes.add(task)
            task.add_done_callback(self._repokes.discard)

    async def _repoke(self, wire: str, key: str) -> None:
        """A poke whose activation failed (quorum, dial, poison hook) leaves no
        live activation behind; as long as this node still owns the mark, try
        again."""
        await asyncio.sleep(self._config.reactivation_retry)
        if self._poke_needed(wire, key):
            self._reactivation.put_nowait(("poke", wire, key))

    # --- inbound actor calls --------------------------------------------------------

    async def _on_request(self, conn: Connection, msg_type: int, body: bytes) -> bytes:
        match msg_type:
            case actor_replication.REPLICATE:
                replicate = codec.decode(body)
                if not isinstance(replicate, actor_replication.Replicate):
                    raise RemoteError(0, "malformed replicate")
                self._replication.handle_replicate(replicate)
                return b""
            case actor_replication.FETCH_STATE:
                fetch = codec.decode(body)
                if not isinstance(fetch, actor_replication.FetchState):
                    raise RemoteError(0, "malformed fetch")
                return codec.encode(self._replication.handle_fetch(fetch))
            case actor_replication.FETCH_PAGES:
                pages = codec.decode(body)
                if not isinstance(pages, actor_replication.FetchPages):
                    raise RemoteError(0, "malformed page fetch")
                return codec.encode(self._replication.handle_fetch_pages(pages))
            case actor_messages.ACTOR_CALL:
                return await self._on_actor_call(body)
            case _:
                raise RemoteError(0, f"unsupported msg_type 0x{msg_type:x}")

    async def _on_actor_call(self, body: bytes) -> bytes:
        call = codec.decode(body)
        if not isinstance(call, actor_messages.ActorCall):
            raise RemoteError(0, "malformed actor call")
        if self._stopping:
            raise RemoteError(actor_messages.CODE_UNAVAILABLE, "node is stopping")
        info = actor_registry.info_by_name(call.actor) or ensure_collection(call.actor)
        if info is None:
            raise RemoteError(
                actor_messages.CODE_UNKNOWN_ACTOR_TYPE,
                f"actor type {call.actor!r} is not registered on this node",
            )
        ring = self._ring
        # a service is unplaced: any node serves it, so there is no wrong owner
        if info.kind == "actor" and (
            ring is None or ring.owner(f"{call.actor}/{call.key}") != self.node_id
        ):
            raise RemoteError(
                actor_messages.CODE_WRONG_OWNER,
                f"{call.actor}/{call.key} is not owned by this node",
            )
        method = info.methods.get(call.method)
        if method is None:
            raise RemoteError(0, f"{call.actor} has no method {call.method!r}")
        if len(call.args) != len(method.params):
            raise RemoteError(
                0, f"{call.actor}.{call.method} expects {len(method.params)} args"
            )
        args = [
            codec.decode_raw(raw, annotation, path=f"{call.actor}.{call.method}({name})")
            for raw, (name, annotation) in zip(call.args, method.params, strict=True)
        ]
        try:
            result = await self._host.dispatch(info, call.key, call.method, args, call.chain)
        except ActorFailedError as exc:
            raise RemoteError(actor_messages.CODE_ACTOR_FAILED, exc.remote_message) from exc
        except ReentrancyError as exc:
            raise RemoteError(actor_messages.CODE_REENTRANCY, str(exc)) from exc
        except ActorUnavailableError as exc:
            raise RemoteError(actor_messages.CODE_UNAVAILABLE, str(exc)) from exc
        except RangeMovingError as exc:
            raise RemoteError(actor_messages.CODE_RANGE_MOVING, str(exc)) from exc
        except QuorumUnavailableError as exc:
            raise RemoteError(actor_messages.CODE_QUORUM_UNAVAILABLE, str(exc)) from exc
        return codec.encode_raw(result)

    async def _on_actor_stream(
        self, conn: Connection, stream: ActorStream, descriptor: bytes
    ) -> None:
        """Owner side of a streaming call (spec 07). Validation errors are sent as
        STREAM_ERROR; once dispatched, the handler's own outcome flows through the
        `_WireSink` (which closes or fails the stream)."""
        try:
            await self._drive_actor_stream(stream, descriptor)
        except RemoteError as exc:
            with contextlib.suppress(Exception):
                await stream.send_error(exc.code, exc.remote_message)
                await stream.finish()
        except Exception as exc:  # malformed descriptor, decode failure, etc.
            with contextlib.suppress(Exception):
                await stream.send_error(0, str(exc))
                await stream.finish()

    async def _drive_actor_stream(self, stream: ActorStream, descriptor: bytes) -> None:
        open_ = codec.decode(descriptor)
        if not isinstance(open_, actor_messages.StreamOpen):
            raise RemoteError(0, "malformed stream open")
        if self._stopping:
            raise RemoteError(actor_messages.CODE_UNAVAILABLE, "node is stopping")
        info = actor_registry.info_by_name(open_.actor) or ensure_collection(open_.actor)
        if info is None:
            raise RemoteError(
                actor_messages.CODE_UNKNOWN_ACTOR_TYPE,
                f"actor type {open_.actor!r} is not registered on this node",
            )
        ring = self._ring
        # a service is unplaced: any node serves it, so there is no wrong owner
        # (spec 11 §6 — this is what lets SpyAgent.subscribe be node-directed)
        if info.kind == "actor" and (
            ring is None or ring.owner(f"{open_.actor}/{open_.key}") != self.node_id
        ):
            raise RemoteError(
                actor_messages.CODE_WRONG_OWNER,
                f"{open_.actor}/{open_.key} is not owned by this node",
            )
        method = info.methods.get(open_.method)
        if method is None:
            raise RemoteError(0, f"{open_.actor} has no method {open_.method!r}")
        if not method.is_streaming:
            raise RemoteError(0, f"{open_.actor}.{open_.method} is not a streaming method")
        if len(open_.args) != len(method.params):
            raise RemoteError(
                0, f"{open_.actor}.{open_.method} expects {len(method.params)} unary args"
            )
        kwargs = {
            name: codec.decode_raw(raw, annotation, path=f"{open_.actor}.{open_.method}({name})")
            for raw, (name, annotation) in zip(open_.args, method.params, strict=True)
        }
        in_iter = (
            _stream_input(stream, method.stream_in[1]) if method.stream_in is not None else None
        )
        await self._host.dispatch_stream(
            info, open_.key, method, kwargs, in_iter, _WireSink(stream), open_.chain
        )


class Client(_Router):
    """Lite member: knows the ring and routes in one hop, hosts nothing and is
    invisible to placement.

    Created by `connect`, never directly. Keeps its member table fresh by
    pulling SYNC from a known member every `Config.client_sync_interval`
    seconds.
    """

    def __init__(
        self,
        *,
        local: LocalNode,
        config: Config,
        tls: TLS | None,
        address_map: Callable[[str], str] | None = None,
    ) -> None:
        super().__init__(config)
        self._local = local
        self._pool = Pool(
            local=local,
            config=config.transport,
            tls=tls,
            on_control=self._on_control,
            address_map=address_map,
        )
        self._seeds: list[str] = []
        self._synced = asyncio.Event()
        self._sync_task: asyncio.Task[None] | None = None

    async def _start(self, seeds: Sequence[str]) -> None:
        self._seeds = list(seeds)
        await self._sync_once()
        self._sync_task = asyncio.create_task(self._sync_loop())

    def spy(
        self,
        actor: type | str | None = None,
        *,
        key: str = "*",
        scope: typing.Literal["local", "cluster"] = "local",
        payloads: bool = False,
    ) -> inspection.Subscription:
        if scope == "local":
            raise ValueError(
                "a client hosts nothing; its spy is always scope='cluster'"
            )
        return _ClusterSubscription(self, inspection.selector_of(actor, key, payloads))

    async def close(self) -> None:
        """Stop syncing and close every connection."""
        self._stopping = True
        for sub in list(self._spy_subs):
            await sub.aclose()
        if self._sync_task is not None:
            self._sync_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sync_task
        await self._pool.close()

    async def _refresh_view(self) -> None:
        with contextlib.suppress(CastyError, OSError):
            await self._sync_once()

    async def _sync_once(self) -> None:
        last_error: Exception | None = None
        for addr in [*self._addrs.values(), *self._seeds]:
            try:
                conn = await self._pool.get(addr)
                self._synced.clear()
                await conn.send_control(
                    membership_messages.SYNC,
                    codec.encode(membership_messages.Sync(records=[])),
                )
                await asyncio.wait_for(self._synced.wait(), self._config.call_timeout)
                return
            except (CastyError, OSError, TimeoutError) as exc:
                last_error = exc
        raise last_error if last_error is not None else ActorUnavailableError("no seeds")

    async def _sync_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.client_sync_interval)
            with contextlib.suppress(CastyError, OSError, TimeoutError):
                await self._sync_once()

    async def _on_control(self, conn: Connection, msg_type: int, body: bytes) -> None:
        decoded = codec.decode(body)
        if not isinstance(decoded, membership_messages.SyncReply):
            return
        members = frozenset(
            Member(node_id=r.member.node_id, addr=r.member.addr, role=r.member.role)
            for r in decoded.records
            if Status(r.status) in (Status.ALIVE, Status.SUSPECT)
        )
        self._rebuild_ring(members)
        self._synced.set()


class Managed[T: ActorSystem]:
    """Result of `start`/`connect`: awaitable *and* an async context manager.

    `node = await start(...)` yields the started system; `async with start(...)
    as node:` starts it on entry and closes it on exit.

    Examples
    --------
    >>> async with casty.start("127.0.0.1:7000") as node:
    ...     counter = node.actor(Counter, "page-views")
    """

    def __init__(self, coro: Coroutine[object, object, T]) -> None:
        self._coro = coro
        self._value: T | None = None

    def __await__(self) -> Generator[object, object, T]:
        return self._coro.__await__()

    async def __aenter__(self) -> T:
        self._value = await self._coro
        return self._value

    async def __aexit__(self, *exc: object) -> None:
        assert self._value is not None
        await self._value.close()


def start(
    listen: str,
    *,
    seeds: Sequence[str] = (),
    tls: TLS | None = None,
    config: Config | None = None,
    supervisor: Supervisor | None = None,
    interceptor: inspection.Interceptor | None = None,
    cluster_name: str = "casty",
    advertise: str | None = None,
) -> Managed[Node]:
    """Start a full cluster member.

    Parameters
    ----------
    listen : str
        `host:port` to bind the listener on.
    seeds : Sequence[str]
        Addresses of existing members to join through. Empty starts a new
        single-node cluster.
    tls : TLS | None
        Cluster-wide TLS material; None means plaintext everywhere.
    config : Config | None
        Every protocol knob; defaults are production-oriented.
    supervisor : Supervisor | None
        Global failure policy for actors (overridable per class).
    interceptor : Interceptor | None
        Sync per-node hook called inline with every spy event this node's
        actors emit (spec 11). Contract: return fast, never block, never
        raise — raises are logged, not propagated.
    cluster_name : str
        Handshake guard: nodes with different names refuse each other.
    advertise : str | None
        Address other nodes dial. Defaults to `listen`; required when
        listening on 0.0.0.0.

    Returns
    -------
    Managed[Node]
        Await it for the started node, or use it as an async context manager
        to close on exit.

    Examples
    --------
    >>> node = await casty.start("127.0.0.1:7000")
    >>> peer = await casty.start("127.0.0.1:7001", seeds=["127.0.0.1:7000"])
    """
    async def _run() -> Node:
        cfg = config or Config()
        local = LocalNode(
            node_id=uuid.uuid4(),
            cluster_name=cluster_name,
            listen_addr=advertise or listen,
            role="member",
        )
        node = Node(
            local=local,
            bind_addr=listen,
            config=cfg,
            tls=tls,
            supervisor=supervisor,
            interceptor=interceptor,
        )
        await node._start(seeds)
        return node

    return Managed(_run())


def connect(
    seeds: Sequence[str],
    *,
    tls: TLS | None = None,
    config: Config | None = None,
    cluster_name: str = "casty",
    address_map: Callable[[str], str] | None = None,
) -> Managed[Client]:
    """Connect as a lite member: routes actor calls, hosts nothing.

    Parameters
    ----------
    seeds : Sequence[str]
        Addresses of cluster members to pull the initial view from; at least
        one must be reachable.
    tls : TLS | None
        Cluster-wide TLS material; None means plaintext everywhere.
    config : Config | None
        Every protocol knob; defaults are production-oriented.
    cluster_name : str
        Handshake guard: must match the cluster's.
    address_map : Callable[[str], str] | None
        Rewrites every announced `host:port` into the address this client
        actually dials — members behind an SSH tunnel or NAT announce their
        private address, and the map turns it into the local tunnel endpoint.
        Applies to every outbound dial, seeds included; identity when None.

    Returns
    -------
    Managed[Client]
        Await it for the connected client, or use it as an async context
        manager to close on exit.
    """

    async def _run() -> Client:
        cfg = config or Config()
        local = LocalNode(
            node_id=uuid.uuid4(), cluster_name=cluster_name, listen_addr=None, role="client"
        )
        client = Client(local=local, config=cfg, tls=tls, address_map=address_map)
        await client._start(seeds)
        return client

    return Managed(_run())
