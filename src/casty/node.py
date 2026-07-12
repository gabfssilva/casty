from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator, Coroutine, Generator, Sequence
from dataclasses import dataclass, field

from casty.actors import messages as actor_messages
from casty.actors import registry as actor_registry
from casty.actors import replication as actor_replication
from casty.actors.host import ActorHost
from casty.actors.registry import ActorInfo, MethodInfo, Supervisor
from casty.actors.replication import Replication
from casty.actors.streaming import local_stream_in, local_stream_out
from casty.collections import ensure as ensure_collection
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
from casty.system import ActorSystem
from casty.transport.connection import ActorStream, Connection, LocalNode
from casty.transport.pool import Pool
from casty.transport.server import Server

logger = logging.getLogger("casty.node")


@dataclass(frozen=True)
class Config:
    transport: TransportConfig = field(default_factory=TransportConfig)
    membership: MembershipConfig = field(default_factory=MembershipConfig)
    call_timeout: float = 10.0
    default_idle_timeout: float = 300.0
    drain_timeout: float = 10.0
    client_sync_interval: float = 5.0
    replication_timeout: float = 5.0
    handoff_timeout: float = 10.0
    replication_batch_bytes: int = 256 * 1024  # pages per replication message (spec 09)


class _Router(ActorSystem):
    """Shared routing: ring lookup, remote ask, WRONG_OWNER retry. Node adds the
    local-dispatch path; Client is remote-only."""

    _pool: Pool  # assigned by subclass __init__

    def __init__(self, config: Config) -> None:
        self._config = config
        self._ring: Ring | None = None
        self._addrs: dict[uuid.UUID, str] = {}
        self._stopping = False
        self._next_member = 0  # round-robin cursor for service calls

    @property
    def ring(self) -> Ring | None:
        return self._ring

    def addr_of(self, node_id: uuid.UUID) -> str | None:
        return self._addrs.get(node_id)

    def _rebuild_ring(self, members: frozenset[Member]) -> None:
        placed = [m for m in members if m.role == "member"]
        self._addrs = {m.node_id: m.addr for m in placed}
        self._ring = Ring.build([m.node_id for m in placed]) if placed else None

    async def call_actor(
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

    # --- streaming (spec 07) --------------------------------------------------------

    def stream_out(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[object]:
        return self._route_stream(info, key, method, kwargs, in_iter, chain, method.stream_out)

    async def stream_in(
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


class Node(_Router):
    """A full cluster member: hosts actors for the token ranges it owns."""

    def __init__(
        self,
        *,
        local: LocalNode,
        bind_addr: str,
        config: Config,
        tls: TLS | None,
        supervisor: Supervisor | None,
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
        self._host = ActorHost(
            router=self,
            replication=self._replication,
            supervisor=supervisor,
            default_idle_timeout=config.default_idle_timeout,
        )
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

    async def close(self) -> None:
        """Graceful: reject new work, drain in-flight handlers, run deactivate
        hooks, hand ranges off, broadcast a clean leave, close connections."""
        self._stopping = True
        await self._host.stop(self._config.drain_timeout)
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

    def _on_view_event(self, event: ViewEvent) -> None:
        self._rebuild_ring(self.membership.alive_members())

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
        if ring is None or ring.owner(f"{open_.actor}/{open_.key}") != self.node_id:
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
    invisible to placement. Keeps its member table fresh by pulling SYNC from a
    known member."""

    def __init__(self, *, local: LocalNode, config: Config, tls: TLS | None) -> None:
        super().__init__(config)
        self._local = local
        self._pool = Pool(
            local=local,
            config=config.transport,
            tls=tls,
            on_control=self._on_control,
        )
        self._seeds: list[str] = []
        self._synced = asyncio.Event()
        self._sync_task: asyncio.Task[None] | None = None

    async def _start(self, seeds: Sequence[str]) -> None:
        self._seeds = list(seeds)
        await self._sync_once()
        self._sync_task = asyncio.create_task(self._sync_loop())

    async def close(self) -> None:
        self._stopping = True
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
    cluster_name : str
        Handshake guard: nodes with different names refuse each other.
    advertise : str | None
        Address other nodes dial. Defaults to `listen`; required when
        listening on 0.0.0.0.
    """
    async def _run() -> Node:
        cfg = config or Config()
        local = LocalNode(
            node_id=uuid.uuid4(),
            cluster_name=cluster_name,
            listen_addr=advertise or listen,
            role="member",
        )
        node = Node(local=local, bind_addr=listen, config=cfg, tls=tls, supervisor=supervisor)
        await node._start(seeds)
        return node

    return Managed(_run())


def connect(
    seeds: Sequence[str],
    *,
    tls: TLS | None = None,
    config: Config | None = None,
    cluster_name: str = "casty",
) -> Managed[Client]:
    """Connect as a lite member: routes actor calls, hosts nothing."""

    async def _run() -> Client:
        cfg = config or Config()
        local = LocalNode(
            node_id=uuid.uuid4(), cluster_name=cluster_name, listen_addr=None, role="client"
        )
        client = Client(local=local, config=cfg, tls=tls)
        await client._start(seeds)
        return client

    return Managed(_run())
