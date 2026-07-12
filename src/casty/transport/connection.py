from __future__ import annotations

import asyncio
import contextlib
import ssl
import struct
import typing
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass

import msgpack

from casty.config import TransportConfig
from casty.errors import (
    CastyError,
    CastyTimeoutError,
    ConnectionLostError,
    HandshakeError,
    ProtocolError,
    RemoteError,
    StreamResetError,
)
from casty.transport import compression, envelope, mux

CONTROL_CHANNEL = 1
MESSAGE_CHANNEL = 3

# A stream opened with this BULK_OPEN kind is not a bulk byte stream: it is a
# second envelope channel, reserved for replication traffic so state fan-out
# never queues behind actor calls (nor the reverse). Each side opens its own
# lane on first use; replies return on the stream the request arrived on.
REPLICATION_KIND = "casty.replication"

# A bulk stream opened with this kind is a per-call bidirectional actor stream
# (spec 07): after the BULK_OPEN header it carries STREAM_ITEM / STREAM_ERROR
# envelopes, one FIN per direction marking end-of-input and end-of-output.
ACTOR_STREAM_KIND = "casty.stream"

PROTOCOL_VERSIONS = [1]

REJECT_WRONG_CLUSTER = 1
REJECT_INCOMPATIBLE_VERSION = 2
REJECT_SELF_CONNECT = 3

_READ_CHUNK = 64 * 1024


@dataclass(frozen=True, slots=True)
class LocalNode:
    node_id: uuid.UUID
    cluster_name: str
    listen_addr: str | None = None
    role: str = "member"


@dataclass(frozen=True, slots=True)
class PeerInfo:
    node_id: uuid.UUID
    listen_addr: str | None
    role: str


type RequestHandler = Callable[["Connection", int, bytes], Awaitable[bytes]]
type ControlHandler = Callable[["Connection", int, bytes], Awaitable[None]]
type BulkHandler = Callable[["Connection", "BulkReceiver"], Awaitable[None]]
type ActorStreamHandler = Callable[["Connection", "ActorStream", bytes], Awaitable[None]]
type CloseHandler = Callable[["Connection", Exception | None], None]


class BulkReceiver:
    """Incoming bulk stream. Iterate to read chunks; the peer's send window is
    only replenished as chunks are consumed, so a slow reader throttles the
    sender end-to-end."""

    def __init__(self, conn: Connection, stream_id: int, kind: str, meta: dict[str, object]):
        self._conn = conn
        self._stream_id = stream_id
        self.kind = kind
        self.meta = meta
        self._queue: asyncio.Queue[bytes | Exception | None] = asyncio.Queue()  # None = EOF

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        item = await self._queue.get()
        if item is None:
            raise StopAsyncIteration
        if isinstance(item, Exception):
            raise item
        await self._conn._replenish(self._stream_id, len(item))
        return item


class BulkSender:
    def __init__(self, conn: Connection, stream_id: int):
        self._conn = conn
        self._stream_id = stream_id
        self._closed = False

    async def send(self, data: bytes) -> None:
        if self._closed:
            raise StreamResetError(self._stream_id, "bulk sender already closed")
        await self._conn._send_stream(self._stream_id, data)

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await self._conn._finish_stream(self._stream_id)

    async def abort(self) -> None:
        if not self._closed:
            self._closed = True
            await self._conn._reset_stream(self._stream_id)


class ActorStream:
    """One side of a per-call bidirectional actor stream (spec 07). Both ends use
    the same object: `send_item`/`send_error`/`finish` drive the local send half;
    iterating yields the peer's item payloads and raises on a STREAM_ERROR, on the
    peer resetting, or on connection loss. Flow-control credit is only returned as
    items are consumed, so a slow reader throttles the sender end-to-end."""

    def __init__(self, conn: Connection, stream_id: int) -> None:
        self._conn = conn
        self._stream_id = stream_id
        self._decoder = envelope.Decoder(max_message_bytes=conn._config.max_message_bytes)
        self._inbound: asyncio.Queue[envelope.Envelope | Exception | None] = asyncio.Queue()
        self._send_finished = False
        self._recv_finished = False

    async def send_item(self, payload: bytes) -> None:
        await self._conn._send_stream(
            self._stream_id, envelope.encode(envelope.MsgType.STREAM_ITEM, 0, payload)
        )

    async def send_error(self, code: int, message: str) -> None:
        body = msgpack.packb({"code": code, "message": message})
        await self._conn._send_stream(
            self._stream_id, envelope.encode(envelope.MsgType.STREAM_ERROR, 0, body)
        )

    async def finish(self) -> None:
        """Half-close the local send direction (end-of-input or end-of-output)."""
        if not self._send_finished:
            self._send_finished = True
            await self._conn._finish_stream(self._stream_id)
            if self._recv_finished:
                self._conn._actor_streams.pop(self._stream_id, None)

    async def reset(self) -> None:
        await self._conn._reset_stream(self._stream_id)

    def _feed(self, data: bytes) -> None:
        for received in self._decoder.feed(data):
            self._inbound.put_nowait(received)

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        item = await self._inbound.get()
        if item is None:
            raise StopAsyncIteration
        if isinstance(item, Exception):
            raise item
        await self._conn._replenish(self._stream_id, envelope.wire_size(len(item.body)))
        if item.msg_type == envelope.MsgType.STREAM_ERROR:
            raise _decode_error(item.body)
        if item.msg_type != envelope.MsgType.STREAM_ITEM:
            raise ProtocolError(f"unexpected 0x{item.msg_type:x} on actor stream {self._stream_id}")
        return item.body


class Connection:
    """One multiplexed casty connection. Create via `Connection.connect`
    (initiator) or `Connection.accept` (listener side); both return only after
    the handshake completed."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        initiator: bool,
        local: LocalNode,
        config: TransportConfig | None = None,
        on_request: RequestHandler | None = None,
        on_control: ControlHandler | None = None,
        on_bulk: BulkHandler | None = None,
        on_actor_stream: ActorStreamHandler | None = None,
        on_close: CloseHandler | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._initiator = initiator
        self._local = local
        self._config = config or TransportConfig()
        self._on_request = on_request
        self._on_control = on_control
        self._on_bulk = on_bulk
        self._on_actor_stream = on_actor_stream
        self._on_close = on_close
        self._mux = mux.Mux(initiator=initiator, config=self._config)
        self._write_lock = asyncio.Lock()
        self._flush_task: asyncio.Task[None] | None = None
        self._send_locks: dict[int, asyncio.Lock] = {}
        self._window_waiters: dict[int, asyncio.Event] = {}
        self._pending: dict[int, asyncio.Future[envelope.Envelope]] = {}
        self._next_correlation = 1
        self._control_decoder = envelope.Decoder(max_message_bytes=self._config.max_message_bytes)
        self._message_decoder = envelope.Decoder(max_message_bytes=self._config.max_message_bytes)
        self._bulk_decoders: dict[int, envelope.Decoder] = {}
        self._bulk_receivers: dict[int, BulkReceiver] = {}
        self._actor_streams: dict[int, ActorStream] = {}
        self._envelope_channels: dict[int, envelope.Decoder] = {}  # peer replication lanes
        self._repl_stream: int | None = None  # our outbound replication lane
        self._repl_open_lock = asyncio.Lock()
        self._hello_future: asyncio.Future[envelope.Envelope] = (
            asyncio.get_running_loop().create_future()
        )
        self._pong_waiters: dict[bytes, asyncio.Future[None]] = {}
        self._ping_counter = 0
        self._handshaken = False
        self._message_channel_open = asyncio.Event()
        self._closed: Exception | None = None
        self._close_event = asyncio.Event()
        self._last_recv = asyncio.get_running_loop().time()
        self._read_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self.peer: PeerInfo | None = None
        self.compression: str | None = None

    # --- factories -------------------------------------------------------------

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        local: LocalNode,
        config: TransportConfig | None = None,
        ssl_context: ssl.SSLContext | None = None,
        on_request: RequestHandler | None = None,
        on_control: ControlHandler | None = None,
        on_bulk: BulkHandler | None = None,
        on_actor_stream: ActorStreamHandler | None = None,
        on_close: CloseHandler | None = None,
    ) -> Connection:
        cfg = config or TransportConfig()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=ssl_context),
                timeout=cfg.connect_timeout,
            )
        except TimeoutError as exc:
            raise CastyTimeoutError(f"connecting to {host}:{port}") from exc
        conn = cls(
            reader,
            writer,
            initiator=True,
            local=local,
            config=cfg,
            on_request=on_request,
            on_control=on_control,
            on_bulk=on_bulk,
            on_actor_stream=on_actor_stream,
            on_close=on_close,
        )
        conn._start()
        try:
            await conn._handshake_initiate()
        except BaseException:
            await conn.close()
            raise
        return conn

    @classmethod
    async def accept(
        cls,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        local: LocalNode,
        config: TransportConfig | None = None,
        on_request: RequestHandler | None = None,
        on_control: ControlHandler | None = None,
        on_bulk: BulkHandler | None = None,
        on_actor_stream: ActorStreamHandler | None = None,
        on_close: CloseHandler | None = None,
    ) -> Connection:
        conn = cls(
            reader,
            writer,
            initiator=False,
            local=local,
            config=config,
            on_request=on_request,
            on_control=on_control,
            on_bulk=on_bulk,
            on_actor_stream=on_actor_stream,
            on_close=on_close,
        )
        conn._start()
        try:
            await conn._handshake_accept()
        except BaseException:
            await conn.close()
            raise
        return conn

    # --- public API --------------------------------------------------------------

    async def ask(
        self, msg_type: int, body: bytes, *, timeout: float | None = None, replication: bool = False
    ) -> bytes:
        self._ensure_open()
        stream_id = await self._replication_lane() if replication else MESSAGE_CHANNEL
        correlation_id = self._next_correlation
        self._next_correlation += 1
        future: asyncio.Future[envelope.Envelope] = asyncio.get_running_loop().create_future()
        self._pending[correlation_id] = future
        try:
            await self._send_envelope(stream_id, msg_type, correlation_id, body)
            reply = await asyncio.wait_for(future, timeout or self._config.request_timeout)
        except TimeoutError as exc:
            raise CastyTimeoutError(f"ask (msg_type=0x{msg_type:x}) timed out") from exc
        finally:
            self._pending.pop(correlation_id, None)
        if reply.msg_type == envelope.MsgType.ERROR:
            raise _decode_error(reply.body)
        return reply.body

    async def tell(self, msg_type: int, body: bytes) -> None:
        self._ensure_open()
        await self._send_envelope(MESSAGE_CHANNEL, msg_type, 0, body)

    async def send_control(self, msg_type: int, body: bytes) -> None:
        """Fire-and-forget on the control channel (membership traffic)."""
        self._ensure_open()
        await self._send_envelope(CONTROL_CHANNEL, msg_type, 0, body)

    async def open_bulk(self, kind: str, meta: dict[str, object] | None = None) -> BulkSender:
        self._ensure_open()
        stream_id = self._mux.open_stream()
        header = envelope.encode(
            envelope.MsgType.BULK_OPEN, 0, msgpack.packb({"kind": kind, "meta": meta or {}})
        )
        await self._send_stream(stream_id, header)
        return BulkSender(self, stream_id)

    async def open_actor_stream(self, descriptor: bytes) -> ActorStream:
        """Open a per-call actor stream (spec 07) and return its handle. The
        descriptor (the packed StreamOpen) rides in the BULK_OPEN meta so the
        owner can route before any item flows."""
        self._ensure_open()
        await self._message_channel_open.wait()  # handshake settled
        stream_id = self._mux.open_stream()
        stream = ActorStream(self, stream_id)
        self._actor_streams[stream_id] = stream
        header = envelope.encode(
            envelope.MsgType.BULK_OPEN,
            0,
            msgpack.packb({"kind": ACTOR_STREAM_KIND, "descriptor": descriptor}),
        )
        await self._send_stream(stream_id, header)
        return stream

    async def _replication_lane(self) -> int:
        """Our outbound replication stream, opened lazily on the first
        replication ask (either side of the connection may need one)."""
        if self._repl_stream is not None:
            return self._repl_stream
        async with self._repl_open_lock:
            if self._repl_stream is None:
                await self._message_channel_open.wait()  # handshake settled
                stream_id = self._mux.open_stream()
                header = envelope.encode(
                    envelope.MsgType.BULK_OPEN,
                    0,
                    msgpack.packb({"kind": REPLICATION_KIND, "meta": {}}),
                )
                await self._send_stream(stream_id, header)
                # streams are bidirectional: replies to our asks come back on
                # this same stream, so it needs an inbound envelope decoder too
                self._envelope_channels[stream_id] = envelope.Decoder(
                    max_message_bytes=self._config.max_message_bytes
                )
                self._repl_stream = stream_id
            return self._repl_stream

    async def ping(self) -> float:
        """Round-trip a mux-level PING; returns latency in seconds."""
        self._ensure_open()
        self._ping_counter += 1
        opaque = struct.pack("!Q", self._ping_counter)
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._pong_waiters[opaque] = future
        started = asyncio.get_running_loop().time()
        try:
            self._mux.ping(opaque)
            self._schedule_flush()
            await asyncio.wait_for(future, self._config.keepalive_timeout)
        except TimeoutError as exc:
            raise CastyTimeoutError("ping timed out") from exc
        finally:
            self._pong_waiters.pop(opaque, None)
        return asyncio.get_running_loop().time() - started

    async def close(self, *, code: int = 0, reason: str = "closing") -> None:
        if self._closed is None:
            # best-effort GO_AWAY: the peer may already be gone
            with contextlib.suppress(CastyError, OSError):
                self._mux.go_away(msgpack.packb({"code": code, "reason": reason}))
                await self._flush()
        self._fail(None)
        await self._shutdown_io()

    async def wait_closed(self) -> None:
        await self._close_event.wait()

    @property
    def is_closed(self) -> bool:
        return self._closed is not None or self._close_event.is_set()

    @property
    def initiator(self) -> bool:
        return self._initiator

    # --- handshake -----------------------------------------------------------------

    async def _handshake_initiate(self) -> None:
        hello = {
            "versions": PROTOCOL_VERSIONS,
            "cluster_name": self._local.cluster_name,
            "node_id": self._local.node_id.bytes,
            "listen_addr": self._local.listen_addr,
            "role": self._local.role,
            "capabilities": {"compression": compression.offered(self._config.compression)},
        }
        self._mux.open_stream()  # control stream (id 1)
        await self._send_envelope(
            CONTROL_CHANNEL, envelope.MsgType.HELLO, 0, msgpack.packb(hello), handshake=True
        )
        reply = await self._await_handshake_envelope()
        if reply.msg_type == envelope.MsgType.HELLO_REJECT:
            reject = _unpack_map(reply.body)
            code = reject.get("code", 0)
            raise HandshakeError(
                code if isinstance(code, int) else 0,
                str(reject.get("reason", "rejected")),
            )
        if reply.msg_type != envelope.MsgType.HELLO_ACK:
            raise ProtocolError(f"expected HELLO_ACK, got 0x{reply.msg_type:x}")
        ack = _unpack_map(reply.body)
        self.peer = _peer_from(ack)
        negotiated = ack.get("compression")
        if negotiated is not None and not isinstance(negotiated, str):
            raise ProtocolError("HELLO_ACK compression must be a string or nil")
        if negotiated is not None:
            self._mux.set_compressor(compression.make(negotiated))
        self.compression = negotiated
        self._handshaken = True
        # establish the message channel (id 3) eagerly
        opened = self._mux.open_stream()
        if opened != MESSAGE_CHANNEL:
            raise ProtocolError(f"expected message channel id 3, got {opened}")
        self._mux.send_data(MESSAGE_CHANNEL, b"")
        self._schedule_flush()
        self._message_channel_open.set()

    async def _handshake_accept(self) -> None:
        received = await self._await_handshake_envelope()
        if received.msg_type != envelope.MsgType.HELLO:
            raise ProtocolError(f"expected HELLO, got 0x{received.msg_type:x}")
        hello = _unpack_map(received.body)
        cluster = hello.get("cluster_name")
        versions_raw = hello.get("versions", [])
        if not isinstance(versions_raw, list):
            raise ProtocolError("HELLO versions must be a list")
        versions = [v for v in versions_raw if isinstance(v, int) and not isinstance(v, bool)]
        node_id_raw = hello.get("node_id", b"")
        if not isinstance(node_id_raw, bytes):
            raise ProtocolError("HELLO node_id must be bytes")
        if cluster != self._local.cluster_name:
            await self._reject(REJECT_WRONG_CLUSTER, f"wrong cluster {cluster!r}")
        if not set(versions) & set(PROTOCOL_VERSIONS):
            await self._reject(REJECT_INCOMPATIBLE_VERSION, f"no common version in {versions}")
        if node_id_raw == self._local.node_id.bytes:
            await self._reject(REJECT_SELF_CONNECT, "connected to self")
        capabilities = hello.get("capabilities", {})
        offer_raw = capabilities.get("compression", []) if isinstance(capabilities, dict) else []
        # unknown codec names are ignored during negotiation; wrong shapes too
        offer = (
            [c for c in offer_raw if isinstance(c, str)] if isinstance(offer_raw, list) else []
        )
        negotiated = compression.negotiate(offer, self._config.compression)
        self.peer = _peer_from(hello)
        ack = {
            "version": max(set(versions) & set(PROTOCOL_VERSIONS)),
            "node_id": self._local.node_id.bytes,
            "listen_addr": self._local.listen_addr,
            "role": self._local.role,
            "compression": negotiated,
        }
        await self._send_envelope(
            CONTROL_CHANNEL, envelope.MsgType.HELLO_ACK, 0, msgpack.packb(ack), handshake=True
        )
        if negotiated is not None:
            self._mux.set_compressor(compression.make(negotiated))
        self.compression = negotiated
        self._handshaken = True

    async def _await_handshake_envelope(self) -> envelope.Envelope:
        try:
            return await asyncio.wait_for(self._hello_future, self._config.handshake_timeout)
        except TimeoutError as exc:
            raise CastyTimeoutError("handshake timed out") from exc

    async def _reject(self, code: int, reason: str) -> typing.NoReturn:
        body = msgpack.packb({"code": code, "reason": reason})
        await self._send_envelope(
            CONTROL_CHANNEL, envelope.MsgType.HELLO_REJECT, 0, body, handshake=True
        )
        await self.close(code=code, reason=reason)
        raise HandshakeError(code, reason)

    # --- sending ---------------------------------------------------------------------

    async def _send_envelope(
        self,
        stream_id: int,
        msg_type: int,
        correlation_id: int,
        body: bytes,
        *,
        handshake: bool = False,
    ) -> None:
        if not handshake and stream_id == MESSAGE_CHANNEL:
            await self._message_channel_open.wait()
        await self._send_stream(stream_id, envelope.encode(msg_type, correlation_id, body))

    async def _send_stream(self, stream_id: int, data: bytes) -> None:
        """Send bytes on a stream, respecting its flow-control window.

        Fast path: when nobody is mid-envelope on this stream and the window
        fits the whole payload, the bytes are queued synchronously (atomic in
        the event loop, so no interleaving is possible) and the shared flusher
        batches them with everything else queued this loop iteration. The
        locked path handles window-limited sends, whose awaits between chunks
        are why the lock exists at all."""
        lock = self._send_locks.setdefault(stream_id, asyncio.Lock())
        if not lock.locked():
            self._ensure_open()
            if len(data) <= self._mux.send_window(stream_id):
                self._mux.send_data(stream_id, data)
                self._schedule_flush()
                return
        async with lock:
            view = memoryview(data)
            while view.nbytes:
                self._ensure_open()
                window = self._mux.send_window(stream_id)
                if window == 0:
                    waiter = self._window_waiters.setdefault(stream_id, asyncio.Event())
                    waiter.clear()
                    await waiter.wait()
                    continue
                step = min(window, view.nbytes)
                self._mux.send_data(stream_id, bytes(view[:step]))
                view = view[step:]
                self._schedule_flush()

    async def _finish_stream(self, stream_id: int) -> None:
        self._mux.send_data(stream_id, b"", fin=True)
        self._schedule_flush()

    async def _reset_stream(self, stream_id: int) -> None:
        self._mux.reset(stream_id)
        self._schedule_flush()

    async def _replenish(self, stream_id: int, nbytes: int) -> None:
        if self._closed is None:
            self._mux.consume(stream_id, nbytes)
            self._schedule_flush()

    def _schedule_flush(self) -> None:
        """Coalesced writes: one flusher task drains the mux buffer for every
        sender, so N concurrent sends cost one write+drain instead of N."""
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def _flush_loop(self) -> None:
        try:
            while True:
                data = self._mux.data_to_send()
                if not data or self._writer.is_closing():
                    return
                async with self._write_lock:
                    self._writer.write(data)
                    await self._writer.drain()
        except OSError as exc:  # ConnectionResetError and friends
            self._fail(ConnectionLostError(str(exc) or "connection lost on write"))

    async def _flush(self) -> None:
        """Direct, awaited flush; only for teardown (GO_AWAY must hit the wire
        before the writer closes). Hot paths go through _schedule_flush."""
        data = self._mux.data_to_send()
        if not data or self._writer.is_closing():
            return
        try:
            async with self._write_lock:
                self._writer.write(data)
                await self._writer.drain()
        except OSError as exc:  # ConnectionResetError and friends
            error = ConnectionLostError(str(exc) or "connection lost on write")
            self._fail(error)
            raise error from exc

    # --- receiving ----------------------------------------------------------------------

    def _start(self) -> None:
        self._read_task = asyncio.create_task(self._read_loop())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def _read_loop(self) -> None:
        try:
            while True:
                data = await self._reader.read(_READ_CHUNK)
                if not data:
                    raise ConnectionLostError("peer closed the connection")
                self._last_recv = asyncio.get_running_loop().time()
                for event in self._mux.feed(data):
                    await self._on_event(event)
                self._schedule_flush()  # auto-pongs / window updates queued by feed
        except asyncio.CancelledError:
            raise
        except ConnectionResetError as exc:
            self._fail(ConnectionLostError(str(exc) or "connection reset"))
        except CastyError as exc:
            self._fail(exc)
        except OSError as exc:
            self._fail(ConnectionLostError(str(exc)))
        finally:
            if self._closed is None:
                self._fail(ConnectionLostError("read loop exited"))
            await self._shutdown_io()

    async def _on_event(self, event: mux.Event) -> None:
        match event:
            case mux.DataReceived(stream_id=sid, data=data):
                await self._on_stream_data(sid, data)
            case mux.StreamOpened(stream_id=sid):
                if not self._handshaken and sid != CONTROL_CHANNEL:
                    raise ProtocolError(f"stream {sid} opened before handshake")
                if sid == MESSAGE_CHANNEL:
                    self._message_channel_open.set()
                elif self._handshaken and sid != CONTROL_CHANNEL:
                    self._bulk_decoders[sid] = envelope.Decoder(
                        max_message_bytes=self._config.max_message_bytes
                    )
            case mux.StreamFinished(stream_id=sid):
                receiver = self._bulk_receivers.pop(sid, None)
                if receiver is not None:
                    receiver._queue.put_nowait(None)  # EOF
                stream = self._actor_streams.get(sid)
                if stream is not None:
                    stream._recv_finished = True
                    stream._inbound.put_nowait(None)  # end of the peer's items
                    if stream._send_finished:
                        del self._actor_streams[sid]
                self._bulk_decoders.pop(sid, None)
                self._envelope_channels.pop(sid, None)
            case mux.StreamRejected(stream_id=sid):
                receiver = self._bulk_receivers.pop(sid, None)
                if receiver is not None:
                    receiver._queue.put_nowait(StreamResetError(sid))
                stream = self._actor_streams.pop(sid, None)
                if stream is not None:
                    stream._inbound.put_nowait(StreamResetError(sid))
                self._bulk_decoders.pop(sid, None)
                self._envelope_channels.pop(sid, None)
                waiter = self._window_waiters.pop(sid, None)
                if waiter is not None:
                    waiter.set()
            case mux.WindowAvailable(stream_id=sid):
                waiter = self._window_waiters.get(sid)
                if waiter is not None:
                    waiter.set()
            case mux.PongReceived(opaque=opaque):
                pong = self._pong_waiters.get(opaque)
                if pong is not None and not pong.done():
                    pong.set_result(None)
            case mux.ConnectionClosed():
                self._fail(ConnectionLostError("peer sent GO_AWAY"))

    async def _on_stream_data(self, stream_id: int, data: bytes) -> None:
        if stream_id == CONTROL_CHANNEL:
            for received in self._control_decoder.feed(data):
                self._on_control_envelope(received)
            await self._replenish(stream_id, len(data))
        elif stream_id == MESSAGE_CHANNEL:
            if not self._handshaken:
                raise ProtocolError("message channel data before handshake")
            for received in self._message_decoder.feed(data):
                self._on_message(received, stream_id)
            await self._replenish(stream_id, len(data))
        elif (stream := self._actor_streams.get(stream_id)) is not None:
            stream._feed(data)  # no replenish here: credit returns as items are consumed
        elif (decoder := self._envelope_channels.get(stream_id)) is not None:
            for received in decoder.feed(data):
                self._on_message(received, stream_id)
            await self._replenish(stream_id, len(data))
        else:
            await self._on_bulk_data(stream_id, data)

    def _on_control_envelope(self, received: envelope.Envelope) -> None:
        if not self._handshaken:
            if not self._hello_future.done():
                self._hello_future.set_result(received)
            return
        if self._on_control is None:
            raise ProtocolError(f"unexpected control message 0x{received.msg_type:x}")
        self._spawn(self._on_control(self, received.msg_type, received.body))

    def _on_message(self, received: envelope.Envelope, stream_id: int) -> None:
        if received.msg_type in (envelope.MsgType.RESPONSE, envelope.MsgType.ERROR):
            pending = self._pending.get(received.correlation_id)
            if pending is not None and not pending.done():
                pending.set_result(received)
            return  # late replies are silently dropped
        self._spawn(self._handle_request(received, stream_id))

    async def _handle_request(self, received: envelope.Envelope, stream_id: int) -> None:
        try:
            if self._on_request is None:
                raise RemoteError(0, "no request handler on this node")
            reply = await self._on_request(self, received.msg_type, received.body)
            if received.correlation_id != 0:
                await self._send_envelope(
                    stream_id, envelope.MsgType.RESPONSE, received.correlation_id, reply
                )
        except (ConnectionLostError, asyncio.CancelledError):
            pass
        except Exception as exc:
            if received.correlation_id != 0 and self._closed is None:
                code = exc.code if isinstance(exc, RemoteError) else 0
                body = msgpack.packb({"code": code, "message": str(exc)})
                await self._send_envelope(
                    stream_id, envelope.MsgType.ERROR, received.correlation_id, body
                )

    async def _on_bulk_data(self, stream_id: int, data: bytes) -> None:
        receiver = self._bulk_receivers.get(stream_id)
        if receiver is not None:
            receiver._queue.put_nowait(data)
            return
        decoder = self._bulk_decoders.get(stream_id)
        if decoder is None:
            raise ProtocolError(f"data on unknown stream {stream_id}")
        opened = decoder.feed(data)
        if not opened:
            return  # BULK_OPEN spans frames; keep buffering
        header, *rest = opened
        if header.msg_type != envelope.MsgType.BULK_OPEN:
            raise ProtocolError(f"bulk stream {stream_id} must start with a single BULK_OPEN")
        info = _unpack_map(header.body)
        if info.get("kind") == REPLICATION_KIND:
            # not bulk: the peer opened its replication lane; this stream keeps
            # carrying envelopes (requests answered on this same stream)
            del self._bulk_decoders[stream_id]
            self._envelope_channels[stream_id] = decoder
            await self._replenish(stream_id, len(data))
            for received in rest:
                self._on_message(received, stream_id)
            return
        if info.get("kind") == ACTOR_STREAM_KIND:
            descriptor_raw = info.get("descriptor", b"")
            descriptor = descriptor_raw if isinstance(descriptor_raw, bytes) else b""
            stream = ActorStream(self, stream_id)
            for received in rest:  # items that shared the BULK_OPEN frame
                stream._inbound.put_nowait(received)
            leftover = decoder.drain()  # partial next envelope
            if leftover:
                stream._feed(leftover)
            del self._bulk_decoders[stream_id]
            self._actor_streams[stream_id] = stream
            await self._replenish(stream_id, envelope.wire_size(len(header.body)))
            if self._on_actor_stream is None:
                await self._reset_stream(stream_id)
                del self._actor_streams[stream_id]
                return
            self._spawn(self._on_actor_stream(self, stream, descriptor))
            return
        if rest:
            raise ProtocolError(f"bulk stream {stream_id} must start with a single BULK_OPEN")
        meta_raw = info.get("meta", {})
        meta = (
            {key: value for key, value in meta_raw.items() if isinstance(key, str)}
            if isinstance(meta_raw, dict)
            else {}
        )
        receiver = BulkReceiver(self, stream_id, kind=str(info.get("kind", "")), meta=meta)
        self._bulk_receivers[stream_id] = receiver
        leftover = decoder.drain()  # bytes that arrived after the header in the same frame
        if leftover:
            receiver._queue.put_nowait(leftover)
        del self._bulk_decoders[stream_id]
        await self._replenish(stream_id, envelope.wire_size(len(header.body)))
        if self._on_bulk is None:
            await self._reset_stream(stream_id)
            del self._bulk_receivers[stream_id]
            return
        self._spawn(self._on_bulk(self, receiver))

    # --- keepalive -----------------------------------------------------------------------

    async def _keepalive_loop(self) -> None:
        try:
            while self._closed is None:
                await asyncio.sleep(self._config.keepalive_interval)
                loop = asyncio.get_running_loop()
                if loop.time() - self._last_recv < self._config.keepalive_interval:
                    continue  # not idle
                try:
                    await self.ping()
                except CastyTimeoutError:
                    self._fail(ConnectionLostError("keepalive timed out"))
                    await self._shutdown_io()
                    return
        except asyncio.CancelledError:
            raise
        except CastyError:
            pass

    # --- lifecycle -------------------------------------------------------------------------

    def _spawn(self, coro: Awaitable[None]) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _ensure_open(self) -> None:
        if self._closed is not None:
            raise self._closed

    def _fail(self, exc: Exception | None) -> None:
        if self._close_event.is_set():
            return
        self._closed = exc or ConnectionLostError("connection closed")
        self._close_event.set()
        error = exc or ConnectionLostError("connection closed")
        for pending in self._pending.values():
            if not pending.done():
                pending.set_exception(error)
        self._pending.clear()
        if not self._hello_future.done():
            self._hello_future.set_exception(error)
        for pong in self._pong_waiters.values():
            if not pong.done():
                pong.set_exception(error)
        self._pong_waiters.clear()
        for receiver in self._bulk_receivers.values():
            receiver._queue.put_nowait(error)
        self._bulk_receivers.clear()
        for stream in self._actor_streams.values():
            stream._inbound.put_nowait(error)
        self._actor_streams.clear()
        self._envelope_channels.clear()
        for waiter in self._window_waiters.values():
            waiter.set()
        if self._on_close is not None:
            self._on_close(self, exc)

    async def _shutdown_io(self) -> None:
        for task in (self._keepalive_task, self._read_task, self._flush_task):
            if task is not None and task is not asyncio.current_task() and not task.done():
                task.cancel()
        if not self._writer.is_closing():
            self._writer.close()
        # The close waiter is shared between the read task's shutdown and
        # explicit close(); whichever loses the race sees CancelledError.
        # Closing the transport is best-effort by this point.
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await self._writer.wait_closed()


def _decode_error(body: bytes) -> RemoteError:
    info = _unpack_map(body)
    code = info.get("code", 0)
    return RemoteError(
        code if isinstance(code, int) else 0, str(info.get("message", "unknown"))
    )


def _unpack_map(body: bytes) -> dict[str, object]:
    try:
        raw: object = msgpack.unpackb(body)
    except Exception as exc:
        raise ProtocolError(f"malformed control payload: {exc}") from exc
    if not isinstance(raw, dict):
        raise ProtocolError("control payload is not a map")
    typed = typing.cast(dict[object, object], raw)
    if not all(isinstance(key, str) for key in typed):
        raise ProtocolError("control payload keys must be strings")
    return {key: value for key, value in typed.items() if isinstance(key, str)}


def _peer_from(info: dict[str, object]) -> PeerInfo:
    node_id_raw = info.get("node_id", b"")
    if not isinstance(node_id_raw, bytes) or len(node_id_raw) != 16:
        raise ProtocolError("peer node_id must be 16 bytes")
    listen_addr = info.get("listen_addr")
    if listen_addr is not None and not isinstance(listen_addr, str):
        raise ProtocolError("peer listen_addr must be a string or nil")
    role = info.get("role", "member")
    if not isinstance(role, str):
        raise ProtocolError("peer role must be a string")
    return PeerInfo(node_id=uuid.UUID(bytes=node_id_raw), listen_addr=listen_addr, role=role)
