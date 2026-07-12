from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from casty.config import TransportConfig
from casty.errors import ProtocolError
from casty.transport import frame as fr

CONNECTION_STREAM_ID = 0


class Compressor(Protocol):
    name: str

    def compress(self, data: bytes) -> bytes: ...

    def decompress(self, data: bytes, max_size: int) -> bytes: ...


# --- events emitted by Mux.feed() -------------------------------------------


@dataclass(frozen=True, slots=True)
class StreamOpened:
    stream_id: int


@dataclass(frozen=True, slots=True)
class DataReceived:
    stream_id: int
    data: bytes


@dataclass(frozen=True, slots=True)
class StreamFinished:
    """Peer half-closed the stream (FIN): no more data will arrive."""

    stream_id: int


@dataclass(frozen=True, slots=True)
class StreamRejected:
    """Peer reset a stream we opened."""

    stream_id: int


@dataclass(frozen=True, slots=True)
class WindowAvailable:
    """Send window went from zero to positive."""

    stream_id: int


@dataclass(frozen=True, slots=True)
class PongReceived:
    opaque: bytes


@dataclass(frozen=True, slots=True)
class ConnectionClosed:
    """Peer sent GO_AWAY; payload is opaque to the mux."""

    payload: bytes


Event = (
    StreamOpened
    | DataReceived
    | StreamFinished
    | StreamRejected
    | WindowAvailable
    | PongReceived
    | ConnectionClosed
)


@dataclass(slots=True)
class _Stream:
    send_window: int
    recv_window: int
    syn_pending: bool = False
    local_closed: bool = False
    remote_closed: bool = False


class Mux:
    """Sans-IO stream multiplexer (yamux-style semantics, casty wire).

    All methods either consume received bytes (`feed`, returning events) or
    queue outbound bytes retrievable via `data_to_send`. No I/O, no clock.

    Flow-control windows are counted in *uncompressed* stream bytes on both
    sides, so sender and receiver agree regardless of per-frame compression.
    """

    def __init__(
        self,
        *,
        initiator: bool,
        config: TransportConfig | None = None,
        compressor: Compressor | None = None,
    ) -> None:
        self._config = config or TransportConfig()
        self._compressor = compressor
        self._next_stream_id = 1 if initiator else 2
        self._peer_parity = 0 if initiator else 1
        self._max_peer_seen = 0
        self._streams: dict[int, _Stream] = {}
        self._decoder = fr.Decoder(max_frame_bytes=self._config.max_frame_bytes)
        self._out = bytearray()
        self._closed = False

    def set_compressor(self, compressor: Compressor) -> None:
        """Install the codec negotiated during the handshake."""
        self._compressor = compressor

    # --- outbound ------------------------------------------------------------

    def open_stream(self) -> int:
        """Allocate a local stream id. SYN travels with the first DATA frame."""
        self._check_open()
        stream_id = self._next_stream_id
        self._next_stream_id += 2
        self._streams[stream_id] = self._new_stream(syn_pending=True)
        return stream_id

    def send_window(self, stream_id: int) -> int:
        return self._require(stream_id).send_window

    def send_data(self, stream_id: int, data: bytes, *, fin: bool = False) -> None:
        self._check_open()
        stream = self._require(stream_id)
        if stream.local_closed:
            raise ProtocolError(f"stream {stream_id} already closed locally")
        if len(data) > stream.send_window:
            raise ProtocolError(
                f"send of {len(data)} bytes exceeds window {stream.send_window} "
                f"on stream {stream_id}; check send_window() first"
            )
        stream.send_window -= len(data)
        syn = stream.syn_pending
        stream.syn_pending = False
        chunks = self._chunk(data)
        for i, chunk in enumerate(chunks):
            flags = fr.Flags.NONE
            if syn and i == 0:
                flags |= fr.Flags.SYN
            if fin and i == len(chunks) - 1:
                flags |= fr.Flags.FIN
            payload = chunk
            if (
                self._compressor is not None
                and len(chunk) >= self._config.compression.min_bytes
            ):
                compressed = self._compressor.compress(chunk)
                if len(compressed) < len(chunk):
                    payload = compressed
                    flags |= fr.Flags.COMPRESSED
            self._emit(fr.Frame(fr.FrameType.DATA, flags, stream_id, payload))
        if fin:
            stream.local_closed = True
            self._reap(stream_id)

    def consume(self, stream_id: int, nbytes: int) -> None:
        """Report consumed stream bytes; replenishes the peer's send window."""
        self._check_open()
        stream = self._streams.get(stream_id)
        if stream is None or stream.remote_closed:
            return  # nothing to replenish on a gone stream
        stream.recv_window += nbytes
        self._emit(
            fr.Frame(fr.FrameType.WINDOW_UPDATE, fr.Flags.NONE, stream_id, credits=nbytes)
        )

    def reset(self, stream_id: int) -> None:
        self._check_open()
        if stream_id in self._streams:
            self._emit(fr.Frame(fr.FrameType.DATA, fr.Flags.RST, stream_id))
            del self._streams[stream_id]

    def ping(self, opaque: bytes) -> None:
        self._check_open()
        if len(opaque) != 8:
            raise ProtocolError("ping opaque must be 8 bytes")
        self._emit(fr.Frame(fr.FrameType.PING, fr.Flags.NONE, CONNECTION_STREAM_ID, opaque))

    def go_away(self, payload: bytes) -> None:
        if not self._closed:
            self._emit(fr.Frame(fr.FrameType.GO_AWAY, fr.Flags.NONE, CONNECTION_STREAM_ID, payload))
            self._closed = True

    def data_to_send(self) -> bytes:
        out = bytes(self._out)
        self._out.clear()
        return out

    # --- inbound -------------------------------------------------------------

    def feed(self, data: bytes) -> list[Event]:
        if self._closed:
            return []
        events: list[Event] = []
        for received in self._decoder.feed(data):
            self._dispatch(received, events)
            if self._closed:
                break
        return events

    def _dispatch(self, received: fr.Frame, events: list[Event]) -> None:
        match received.type:
            case fr.FrameType.DATA:
                self._on_data(received, events)
            case fr.FrameType.WINDOW_UPDATE:
                self._on_window_update(received, events)
            case fr.FrameType.PING:
                if received.flags & fr.Flags.ACK:
                    events.append(PongReceived(received.payload))
                else:
                    self._emit(
                        fr.Frame(
                            fr.FrameType.PING,
                            fr.Flags.ACK,
                            CONNECTION_STREAM_ID,
                            received.payload,
                        )
                    )
            case fr.FrameType.GO_AWAY:
                self._closed = True
                events.append(ConnectionClosed(received.payload))

    def _on_data(self, received: fr.Frame, events: list[Event]) -> None:
        stream_id = received.stream_id
        if stream_id == CONNECTION_STREAM_ID:
            raise ProtocolError("DATA on stream 0")

        if received.flags & fr.Flags.RST:
            if received.payload:
                raise ProtocolError("RST carries no payload")
            if self._streams.pop(stream_id, None) is not None:
                events.append(StreamRejected(stream_id))
            return

        stream = self._streams.get(stream_id)
        if stream is None:
            stream = self._accept_peer_stream(received, events)
            if stream is None:
                return  # refused (RST queued)

        if stream.remote_closed:
            raise ProtocolError(f"DATA on remotely-closed stream {stream_id}")

        payload = received.payload
        if received.flags & fr.Flags.COMPRESSED:
            if self._compressor is None:
                raise ProtocolError("compressed frame on a connection without a codec")
            payload = self._compressor.decompress(
                payload, self._config.max_frame_bytes
            )
        if payload:
            if len(payload) > stream.recv_window:
                raise ProtocolError(f"peer exceeded recv window on stream {stream_id}")
            stream.recv_window -= len(payload)
            events.append(DataReceived(stream_id, payload))
        if received.flags & fr.Flags.FIN:
            stream.remote_closed = True
            events.append(StreamFinished(stream_id))
            self._reap(stream_id)

    def _accept_peer_stream(
        self, received: fr.Frame, events: list[Event]
    ) -> _Stream | None:
        stream_id = received.stream_id
        if stream_id % 2 != self._peer_parity:
            raise ProtocolError(f"peer opened stream {stream_id} with wrong parity")
        if stream_id <= self._max_peer_seen:
            raise ProtocolError(f"peer reused or reordered stream id {stream_id}")
        if not received.flags & fr.Flags.SYN:
            raise ProtocolError(f"first frame on stream {stream_id} lacks SYN")
        self._max_peer_seen = stream_id
        if len(self._streams) >= self._config.max_concurrent_streams:
            self._emit(fr.Frame(fr.FrameType.DATA, fr.Flags.RST, stream_id))
            return None
        stream = self._new_stream(syn_pending=False)
        self._streams[stream_id] = stream
        events.append(StreamOpened(stream_id))
        return stream

    def _on_window_update(self, received: fr.Frame, events: list[Event]) -> None:
        stream = self._streams.get(received.stream_id)
        if stream is None:
            return  # stream may have been reset/finished locally; stale credit is fine
        was_blocked = stream.send_window == 0
        stream.send_window += received.credits
        if was_blocked and stream.send_window > 0:
            events.append(WindowAvailable(received.stream_id))

    # --- internals -----------------------------------------------------------

    def _new_stream(self, *, syn_pending: bool) -> _Stream:
        return _Stream(
            send_window=self._config.initial_window_bytes,
            recv_window=self._config.initial_window_bytes,
            syn_pending=syn_pending,
        )

    def _chunk(self, data: bytes) -> list[bytes]:
        if not data:
            return [b""]
        size = self._config.max_frame_bytes
        view = memoryview(data)
        return [bytes(view[i : i + size]) for i in range(0, len(data), size)]

    def _require(self, stream_id: int) -> _Stream:
        stream = self._streams.get(stream_id)
        if stream is None:
            raise ProtocolError(f"unknown stream {stream_id}")
        return stream

    def _reap(self, stream_id: int) -> None:
        stream = self._streams.get(stream_id)
        if stream is not None and stream.local_closed and stream.remote_closed:
            del self._streams[stream_id]

    def _emit(self, outbound: fr.Frame) -> None:
        self._out.extend(fr.encode(outbound))

    def _check_open(self) -> None:
        if self._closed:
            raise ProtocolError("connection is closed")
