"""
QUIC Stream Actor - Manages individual stream lifecycle with backpressure.

States:
    active      → Normal read/write
    suspended   → Reading paused (backpressure)
    half_closed → Peer finished, we can still write
    closing     → Graceful close in progress
"""

from collections import deque
from typing import TYPE_CHECKING, Any

from casty import Actor, LocalRef, Context

from .messages import (
    StreamRegister,
    Write,
    SuspendReading,
    ResumeReading,
    CloseStream,
    ResetStream,
    Received,
    WriteAck,
    StreamClosed,
    StreamReset,
    PeerFinished,
    CommandFailed,
    _StreamData,
    _StreamReset,
)

if TYPE_CHECKING:
    from .connection import Connection


class Stream(Actor):
    """QUIC stream actor with backpressure support.

    States:
    - active: Normal read/write operations
    - suspended: Reading paused (backpressure)
    - half_closed: Peer finished sending, we can still write
    - closing: Graceful close in progress

    Usage (after receiving StreamCreated):
        # Write data
        await stream.send(quic.Write(b"hello", ack="msg1"))

        # End stream
        await stream.send(quic.Write(b"goodbye", end_stream=True))

        # Backpressure
        await stream.send(quic.SuspendReading())
        await stream.send(quic.ResumeReading())

        # Close
        await stream.send(quic.CloseStream())

    Events sent to handler:
        - Received(data) for incoming data
        - WriteAck(token) when write completes
        - PeerFinished() when peer sends FIN
        - StreamClosed() on graceful close
        - StreamReset(error_code) on reset
        - CommandFailed(command, cause) on errors
    """

    def __init__(
        self,
        connection: "Connection",
        stream_id: int,
        handler: LocalRef,
        is_unidirectional: bool = False,
    ):
        self._connection = connection
        self._stream_id = stream_id
        self._handler = handler
        self._is_unidirectional = is_unidirectional

        # Backpressure state
        self._reading_paused = False
        self._pending_data: deque[tuple[bytes, bool]] = deque()  # (data, end_stream)

        # Write state
        self._write_ended = False

        # Peer state
        self._peer_finished = False

    @property
    def stream_id(self) -> int:
        """Get the QUIC stream ID."""
        return self._stream_id

    async def on_stop(self):
        """Cleanup on actor stop."""
        self._connection.remove_stream(self._stream_id)

    # =========================================================================
    # STATE: ACTIVE (default receive)
    # =========================================================================

    async def receive(self, msg, ctx: Context):
        """Active state - normal read/write."""
        match msg:
            case StreamRegister(handler):
                self._handler = handler

            case Write(data, ack, end_stream):
                await self._do_write(data, ack, end_stream, ctx)

            case SuspendReading():
                self._reading_paused = True
                ctx.become(self._suspended)

            case ResumeReading():
                pass  # Already active

            case CloseStream(error_code):
                ctx.become(self._closing)
                await self._start_close(error_code, ctx)

            case ResetStream(error_code):
                await self._do_reset(error_code, ctx)

            # Internal: data from connection
            case _StreamData(stream_id, data, end_stream):
                if self._reading_paused:
                    self._pending_data.append((data, end_stream))
                else:
                    if data:
                        await self._send_event(Received(data), ctx)
                    if end_stream:
                        await self._handle_peer_finished(ctx)

            case _StreamReset(stream_id, error_code):
                await self._handle_reset(error_code, ctx)

    # =========================================================================
    # STATE: SUSPENDED (backpressure)
    # =========================================================================

    async def _suspended(self, msg, ctx: Context):
        """Suspended state - reading paused."""
        match msg:
            case StreamRegister(handler):
                self._handler = handler

            case Write(data, ack, end_stream):
                await self._do_write(data, ack, end_stream, ctx)

            case SuspendReading():
                pass  # Already suspended

            case ResumeReading():
                self._reading_paused = False
                # Flush pending data
                while self._pending_data:
                    data, end_stream = self._pending_data.popleft()
                    if data:
                        await self._send_event(Received(data), ctx)
                    if end_stream:
                        await self._handle_peer_finished(ctx)
                ctx.unbecome()

            case CloseStream(error_code):
                ctx.become(self._closing, discard_old=True)
                await self._start_close(error_code, ctx)

            case ResetStream(error_code):
                await self._do_reset(error_code, ctx)

            # Buffer data while suspended
            case _StreamData(stream_id, data, end_stream):
                self._pending_data.append((data, end_stream))

            case _StreamReset(stream_id, error_code):
                await self._handle_reset(error_code, ctx)

    # =========================================================================
    # STATE: HALF_CLOSED (peer finished, we can still write)
    # =========================================================================

    async def _half_closed(self, msg, ctx: Context):
        """Half-closed state - peer finished, we can still write."""
        match msg:
            case StreamRegister(handler):
                self._handler = handler

            case Write(data, ack, end_stream):
                await self._do_write(data, ack, end_stream, ctx)

            case SuspendReading() | ResumeReading():
                pass  # No more reading

            case CloseStream(error_code):
                ctx.become(self._closing, discard_old=True)
                await self._start_close(error_code, ctx)

            case ResetStream(error_code):
                await self._do_reset(error_code, ctx)

            case _StreamData(stream_id, data, end_stream):
                # Shouldn't happen, but handle gracefully
                pass

            case _StreamReset(stream_id, error_code):
                await self._handle_reset(error_code, ctx)

    # =========================================================================
    # STATE: CLOSING
    # =========================================================================

    async def _closing(self, msg, ctx: Context):
        """Closing state - graceful close in progress."""
        match msg:
            case Write():
                await self._send_command_failed(
                    msg,
                    ConnectionError("Stream is closing"),
                    ctx,
                )

            case CloseStream() | ResetStream():
                pass  # Already closing

            case SuspendReading() | ResumeReading():
                pass  # Ignore

            case _StreamData() | _StreamReset():
                pass  # Ignore during close

            case StreamRegister(handler):
                self._handler = handler

    # =========================================================================
    # OPERATIONS
    # =========================================================================

    async def _do_write(
        self,
        data: bytes,
        ack: Any,
        end_stream: bool,
        ctx: Context,
    ):
        """Write data to the stream."""
        if self._write_ended:
            await self._send_command_failed(
                Write(data, ack, end_stream),
                ConnectionError("Stream write already ended"),
                ctx,
            )
            return

        try:
            self._connection.send_stream_data(
                self._stream_id,
                data,
                end_stream=end_stream,
            )

            if end_stream:
                self._write_ended = True

            if ack is not None:
                await self._send_event(WriteAck(ack), ctx)

            # If we sent end_stream and peer finished, stream is done
            if end_stream and self._peer_finished:
                ctx.become(self._closing)
                await self._send_event(StreamClosed(), ctx)

        except Exception as exc:
            await self._send_command_failed(Write(data, ack, end_stream), exc, ctx)

    async def _start_close(self, error_code: int, ctx: Context):
        """Start graceful stream close."""
        if not self._write_ended:
            try:
                # Send empty data with end_stream=True
                self._connection.send_stream_data(
                    self._stream_id,
                    b"",
                    end_stream=True,
                )
                self._write_ended = True
            except Exception:
                pass

        await self._send_event(StreamClosed(), ctx)

    async def _do_reset(self, error_code: int, ctx: Context):
        """Reset the stream."""
        try:
            self._connection.reset_stream(self._stream_id, error_code)
            self._write_ended = True
            await self._send_event(StreamReset(error_code), ctx)
        except Exception as exc:
            await self._send_command_failed(ResetStream(error_code), exc, ctx)

    async def _handle_peer_finished(self, ctx: Context):
        """Handle peer finishing (FIN received)."""
        self._peer_finished = True
        await self._send_event(PeerFinished(), ctx)

        # If we also finished, stream is done
        if self._write_ended:
            ctx.become(self._closing)
            await self._send_event(StreamClosed(), ctx)
        else:
            # Move to half-closed state
            ctx.become(self._half_closed)

    async def _handle_reset(self, error_code: int, ctx: Context):
        """Handle stream reset from peer."""
        self._peer_finished = True
        self._write_ended = True
        ctx.become(self._closing)
        await self._send_event(StreamReset(error_code), ctx)

    # =========================================================================
    # HELPERS
    # =========================================================================

    async def _send_event(self, event, ctx: Context):
        """Send event to handler."""
        if self._handler:
            await self._handler.send(event, sender=ctx.self_ref)

    async def _send_command_failed(self, command, error: Exception, ctx: Context):
        """Send CommandFailed event."""
        await self._send_event(CommandFailed(command, error), ctx)
