"""
QUIC Connection Actor - Manages streams and connection lifecycle.

States:
    active   → Normal operation
    draining → Connection closing, no new streams
"""

import asyncio
from typing import TYPE_CHECKING

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.events import (
    ConnectionTerminated,
    HandshakeCompleted as QuicHandshakeCompleted,
    StreamDataReceived,
    StreamReset as QuicStreamReset,
    DatagramFrameReceived,
)

from casty import Actor, LocalRef, Context

from .stream import Stream
from .messages import (
    Register,
    CreateStream,
    CloseConnection,
    SendDatagram,
    StreamCreated,
    DatagramReceived,
    DatagramAck,
    ConnectionClosed,
    ConnectionErrorClosed,
    HandshakeCompleted,
    CommandFailed,
    _StreamData,
    _StreamReset,
)
from .options import QuicOptions

if TYPE_CHECKING:
    from aioquic.quic.connection import QuicConnection as AioQuicConnection


class Connection(Actor):
    """QUIC connection actor managing streams.

    States:
    - active: Normal operation, can create streams
    - draining: Connection closing, no new streams allowed

    Manages:
    - Stream lifecycle (create, data routing, close)
    - Datagram support (RFC 9221)
    - Connection-level flow control
    - Event routing to handler

    Usage (after receiving Connected/Accepted):
        # Create stream
        await connection.send(quic.CreateStream())

        # Send datagram
        await connection.send(quic.SendDatagram(b"data"))

        # Close connection
        await connection.send(quic.CloseConnection())

    Events sent to handler:
        - StreamCreated(stream, stream_id, ...) for new streams
        - DatagramReceived(data) for received datagrams
        - HandshakeCompleted(...) on TLS completion
        - ConnectionClosed(...) on graceful close
        - ConnectionErrorClosed(cause) on error
    """

    def __init__(
        self,
        protocol: QuicConnectionProtocol,
        handler: LocalRef,
        options: QuicOptions | None = None,
        is_client: bool = True,
    ):
        self._protocol = protocol
        self._handler = handler
        self._options = options or QuicOptions()
        self._is_client = is_client

        # Stream management
        self._streams: dict[int, LocalRef] = {}
        self._auto_create_streams = True

        # Background tasks
        self._event_task: asyncio.Task | None = None
        self._pending_tasks: set[asyncio.Task] = set()

        # State
        self._closing = False

    @property
    def quic(self) -> "AioQuicConnection":
        """Access underlying QUIC connection."""
        return self._protocol._quic

    async def on_start(self):
        """Start event processing loop."""
        self._event_task = asyncio.create_task(self._event_loop())

    async def on_stop(self):
        """Cleanup on actor stop."""
        await self._shutdown()

    # =========================================================================
    # STATE: ACTIVE (default receive)
    # =========================================================================

    async def receive(self, msg, ctx: Context):
        """Active state - normal operation."""
        match msg:
            case Register(handler, auto_create_streams):
                self._handler = handler
                self._auto_create_streams = auto_create_streams
                ctx.reply(None)  # Acknowledge registration

            case CreateStream(bidirectional, handler):
                await self._create_stream(bidirectional, handler, ctx)

            case SendDatagram(data, ack):
                await self._send_datagram(data, ack, ctx)

            case CloseConnection(error_code, reason):
                ctx.become(self._draining)
                await self._start_close(error_code, reason, ctx)
                ctx.reply(None)  # Acknowledge close request

            # Internal: stream data from event loop
            case _StreamData(stream_id, data, end_stream):
                await self._route_stream_data(stream_id, data, end_stream, ctx)

            case _StreamReset(stream_id, error_code):
                await self._route_stream_reset(stream_id, error_code)

    # =========================================================================
    # STATE: DRAINING
    # =========================================================================

    async def _draining(self, msg, ctx: Context):
        """Draining state - connection closing."""
        match msg:
            case Register(handler, auto_create_streams):
                self._handler = handler
                self._auto_create_streams = auto_create_streams

            case CreateStream() | SendDatagram():
                await self._send_command_failed(
                    msg,
                    ConnectionError("Connection is closing"),
                    ctx,
                )

            case CloseConnection():
                pass  # Already closing

            # Still process stream data during drain
            case _StreamData(stream_id, data, end_stream):
                await self._route_stream_data(stream_id, data, end_stream, ctx)

            case _StreamReset(stream_id, error_code):
                await self._route_stream_reset(stream_id, error_code)

    # =========================================================================
    # STREAM OPERATIONS
    # =========================================================================

    async def _create_stream(
        self,
        bidirectional: bool,
        handler: LocalRef | None,
        ctx: Context,
    ):
        """Create a new QUIC stream."""
        try:
            quic = self.quic
            stream_id = quic.get_next_available_stream_id(
                is_unidirectional=not bidirectional
            )

            stream_handler = handler or self._handler

            # Spawn stream actor
            stream = await ctx.spawn(
                Stream,
                connection=self,
                stream_id=stream_id,
                handler=stream_handler,
                is_unidirectional=not bidirectional,
            )

            self._streams[stream_id] = stream

            event = StreamCreated(
                stream=stream,
                stream_id=stream_id,
                is_unidirectional=not bidirectional,
                initiated_locally=True,
            )

            # Send to handler and reply to caller
            await self._send_event(event, ctx)
            ctx.reply(event)

        except Exception as exc:
            failed = CommandFailed(CreateStream(bidirectional, handler), exc)
            await self._send_event(failed, ctx)
            ctx.reply(failed)

    async def _route_stream_data(
        self,
        stream_id: int,
        data: bytes,
        end_stream: bool,
        ctx: Context,
    ):
        """Route data to appropriate stream actor."""
        if stream_id in self._streams:
            stream = self._streams[stream_id]
            await stream.send(_StreamData(stream_id, data, end_stream))
        elif self._auto_create_streams:
            # Auto-create stream for incoming data
            is_unidirectional = bool(stream_id & 0x2)
            is_client_initiated = not bool(stream_id & 0x1)

            stream = await ctx.spawn(
                Stream,
                connection=self,
                stream_id=stream_id,
                handler=self._handler,
                is_unidirectional=is_unidirectional,
            )

            self._streams[stream_id] = stream

            # Notify handler of new stream
            await self._send_event(
                StreamCreated(
                    stream=stream,
                    stream_id=stream_id,
                    is_unidirectional=is_unidirectional,
                    initiated_locally=False,
                ),
                ctx,
            )

            # Send the data to the new stream
            await stream.send(_StreamData(stream_id, data, end_stream))

    async def _route_stream_reset(self, stream_id: int, error_code: int):
        """Route stream reset to appropriate stream actor."""
        if stream_id in self._streams:
            stream = self._streams[stream_id]
            await stream.send(_StreamReset(stream_id, error_code))

    def remove_stream(self, stream_id: int):
        """Remove stream from tracking (called by stream actor)."""
        self._streams.pop(stream_id, None)

    # =========================================================================
    # DATAGRAM OPERATIONS
    # =========================================================================

    async def _send_datagram(self, data: bytes, ack: any, ctx: Context):
        """Send unreliable datagram."""
        try:
            if not self._options.enable_datagrams:
                raise RuntimeError("Datagrams not enabled in options")

            self.quic.send_datagram_frame(data)
            self._protocol.transmit()

            if ack is not None:
                event = DatagramAck(ack)
                await self._send_event(event, ctx)
                ctx.reply(event)
            else:
                ctx.reply(None)

        except Exception as exc:
            failed = CommandFailed(SendDatagram(data, ack), exc)
            await self._send_event(failed, ctx)
            ctx.reply(failed)

    # =========================================================================
    # CONNECTION LIFECYCLE
    # =========================================================================

    async def _start_close(self, error_code: int, reason: str, ctx: Context):
        """Start graceful connection close."""
        self._closing = True

        try:
            self.quic.close(error_code=error_code, reason_phrase=reason)
            self._protocol.transmit()

        except Exception as exc:
            await self._send_event(ConnectionErrorClosed(exc), ctx)

    async def _event_loop(self):
        """Process QUIC events from the protocol."""
        try:
            while not self._closing:
                # Wait for events
                await asyncio.sleep(0.01)

                # Process all pending events
                while True:
                    event = self.quic.next_event()
                    if event is None:
                        break

                    await self._handle_event(event)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            if not self._closing:
                await self._send_event(ConnectionErrorClosed(exc), self._ctx)

    async def _handle_event(self, event):
        """Handle a QUIC event."""
        if isinstance(event, StreamDataReceived):
            # Route to self for proper actor message handling
            await self._ctx.self_ref.send(
                _StreamData(
                    stream_id=event.stream_id,
                    data=event.data,
                    end_stream=event.end_stream,
                )
            )

        elif isinstance(event, QuicStreamReset):
            await self._ctx.self_ref.send(
                _StreamReset(
                    stream_id=event.stream_id,
                    error_code=event.error_code,
                )
            )

        elif isinstance(event, DatagramFrameReceived):
            await self._send_event(DatagramReceived(event.data), self._ctx)

        elif isinstance(event, QuicHandshakeCompleted):
            tls = self.quic.tls
            await self._send_event(
                HandshakeCompleted(
                    alpn_protocol=tls.alpn_negotiated if tls else None,
                    session_resumed=tls.session_resumed if tls else False,
                    session_ticket=getattr(tls, "session_ticket", None) if tls else None,
                ),
                self._ctx,
            )

        elif isinstance(event, ConnectionTerminated):
            self._closing = True
            await self._send_event(
                ConnectionClosed(
                    error_code=event.error_code,
                    reason=event.reason_phrase or "",
                ),
                self._ctx,
            )

    async def _shutdown(self):
        """Shutdown connection and cleanup."""
        self._closing = True

        # Cancel event loop
        if self._event_task:
            self._event_task.cancel()
            try:
                await self._event_task
            except asyncio.CancelledError:
                pass
            self._event_task = None

        # Cancel pending tasks
        for task in self._pending_tasks:
            if not task.done():
                task.cancel()
        if self._pending_tasks:
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
        self._pending_tasks.clear()

        # Close protocol
        try:
            self._protocol.close()
        except Exception:
            pass

    # =========================================================================
    # HELPERS
    # =========================================================================

    def send_stream_data(self, stream_id: int, data: bytes, end_stream: bool = False):
        """Send data on a stream (called by stream actors)."""
        self.quic.send_stream_data(stream_id, data, end_stream=end_stream)
        self._protocol.transmit()

    def reset_stream(self, stream_id: int, error_code: int):
        """Reset a stream (called by stream actors)."""
        self.quic.reset_stream(stream_id, error_code)
        self._protocol.transmit()

    async def _send_event(self, event, ctx: Context):
        """Send event to handler."""
        if self._handler:
            await self._handler.send(event, sender=ctx.self_ref)

    async def _send_command_failed(self, command, error: Exception, ctx: Context):
        """Send CommandFailed event."""
        await self._send_event(CommandFailed(command, error), ctx)
