"""
TCP Connection Actor - Akka-style with become/unbecome state machine.

States:
    active     → Normal operation, reading and writing
    suspended  → Reading paused (backpressure), writes still allowed
    closing    → Close requested, flushing pending writes
    half_closed → Peer closed their side, we can still write
"""

import asyncio
import os
import struct
from asyncio import StreamReader, StreamWriter
from collections import deque
from typing import Any

from casty import Actor, LocalRef, Context

from .messages import (
    Register,
    Write,
    WriteFile,
    Close,
    ConfirmedClose,
    Abort,
    SuspendReading,
    ResumeReading,
    ResumeWriting,
    Received,
    WriteAck,
    WritingResumed,
    Closed,
    ConfirmedClosed,
    Aborted,
    PeerClosed,
    ErrorClosed,
    CommandFailed,
)
from .options import SocketOptions


class Connection(Actor):
    """TCP connection actor with Akka-style state machine.

    Uses become/unbecome for clean state transitions:
    - active: Normal read/write operations
    - suspended: Backpressure - reading paused
    - closing: Graceful shutdown in progress
    - half_closed: Peer closed, but we can still write
    """

    def __init__(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        handler: LocalRef,
        options: SocketOptions | None = None,
    ):
        self._reader = reader
        self._writer = writer
        self._handler = handler
        self._options = options or SocketOptions()

        # Settings (can be changed via Register)
        self._keep_open_on_peer_closed = False
        self._use_resume_writing = False

        # Background tasks
        self._read_task: asyncio.Task | None = None
        self._idle_task: asyncio.Task | None = None

        # Write queue for backpressure
        self._write_queue: deque[tuple[bytes, Any]] = deque()
        self._writing_suspended = False

        # Activity tracking for idle timeout
        self._last_activity: float = 0

        # Internal state flags for read loop coordination
        self._reading_paused = False
        self._shutdown_requested = False

    @property
    def remote_address(self) -> tuple[str, int]:
        """Get remote peer address."""
        peername = self._writer.get_extra_info("peername")
        return peername if peername else ("unknown", 0)

    @property
    def local_address(self) -> tuple[str, int]:
        """Get local socket address."""
        sockname = self._writer.get_extra_info("sockname")
        return sockname if sockname else ("unknown", 0)

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    async def on_start(self):
        self._last_activity = asyncio.get_event_loop().time()
        self._read_task = asyncio.create_task(self._read_loop())

        if self._options.idle_timeout:
            self._idle_task = asyncio.create_task(self._idle_monitor())

    async def on_stop(self):
        await self._do_shutdown(send_event=False)

    # =========================================================================
    # STATE: ACTIVE (default receive)
    # =========================================================================

    async def receive(self, msg, ctx: Context):
        """Active state - normal read/write operations."""
        match msg:
            case Register(handler, keep_open, use_resume):
                self._handler = handler
                self._keep_open_on_peer_closed = keep_open
                self._use_resume_writing = use_resume

            case Write(data, ack):
                await self._do_write(data, ack)

            case WriteFile(path, position, count, ack):
                await self._do_write_file(path, position, count, ack)

            case SuspendReading():
                self._reading_paused = True
                ctx.become(self._suspended)

            case ResumeReading():
                pass  # Already active

            case ResumeWriting():
                await self._do_resume_writing()

            case Close():
                ctx.become(self._closing)
                await self._start_graceful_close()

            case ConfirmedClose():
                ctx.become(self._closing)
                await self._start_confirmed_close()

            case Abort():
                await self._do_abort()

    # =========================================================================
    # STATE: SUSPENDED (backpressure)
    # =========================================================================

    async def _suspended(self, msg, ctx: Context):
        """Suspended state - reading paused, writes still allowed."""
        match msg:
            case Register(handler, keep_open, use_resume):
                self._handler = handler
                self._keep_open_on_peer_closed = keep_open
                self._use_resume_writing = use_resume

            case Write(data, ack):
                await self._do_write(data, ack)

            case WriteFile(path, position, count, ack):
                await self._do_write_file(path, position, count, ack)

            case SuspendReading():
                pass  # Already suspended

            case ResumeReading():
                self._reading_paused = False
                ctx.unbecome()  # Back to active

            case ResumeWriting():
                await self._do_resume_writing()

            case Close():
                ctx.become(self._closing, discard_old=True)
                await self._start_graceful_close()

            case ConfirmedClose():
                ctx.become(self._closing, discard_old=True)
                await self._start_confirmed_close()

            case Abort():
                await self._do_abort()

    # =========================================================================
    # STATE: CLOSING (graceful shutdown)
    # =========================================================================

    async def _closing(self, msg, ctx: Context):
        """Closing state - rejecting new operations, waiting for shutdown."""
        match msg:
            case Write(data, ack):
                await self._send_command_failed(
                    Write(data, ack),
                    ConnectionError("Connection is closing"),
                )

            case WriteFile(path, position, count, ack):
                await self._send_command_failed(
                    WriteFile(path, position, count, ack),
                    ConnectionError("Connection is closing"),
                )

            case SuspendReading() | ResumeReading() | ResumeWriting():
                pass  # Ignore during close

            case Close() | ConfirmedClose():
                pass  # Already closing

            case Abort():
                await self._do_abort()

            case Register():
                pass  # Ignore during close

    # =========================================================================
    # STATE: HALF-CLOSED (peer closed, we can still write)
    # =========================================================================

    async def _half_closed(self, msg, ctx: Context):
        """Half-closed state - peer closed, but we can still write."""
        match msg:
            case Register(handler, keep_open, use_resume):
                self._handler = handler
                self._keep_open_on_peer_closed = keep_open
                self._use_resume_writing = use_resume

            case Write(data, ack):
                await self._do_write(data, ack)

            case WriteFile(path, position, count, ack):
                await self._do_write_file(path, position, count, ack)

            case ResumeWriting():
                await self._do_resume_writing()

            case SuspendReading() | ResumeReading():
                pass  # No reading in half-closed

            case Close():
                ctx.become(self._closing, discard_old=True)
                await self._start_graceful_close()

            case ConfirmedClose():
                # Peer already closed, just do normal close
                ctx.become(self._closing, discard_old=True)
                await self._start_graceful_close()

            case Abort():
                await self._do_abort()

    # =========================================================================
    # READ LOOP (runs in background)
    # =========================================================================

    async def _read_loop(self):
        """Background read loop with backpressure support."""
        try:
            while not self._shutdown_requested:
                # Honor backpressure
                while self._reading_paused and not self._shutdown_requested:
                    await asyncio.sleep(0.01)

                if self._shutdown_requested:
                    break

                try:
                    data = await asyncio.wait_for(
                        self._reader.read(65536),
                        timeout=0.1,
                    )
                except asyncio.TimeoutError:
                    continue

                if not data:
                    # EOF - peer closed their side
                    await self._handle_peer_closed()
                    break

                self._touch_activity()
                await self._send_event(Received(data))

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            if not self._shutdown_requested:
                await self._send_event(ErrorClosed(exc))
                await self._do_shutdown(send_event=False)

    async def _handle_peer_closed(self):
        """Handle peer closing their side."""
        await self._send_event(PeerClosed())

        if self._keep_open_on_peer_closed:
            # Transition to half-closed state
            self._ctx.become(self._half_closed, discard_old=True)
        else:
            await self._do_shutdown(send_event=False)

    # =========================================================================
    # WRITE OPERATIONS
    # =========================================================================

    async def _do_write(self, data: bytes, ack: Any):
        """Perform a write operation."""
        if self._writing_suspended:
            self._write_queue.append((data, ack))
            return

        try:
            self._writer.write(data)
            await self._writer.drain()
            self._touch_activity()

            if ack is not None:
                await self._send_event(WriteAck(ack))

        except Exception as exc:
            if self._use_resume_writing:
                self._writing_suspended = True
                self._write_queue.appendleft((data, ack))
            else:
                await self._send_command_failed(Write(data, ack), exc)

    async def _do_write_file(
        self, path: str, position: int, count: int | None, ack: Any
    ):
        """Send a file efficiently."""
        cmd = WriteFile(path, position, count, ack)

        try:
            file_size = os.path.getsize(path)
            remaining = count if count is not None else (file_size - position)

            with open(path, "rb") as f:
                f.seek(position)

                while remaining > 0 and not self._shutdown_requested:
                    chunk_size = min(remaining, 65536)
                    data = f.read(chunk_size)
                    if not data:
                        break

                    self._writer.write(data)
                    await self._writer.drain()
                    remaining -= len(data)

            self._touch_activity()

            if ack is not None:
                await self._send_event(WriteAck(ack))

        except Exception as exc:
            await self._send_command_failed(cmd, exc)

    async def _do_resume_writing(self):
        """Resume writing after suspension."""
        if not self._writing_suspended:
            return

        self._writing_suspended = False
        await self._send_event(WritingResumed())

        # Flush queued writes
        while self._write_queue and not self._writing_suspended:
            data, ack = self._write_queue.popleft()
            try:
                self._writer.write(data)
                await self._writer.drain()
                if ack is not None:
                    await self._send_event(WriteAck(ack))
            except Exception:
                self._writing_suspended = True
                self._write_queue.appendleft((data, ack))
                break

    # =========================================================================
    # CLOSE OPERATIONS
    # =========================================================================

    async def _start_graceful_close(self):
        """Start graceful close - flush writes then close."""
        await self._flush_write_queue()
        await self._send_event(Closed())
        await self._do_shutdown(send_event=False)

    async def _start_confirmed_close(self):
        """Start confirmed close - wait for peer to close."""
        await self._flush_write_queue()

        try:
            if self._writer.can_write_eof():
                self._writer.write_eof()
                await self._writer.drain()

            # Wait for peer to close (up to 30 seconds)
            for _ in range(300):
                if self._shutdown_requested:
                    break
                # Check if read loop detected EOF
                await asyncio.sleep(0.1)

            await self._send_event(ConfirmedClosed())

        except Exception:
            await self._send_event(Closed())

        await self._do_shutdown(send_event=False)

    async def _do_abort(self):
        """Immediate close with RST."""
        # Set SO_LINGER to 0 for RST
        try:
            sock = self._writer.get_extra_info("socket")
            if sock:
                import socket
                sock.setsockopt(
                    socket.SOL_SOCKET,
                    socket.SO_LINGER,
                    struct.pack("ii", 1, 0),
                )
        except Exception:
            pass

        await self._send_event(Aborted())
        await self._do_shutdown(send_event=False)

    async def _flush_write_queue(self):
        """Flush all pending writes."""
        while self._write_queue:
            data, ack = self._write_queue.popleft()
            try:
                self._writer.write(data)
                await self._writer.drain()
                if ack is not None:
                    await self._send_event(WriteAck(ack))
            except Exception:
                break

    # =========================================================================
    # IDLE TIMEOUT
    # =========================================================================

    def _touch_activity(self):
        """Record activity for idle timeout."""
        self._last_activity = asyncio.get_event_loop().time()

    async def _idle_monitor(self):
        """Monitor for idle timeout."""
        timeout = self._options.idle_timeout
        if not timeout:
            return

        try:
            while not self._shutdown_requested:
                await asyncio.sleep(1.0)
                elapsed = asyncio.get_event_loop().time() - self._last_activity

                if elapsed >= timeout:
                    await self._send_event(
                        ErrorClosed(TimeoutError(f"Idle timeout ({timeout}s)"))
                    )
                    await self._do_shutdown(send_event=False)
                    break

        except asyncio.CancelledError:
            pass

    # =========================================================================
    # HELPERS
    # =========================================================================

    async def _send_event(self, event):
        """Send event to handler."""
        if self._handler:
            await self._handler.send(event, sender=self._ctx.self_ref)

    async def _send_command_failed(self, command: Any, error: Exception):
        """Send CommandFailed event."""
        await self._send_event(CommandFailed(command, error))

    async def _do_shutdown(self, send_event: bool = True):
        """Shutdown the connection."""
        if self._shutdown_requested:
            return

        self._shutdown_requested = True

        # Cancel read task
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
            self._read_task = None

        # Cancel idle task
        if self._idle_task:
            self._idle_task.cancel()
            try:
                await self._idle_task
            except asyncio.CancelledError:
                pass
            self._idle_task = None

        # Close writer
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass

        if send_event:
            await self._send_event(Closed())
