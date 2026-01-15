"""
TCP Client Actor - Akka-style with become/unbecome state machine.

States:
    disconnected → Waiting for Connect command
    connected    → Has active connection
"""

import asyncio

from casty import Actor, LocalRef, Context

from .connection import Connection
from .messages import (
    Connect,
    Connected,
    CommandFailed,
    Write,
    WriteFile,
    Close,
    ConfirmedClose,
    Abort,
    SuspendReading,
    ResumeReading,
    ResumeWriting,
)
from .options import SocketOptions, TLSConfig


class Client(Actor):
    """TCP client actor with state machine.

    Uses become/unbecome for clean state management:
    - disconnected: Waiting for Connect command
    - connected: Has active connection, forwards commands

    Usage:
        client = await system.spawn(tcp.Client)
        await client.send(tcp.Connect("localhost", 8080))

    Events sent to parent:
        - Connected(connection, remote_address, local_address) on success
        - CommandFailed(command, cause) on errors

    After connection, you can send Write/Close/etc directly to this actor
    and they will be forwarded to the connection.
    """

    def __init__(self):
        self._conn: LocalRef | None = None
        self._handler: LocalRef | None = None

    # =========================================================================
    # STATE: DISCONNECTED (default receive)
    # =========================================================================

    async def receive(self, msg, ctx: Context):
        """Disconnected state - waiting for Connect command."""
        match msg:
            case Connect(host, port, options, tls):
                await self._do_connect(host, port, options, tls, ctx)

            case Write() | WriteFile() | Close() | ConfirmedClose() | Abort():
                await self._send_command_failed(
                    msg,
                    ConnectionError("Not connected"),
                    ctx,
                )

            case SuspendReading() | ResumeReading() | ResumeWriting():
                pass  # Ignore when disconnected

    # =========================================================================
    # STATE: CONNECTED
    # =========================================================================

    async def _connected(self, msg, ctx: Context):
        """Connected state - forward commands to connection."""
        match msg:
            case Connect(host, port, options, tls):
                # Already connected - send error
                await self._send_command_failed(
                    Connect(host, port, options, tls),
                    RuntimeError("Already connected"),
                    ctx,
                )

            case Write() | WriteFile() | Close() | ConfirmedClose() | Abort() | SuspendReading() | ResumeReading() | ResumeWriting():
                # Forward to connection
                if self._conn:
                    await self._conn.send(msg)

    # =========================================================================
    # OPERATIONS
    # =========================================================================

    async def _do_connect(
        self,
        host: str,
        port: int,
        options: SocketOptions | None,
        tls: TLSConfig | None,
        ctx: Context,
    ):
        """Handle Connect command."""
        cmd = Connect(host, port, options, tls)
        self._handler = ctx.parent
        opts = options or SocketOptions()

        # Setup TLS if configured
        ssl_context = None
        if tls:
            try:
                ssl_context = tls.create_context()
            except Exception as exc:
                await self._send_command_failed(cmd, exc, ctx)
                return

        try:
            # Connect with timeout
            coro = asyncio.open_connection(
                host,
                port,
                ssl=ssl_context,
                server_hostname=tls.server_hostname if tls else None,
            )

            if opts.connect_timeout:
                reader, writer = await asyncio.wait_for(
                    coro, timeout=opts.connect_timeout
                )
            else:
                reader, writer = await coro

            # Apply socket options
            sock = writer.get_extra_info("socket")
            if sock:
                opts.apply(sock)

            # Get addresses
            peername = writer.get_extra_info("peername")
            sockname = writer.get_extra_info("sockname")
            remote_address = peername if peername else (host, port)
            local_address = sockname if sockname else ("unknown", 0)

            # Spawn connection actor
            self._conn = await ctx.spawn(
                Connection,
                reader=reader,
                writer=writer,
                handler=self._handler,
                options=opts,
            )

            # Transition to connected state
            ctx.become(self._connected)

            # Notify handler
            await self._send_event(
                Connected(self._conn, remote_address, local_address),
                ctx,
            )

        except asyncio.TimeoutError:
            await self._send_command_failed(
                cmd,
                TimeoutError(f"Connection to {host}:{port} timed out"),
                ctx,
            )
        except Exception as exc:
            await self._send_command_failed(cmd, exc, ctx)

    async def _send_event(self, event, ctx: Context):
        """Send event to handler (parent)."""
        if self._handler:
            await self._handler.send(event, sender=ctx.self_ref)

    async def _send_command_failed(self, command, error: Exception, ctx: Context):
        """Send CommandFailed event."""
        await self._send_event(CommandFailed(command, error), ctx)
