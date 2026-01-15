"""
TCP Server Actor - Akka-style with become/unbecome state machine.

States:
    unbound → Waiting for Bind command
    bound   → Accepting connections
"""

import asyncio
import ssl
from asyncio import StreamReader, StreamWriter

from casty import Actor, LocalRef, Context

from .connection import Connection
from .messages import (
    Bind,
    Unbind,
    Bound,
    Unbound,
    Accepted,
    CommandFailed,
)
from .options import SocketOptions, TLSConfig


class Server(Actor):
    """TCP server actor with state machine.

    Uses become/unbecome for clean state management:
    - unbound: Waiting for Bind command
    - bound: Actively accepting connections

    Usage:
        server = await system.spawn(tcp.Server)
        await server.send(tcp.Bind("0.0.0.0", 8080))

    Events sent to parent:
        - Bound(host, port) on successful bind
        - Accepted(connection, remote_address) for each new connection
        - CommandFailed(command, cause) on errors
        - Unbound() when server stops
    """

    def __init__(self):
        self._server: asyncio.AbstractServer | None = None
        self._handler: LocalRef | None = None
        self._options: SocketOptions | None = None
        self._tls_context: ssl.SSLContext | None = None

    async def on_stop(self):
        await self._close_server()

    # =========================================================================
    # STATE: UNBOUND (default receive)
    # =========================================================================

    async def receive(self, msg, ctx: Context):
        """Unbound state - waiting for Bind command."""
        match msg:
            case Bind(host, port, options, backlog, tls):
                await self._do_bind(host, port, options, backlog, tls, ctx)

            case Unbind():
                pass  # Not bound, nothing to do

    # =========================================================================
    # STATE: BOUND
    # =========================================================================

    async def _bound(self, msg, ctx: Context):
        """Bound state - accepting connections."""
        match msg:
            case Bind(host, port, options, backlog, tls):
                # Already bound - send error
                await self._send_command_failed(
                    Bind(host, port, options, backlog, tls),
                    RuntimeError("Server already bound"),
                    ctx,
                )

            case Unbind():
                await self._do_unbind(ctx)

    # =========================================================================
    # OPERATIONS
    # =========================================================================

    async def _do_bind(
        self,
        host: str,
        port: int,
        options: SocketOptions | None,
        backlog: int,
        tls: TLSConfig | None,
        ctx: Context,
    ):
        """Handle Bind command."""
        cmd = Bind(host, port, options, backlog, tls)
        self._handler = ctx.parent
        self._options = options or SocketOptions()

        # Setup TLS if configured
        if tls:
            try:
                self._tls_context = tls.create_context()
            except Exception as exc:
                await self._send_command_failed(cmd, exc, ctx)
                return

        try:
            self._server = await asyncio.start_server(
                lambda r, w: self._handle_client(r, w, ctx),
                host,
                port,
                backlog=backlog,
                reuse_address=self._options.reuse_address,
                ssl=self._tls_context,
            )

            # Apply socket options
            for sock in self._server.sockets:
                self._options.apply(sock)

            # Transition to bound state
            ctx.become(self._bound)

            # Get actual bound address (useful when port=0)
            addr = self._server.sockets[0].getsockname()
            await self._send_event(Bound(addr[0], addr[1]), ctx)

        except Exception as exc:
            await self._send_command_failed(cmd, exc, ctx)

    async def _do_unbind(self, ctx: Context):
        """Handle Unbind command."""
        await self._close_server()

        # Transition back to unbound
        ctx.unbecome()

        if self._handler:
            await self._handler.send(Unbound(), sender=self._ctx.self_ref)

    async def _handle_client(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        ctx: Context,
    ):
        """Handle new client connection."""
        if not self._handler:
            writer.close()
            await writer.wait_closed()
            return

        try:
            peername = writer.get_extra_info("peername")
            remote_address = peername if peername else ("unknown", 0)

            conn = await ctx.spawn(
                Connection,
                reader=reader,
                writer=writer,
                handler=self._handler,
                options=self._options,
            )

            await self._handler.send(
                Accepted(conn, remote_address),
                sender=self._ctx.self_ref,
            )

        except Exception:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _close_server(self):
        """Close the server socket."""
        if self._server:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None

    async def _send_event(self, event, ctx: Context):
        """Send event to handler (parent)."""
        if self._handler:
            await self._handler.send(event, sender=ctx.self_ref)

    async def _send_command_failed(self, command, error: Exception, ctx: Context):
        """Send CommandFailed event."""
        await self._send_event(CommandFailed(command, error), ctx)
