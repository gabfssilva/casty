"""
QUIC Server Actor - Akka-style with become/unbecome state machine.

States:
    unbound → Waiting for Bind command
    bound   → Accepting connections
"""

import asyncio
from typing import TYPE_CHECKING

from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import HandshakeCompleted as QuicHandshakeCompleted

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
from .options import QuicOptions, TlsCertificateConfig

if TYPE_CHECKING:
    from aioquic.asyncio.server import QuicServer as AioQuicServer


class _ServerConnectionProtocol(QuicConnectionProtocol):
    """Custom QUIC protocol handler for server connections."""

    def __init__(self, *args, server_actor: "Server | None" = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._server_actor = server_actor
        self._handshake_notified = False

    def quic_event_received(self, event):
        """Handle QUIC events."""
        if isinstance(event, QuicHandshakeCompleted) and not self._handshake_notified:
            self._handshake_notified = True
            if self._server_actor:
                self._server_actor._on_handshake_completed(self)


class Server(Actor):
    """QUIC server actor with state machine.

    Uses become/unbecome for clean state management:
    - unbound: Waiting for Bind command
    - bound: Actively accepting connections

    Usage:
        server = await system.spawn(quic.Server)
        bound = await server.ask(quic.Bind("0.0.0.0", 4433, certificate=cert_config))

    Events sent to parent:
        - Bound(host, port) on successful bind (also returned via ask)
        - Accepted(connection, remote_address) for each new connection
        - CommandFailed(command, cause) on errors
        - Unbound() when server stops
    """

    def __init__(self):
        self._server: "AioQuicServer | None" = None
        self._handler: LocalRef | None = None
        self._options: QuicOptions | None = None
        self._config: QuicConfiguration | None = None
        self._pending_tasks: set[asyncio.Task] = set()
        self._bound_host: str = ""
        self._bound_port: int = 0

    async def on_stop(self):
        await self._close_server()

    # =========================================================================
    # STATE: UNBOUND (default receive)
    # =========================================================================

    async def receive(self, msg, ctx: Context):
        """Unbound state - waiting for Bind command."""
        match msg:
            case Bind(host, port, certificate, options):
                await self._do_bind(host, port, certificate, options, ctx)

            case Unbind():
                ctx.reply(None)  # Not bound, nothing to do

    # =========================================================================
    # STATE: BOUND
    # =========================================================================

    async def _bound(self, msg, ctx: Context):
        """Bound state - accepting connections."""
        match msg:
            case Bind(host, port, certificate, options):
                ctx.reply(
                    CommandFailed(
                        Bind(host, port, certificate, options),
                        RuntimeError("Server already bound"),
                    )
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
        certificate: TlsCertificateConfig,
        options: QuicOptions | None,
        ctx: Context,
    ):
        """Handle Bind command."""
        cmd = Bind(host, port, certificate, options)
        self._handler = ctx.parent
        self._options = options or QuicOptions()

        try:
            # Create QUIC configuration
            self._config = QuicConfiguration(
                is_client=False,
                max_datagram_frame_size=(
                    self._options.max_datagram_size
                    if self._options.enable_datagrams
                    else None
                ),
            )

            # Load certificate
            certificate.load_into_config(self._config)

            # Apply options
            self._options.apply_to_config(self._config)

            # Capture self for protocol factory
            server_actor = self

            def create_protocol(*args, **kwargs):
                return _ServerConnectionProtocol(
                    *args, server_actor=server_actor, **kwargs
                )

            # Use aioquic.asyncio.serve() - the proper way
            self._server = await serve(
                host=host,
                port=port,
                configuration=self._config,
                create_protocol=create_protocol,
            )

            # Get actual bound address
            if self._server._transport:
                sockname = self._server._transport.get_extra_info("sockname")
                if sockname:
                    self._bound_host = sockname[0]
                    self._bound_port = sockname[1]
                else:
                    self._bound_host = host
                    self._bound_port = port
            else:
                self._bound_host = host
                self._bound_port = port

            # Transition to bound state
            ctx.become(self._bound)

            # Reply to caller (for ask pattern)
            ctx.reply(Bound(self._bound_host, self._bound_port))

        except Exception as exc:
            ctx.reply(CommandFailed(cmd, exc))

    async def _do_unbind(self, ctx: Context):
        """Handle Unbind command."""
        await self._close_server()

        # Transition back to unbound
        ctx.unbecome()

        # Notify handler
        if self._handler:
            await self._handler.send(Unbound(), sender=self._ctx.self_ref)

        ctx.reply(None)

    def _on_handshake_completed(self, protocol: _ServerConnectionProtocol):
        """Called when a client completes handshake (from protocol callback)."""
        task = asyncio.create_task(self._handle_connection(protocol))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _handle_connection(
        self,
        protocol: _ServerConnectionProtocol,
    ):
        """Handle new QUIC connection."""
        if not self._handler:
            protocol.close()
            return

        try:
            quic_conn = protocol._quic
            peername = (
                protocol._transport.get_extra_info("peername")
                if protocol._transport
                else None
            )
            remote_address = peername if peername else ("unknown", 0)

            # Get server name from handshake
            server_name = (
                quic_conn.tls.server_name if quic_conn and quic_conn.tls else None
            )

            # Spawn connection actor
            conn = await self._ctx.spawn(
                Connection,
                protocol=protocol,
                handler=self._handler,
                options=self._options,
                is_client=False,
            )

            await self._handler.send(
                Accepted(
                    connection=conn,
                    remote_address=remote_address,
                    server_name=server_name,
                ),
                sender=self._ctx.self_ref,
            )

        except Exception:
            try:
                protocol.close()
            except Exception:
                pass

    async def _close_server(self):
        """Close the server."""
        # Cancel pending tasks
        for task in self._pending_tasks:
            if not task.done():
                task.cancel()
        if self._pending_tasks:
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
        self._pending_tasks.clear()

        # Close server
        if self._server:
            self._server.close()
            self._server = None
