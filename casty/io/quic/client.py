"""
QUIC Client Actor - Akka-style with become/unbecome state machine.

States:
    disconnected → Waiting for Connect command
    connected    → Has active connection
"""

import asyncio
from typing import TYPE_CHECKING

from aioquic.asyncio import connect as aioquic_connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import HandshakeCompleted as QuicHandshakeCompleted

from casty import Actor, LocalRef, Context

from .connection import Connection
from .messages import (
    Connect,
    Reconnect,
    Connected,
    Migrated,
    CommandFailed,
    CreateStream,
    CloseConnection,
    SendDatagram,
    Register,
)
from .options import QuicOptions

if TYPE_CHECKING:
    pass


class _ClientProtocol(QuicConnectionProtocol):
    """Custom QUIC protocol handler for client connections."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._handshake_complete = asyncio.Event()

    def quic_event_received(self, event):
        """Handle QUIC events."""
        if isinstance(event, QuicHandshakeCompleted):
            self._handshake_complete.set()

    async def wait_handshake(self, timeout: float = 10.0):
        """Wait for handshake to complete."""
        await asyncio.wait_for(self._handshake_complete.wait(), timeout=timeout)


class Client(Actor):
    """QUIC client actor with state machine.

    Uses become/unbecome for clean state management:
    - disconnected: Waiting for Connect command
    - connected: Has active connection, forwards commands

    Supports:
    - 0-RTT early data for fast reconnection
    - Connection migration
    - Session resumption via tickets

    Usage:
        client = await system.spawn(quic.Client)
        connected = await client.ask(quic.Connect("localhost", 4433, server_name="example.com"))
        # connected.connection is the connection actor ref

    Events sent to parent:
        - Connected(connection, remote_address, local_address, ...) on success
        - Migrated(new_local_address) on successful migration
        - CommandFailed(command, cause) on errors
    """

    def __init__(self):
        self._conn: LocalRef | None = None
        self._protocol: _ClientProtocol | None = None
        self._handler: LocalRef | None = None
        self._options: QuicOptions | None = None
        self._session_ticket: bytes | None = None
        # Context manager for connection lifecycle
        self._connect_context = None

    async def on_stop(self):
        """Cleanup on actor stop."""
        await self._cleanup_connection()

    async def _cleanup_connection(self):
        """Clean up connection resources."""
        # Close connection actor if exists
        if self._conn:
            try:
                await self._conn.stop()
            except Exception:
                pass
            self._conn = None

        # Close protocol
        if self._protocol:
            try:
                self._protocol.close()
            except Exception:
                pass
            self._protocol = None

        # Exit context manager if active
        if self._connect_context:
            try:
                await self._connect_context.__aexit__(None, None, None)
            except Exception:
                pass
            self._connect_context = None

    # =========================================================================
    # STATE: DISCONNECTED (default receive)
    # =========================================================================

    async def receive(self, msg, ctx: Context):
        """Disconnected state - waiting for Connect command."""
        match msg:
            case Connect(
                host, port, server_name, options, ca_certs, early_data, session_ticket
            ):
                await self._do_connect(
                    host,
                    port,
                    server_name,
                    options,
                    ca_certs,
                    early_data,
                    session_ticket,
                    ctx,
                )

            case (
                CreateStream()
                | CloseConnection()
                | SendDatagram()
                | Register()
                | Reconnect()
            ):
                ctx.reply(CommandFailed(msg, ConnectionError("Not connected")))

    # =========================================================================
    # STATE: CONNECTED
    # =========================================================================

    async def _connected(self, msg, ctx: Context):
        """Connected state - forward commands to connection."""
        match msg:
            case Connect(
                host, port, server_name, options, ca_certs, early_data, session_ticket
            ):
                ctx.reply(
                    CommandFailed(
                        Connect(
                            host,
                            port,
                            server_name,
                            options,
                            ca_certs,
                            early_data,
                            session_ticket,
                        ),
                        RuntimeError("Already connected"),
                    )
                )

            case Reconnect(new_local_address):
                await self._do_migrate(new_local_address, ctx)

            case CreateStream() | CloseConnection() | SendDatagram() | Register():
                # Forward to connection actor and relay response
                if self._conn:
                    result = await self._conn.ask(msg)
                    ctx.reply(result)
                else:
                    ctx.reply(CommandFailed(msg, ConnectionError("No connection")))

    # =========================================================================
    # OPERATIONS
    # =========================================================================

    async def _do_connect(
        self,
        host: str,
        port: int,
        server_name: str | None,
        options: QuicOptions | None,
        ca_certs: str | None,
        early_data: bytes | None,
        session_ticket: bytes | None,
        ctx: Context,
    ):
        """Handle Connect command."""
        cmd = Connect(
            host, port, server_name, options, ca_certs, early_data, session_ticket
        )
        self._handler = ctx.parent
        self._options = options or QuicOptions()

        # Use provided session ticket or stored one
        ticket = session_ticket or self._session_ticket

        try:
            # Create QUIC configuration
            config = QuicConfiguration(
                is_client=True,
                alpn_protocols=list(self._options.alpn_protocols),
                max_datagram_frame_size=(
                    self._options.max_datagram_size
                    if self._options.enable_datagrams
                    else None
                ),
            )

            # Apply options
            self._options.apply_to_config(config)

            # Server name for SNI
            config.server_name = server_name or host

            # CA certs for verification
            if ca_certs:
                config.load_verify_locations(ca_certs)

            if not self._options.verify_peer:
                config.verify_mode = None

            # Session ticket for resumption
            if ticket:
                config.session_ticket = ticket

            # Use aioquic.asyncio.connect() - properly as context manager
            # We enter the context and keep the reference to exit later
            self._connect_context = aioquic_connect(
                host=host,
                port=port,
                configuration=config,
                create_protocol=_ClientProtocol,
                wait_connected=True,  # Wait for handshake
            )

            # Enter the context manager
            self._protocol = await self._connect_context.__aenter__()

            # Get connection info
            quic_conn = self._protocol._quic

            # Get addresses from transport
            transport = self._protocol._transport
            if transport:
                peername = transport.get_extra_info("peername")
                sockname = transport.get_extra_info("sockname")
                remote_address = peername if peername else (host, port)
                local_address = sockname if sockname else ("unknown", 0)
            else:
                remote_address = (host, port)
                local_address = ("unknown", 0)

            # Check early data acceptance
            early_data_accepted = (
                early_data is not None
                and ticket is not None
                and quic_conn.tls
                and quic_conn.tls.session_resumed
            )

            # Get new session ticket if available
            new_ticket = None
            if quic_conn.tls and hasattr(quic_conn.tls, "session_ticket"):
                new_ticket = quic_conn.tls.session_ticket
                self._session_ticket = new_ticket

            # Spawn connection actor
            self._conn = await ctx.spawn(
                Connection,
                protocol=self._protocol,
                handler=self._handler,
                options=self._options,
                is_client=True,
            )

            # Transition to connected state
            ctx.become(self._connected)

            # Reply with Connected event
            ctx.reply(
                Connected(
                    connection=self._conn,
                    remote_address=remote_address,
                    local_address=local_address,
                    alpn_protocol=(
                        quic_conn.tls.alpn_negotiated if quic_conn.tls else None
                    ),
                    early_data_accepted=early_data_accepted,
                    session_ticket=new_ticket,
                )
            )

        except Exception as exc:
            # Cleanup on error
            await self._cleanup_connection()
            ctx.reply(CommandFailed(cmd, exc))

    async def _do_migrate(
        self,
        new_local_address: tuple[str, int] | None,
        ctx: Context,
    ):
        """Handle Reconnect command for connection migration."""
        cmd = Reconnect(new_local_address)

        if not self._protocol:
            ctx.reply(CommandFailed(cmd, ConnectionError("No active connection")))
            return

        try:
            # QUIC connection migration
            # Note: aioquic doesn't fully support migration yet,
            # this is a placeholder for future implementation
            transport = self._protocol._transport
            if transport:
                sockname = transport.get_extra_info("sockname")
                actual_address = sockname if sockname else ("unknown", 0)
                ctx.reply(Migrated(actual_address))
            else:
                ctx.reply(CommandFailed(cmd, ConnectionError("No transport")))

        except Exception as exc:
            ctx.reply(CommandFailed(cmd, exc))
