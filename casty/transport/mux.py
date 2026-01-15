"""
TransportMux - Protocol multiplexing over TCP.

Multiplexes multiple protocols over a single TCP port using a simple
wire format: [length: 4B][protocol: 1B][payload: NB]
"""

import logging
import struct
from dataclasses import dataclass, field
from typing import Any

import msgpack

from casty import Actor, LocalRef, Context
from casty.io import tcp

from .messages import (
    ProtocolType,
    RegisterHandler,
    ConnectTo,
    ConnectToAddress,
    Disconnect,
    Send,
    Broadcast,
    Listening,
    PeerConnected,
    PeerDisconnected,
    Incoming,
)

log = logging.getLogger(__name__)


# =============================================================================
# Internal State
# =============================================================================


@dataclass
class ConnectionState:
    """State of an active connection."""
    node_id: str
    address: tuple[str, int]
    connection: LocalRef
    recv_buffer: bytearray = field(default_factory=bytearray)


@dataclass
class PendingConnection:
    """State of a connection in handshake phase."""
    connection: LocalRef
    address: tuple[str, int]
    recv_buffer: bytearray = field(default_factory=bytearray)
    is_outgoing: bool = False
    target_node_id: str | None = None  # Known for outgoing connections


# =============================================================================
# Internal Handshake Messages
# =============================================================================


@dataclass(frozen=True, slots=True)
class _Handshake:
    """Internal: Node handshake request."""
    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class _HandshakeAck:
    """Internal: Node handshake acknowledgment."""
    node_id: str
    address: tuple[str, int]
    accepted: bool = True


# =============================================================================
# TransportMux Actor
# =============================================================================


class TransportMux(Actor):
    """Multiplexes multiple protocols over a single TCP port.

    Wire format:
        [length: 4 bytes BE][protocol: 1 byte][payload: N bytes]

    The protocol byte determines which registered handler receives the payload.
    Handshake protocol (0x03) is handled internally to identify peer node_id.

    TransportMux is protocol-agnostic - it doesn't know anything about SWIM,
    Gossip, or Actor protocols. It just routes based on the protocol byte.

    Example:
        mux = await ctx.spawn(
            TransportMux,
            node_id="node-1",
            bind_address=("0.0.0.0", 7946),
        )

        # Register protocol handlers
        await mux.send(RegisterHandler(ProtocolType.SWIM, swim_ref))
        await mux.send(RegisterHandler(ProtocolType.ACTOR, cluster_ref))
        await mux.send(RegisterHandler(ProtocolType.GOSSIP, gossip_ref))

        # Connect to peer
        await mux.send(ConnectTo("node-2", ("192.168.1.10", 7946)))

        # Send data (from handler)
        await mux.send(Send(ProtocolType.SWIM, "node-2", swim_bytes))
    """

    HEADER_SIZE = 5  # 4 bytes length + 1 byte protocol
    MAX_MESSAGE_SIZE = 64 * 1024

    def __init__(
        self,
        node_id: str,
        bind_address: tuple[str, int] = ("0.0.0.0", 7946),
        advertise_address: tuple[str, int] | None = None,
    ):
        self._node_id = node_id
        self._bind_address = bind_address
        self._advertise_address = advertise_address

        # TCP server
        self._server: LocalRef | None = None
        self._listening_address: tuple[str, int] | None = None

        # Protocol handlers: protocol -> handler ref
        self._handlers: dict[ProtocolType, LocalRef] = {}

        # Active connections: node_id -> state
        self._connections: dict[str, ConnectionState] = {}

        # Pending connections (handshake in progress)
        self._pending: dict[LocalRef, PendingConnection] = {}

        # Reverse lookup: connection ref -> node_id
        self._connection_to_node: dict[LocalRef, str] = {}

    @property
    def node_id(self) -> str:
        """Return this node's ID."""
        return self._node_id

    @property
    def listening_address(self) -> tuple[str, int] | None:
        """Return the address we're listening on."""
        return self._listening_address

    @property
    def announced_address(self) -> tuple[str, int] | None:
        """Return the address announced to other nodes.

        Uses advertise_address if set, otherwise listening_address or bind_address.
        """
        if self._advertise_address:
            return self._advertise_address
        return self._listening_address or self._bind_address

    @property
    def connected_nodes(self) -> list[str]:
        """Return list of connected node IDs."""
        return list(self._connections.keys())

    async def on_start(self) -> None:
        """Start TCP server."""
        self._server = await self._ctx.spawn(tcp.Server)
        await self._server.send(tcp.Bind(
            host=self._bind_address[0],
            port=self._bind_address[1],
        ))

    async def on_stop(self) -> None:
        """Clean up connections."""
        for conn in self._connections.values():
            await conn.connection.send(tcp.Close())

    async def receive(self, msg: Any, ctx: Context) -> None:
        match msg:
            # ----- TCP Events -----
            case tcp.Bound(host, port):
                self._listening_address = (host, port)
                log.info(f"[{self._node_id}] TransportMux listening on {host}:{port}")
                # Notify parent
                if ctx.parent:
                    await ctx.parent.send(
                        Listening((host, port)),
                        sender=ctx.self_ref,
                    )

            case tcp.Accepted(connection, remote_address):
                await self._handle_accepted(connection, remote_address, ctx)

            case tcp.Connected(connection, remote_address, _):
                await self._handle_connected(connection, remote_address, ctx)

            case tcp.Received(data):
                await self._handle_received(data, ctx)

            case tcp.Closed() | tcp.PeerClosed() | tcp.ErrorClosed():
                await self._handle_closed(ctx)

            case tcp.CommandFailed(cmd, cause):
                log.error(f"[{self._node_id}] TCP command failed: {cmd} - {cause}")
                await self._handle_command_failed(cmd, ctx)

            # ----- Commands -----
            case RegisterHandler(protocol, handler):
                self._handlers[protocol] = handler
                log.info(f"[{self._node_id}] Registered handler for {protocol.name}: {handler}")

            case ConnectTo(node_id, address):
                await self._connect_to(node_id, address, ctx)

            case ConnectToAddress(address):
                await self._connect_to_address(address, ctx)

            case Disconnect(node_id):
                await self._disconnect(node_id)

            case Send(protocol, to_node, data):
                await self._send_to_node(protocol, to_node, data)

            case Broadcast(protocol, data, exclude):
                await self._broadcast(protocol, data, exclude)

    # =========================================================================
    # TCP Event Handlers
    # =========================================================================

    async def _handle_accepted(
        self,
        connection: LocalRef,
        remote_address: tuple[str, int],
        ctx: Context,
    ) -> None:
        """Handle incoming connection - wait for handshake."""
        await connection.send(tcp.Register(handler=ctx.self_ref))
        self._pending[connection] = PendingConnection(
            connection=connection,
            address=remote_address,
            is_outgoing=False,
        )
        log.debug(f"[{self._node_id}] Accepted connection from {remote_address}")

    async def _handle_connected(
        self,
        connection: LocalRef,
        remote_address: tuple[str, int],
        ctx: Context,
    ) -> None:
        """Handle outgoing connection established - send handshake."""
        # Register to receive data from the connection
        await connection.send(tcp.Register(handler=ctx.self_ref))

        # Find the pending connection (keyed by Client actor, which is ctx.sender)
        client = ctx.sender
        pending = self._pending.pop(client, None)
        if pending:
            # Update pending to use the actual connection
            pending.connection = connection
            pending.address = remote_address
            self._pending[connection] = pending

            # Send handshake
            handshake = _Handshake(
                node_id=self._node_id,
                address=self.announced_address or self._bind_address,
            )
            await self._send_frame(
                connection,
                ProtocolType.HANDSHAKE,
                self._encode_handshake(handshake),
            )
            log.debug(f"[{self._node_id}] Sent handshake to {remote_address}")

    async def _handle_received(self, data: bytes, ctx: Context) -> None:
        """Handle incoming data."""
        sender = ctx.sender
        if not sender:
            return

        # Check if pending (handshake phase)
        pending = self._pending.get(sender)
        if pending:
            pending.recv_buffer.extend(data)
            await self._process_handshake(sender, pending, ctx)
            return

        # Active connection
        node_id = self._connection_to_node.get(sender)
        if node_id:
            conn = self._connections.get(node_id)
            if conn:
                conn.recv_buffer.extend(data)
                await self._process_buffer(node_id, conn)

    async def _handle_closed(self, ctx: Context) -> None:
        """Handle connection closed."""
        sender = ctx.sender
        if not sender:
            return

        # Check pending
        pending = self._pending.pop(sender, None)
        if pending:
            log.debug(f"[{self._node_id}] Pending connection closed: {pending.address}")
            return

        # Check active
        node_id = self._connection_to_node.pop(sender, None)
        if node_id:
            self._connections.pop(node_id, None)
            log.info(f"[{self._node_id}] Peer disconnected: {node_id}")
            await self._notify_handlers(PeerDisconnected(node_id, "closed"))

    async def _handle_command_failed(self, cmd: Any, ctx: Context) -> None:
        """Handle TCP command failure."""
        # Check if this was a connect attempt
        sender = ctx.sender
        if sender:
            pending = self._pending.pop(sender, None)
            if pending and pending.target_node_id:
                log.warning(
                    f"[{self._node_id}] Failed to connect to {pending.target_node_id}"
                )

    # =========================================================================
    # Handshake Processing
    # =========================================================================

    async def _process_handshake(
        self,
        connection: LocalRef,
        pending: PendingConnection,
        ctx: Context,
    ) -> None:
        """Process handshake data."""
        result = self._decode_frame(bytes(pending.recv_buffer))
        if result is None:
            return  # Need more data

        protocol, payload, remaining = result
        pending.recv_buffer = bytearray(remaining)

        if protocol != ProtocolType.HANDSHAKE:
            log.warning(f"[{self._node_id}] Expected handshake, got {protocol}")
            await connection.send(tcp.Close())
            self._pending.pop(connection, None)
            return

        msg = self._decode_handshake(payload)

        match msg:
            case _Handshake(node_id, address):
                # Incoming handshake - send ack
                ack = _HandshakeAck(
                    node_id=self._node_id,
                    address=self.announced_address or self._bind_address,
                )
                await self._send_frame(
                    connection,
                    ProtocolType.HANDSHAKE,
                    self._encode_handshake(ack),
                )
                # Promote connection
                await self._promote_connection(connection, pending, node_id, address)

            case _HandshakeAck(node_id, address, accepted):
                if not accepted:
                    log.warning(f"[{self._node_id}] Handshake rejected by {node_id}")
                    await connection.send(tcp.Close())
                    self._pending.pop(connection, None)
                    return
                # Promote connection
                await self._promote_connection(connection, pending, node_id, address)

    async def _promote_connection(
        self,
        connection: LocalRef,
        pending: PendingConnection,
        node_id: str,
        address: tuple[str, int],
    ) -> None:
        """Promote pending connection to active."""
        self._pending.pop(connection, None)

        # Check for duplicate
        if node_id in self._connections:
            log.debug(f"[{self._node_id}] Duplicate connection to {node_id}, closing new")
            await connection.send(tcp.Close())
            return

        # Create active state
        conn = ConnectionState(
            node_id=node_id,
            address=address,
            connection=connection,
            recv_buffer=pending.recv_buffer,
        )
        self._connections[node_id] = conn
        self._connection_to_node[connection] = node_id

        log.info(f"[{self._node_id}] Peer connected: {node_id} at {address}")
        await self._notify_handlers(PeerConnected(node_id, address))

        # Process any buffered data
        if conn.recv_buffer:
            await self._process_buffer(node_id, conn)

    # =========================================================================
    # Data Processing
    # =========================================================================

    async def _process_buffer(self, node_id: str, conn: ConnectionState) -> None:
        """Process buffered data, routing to handlers."""
        while True:
            result = self._decode_frame(bytes(conn.recv_buffer))
            if result is None:
                break

            protocol, payload, remaining = result
            conn.recv_buffer = bytearray(remaining)

            log.debug(f"[{self._node_id}] Decoded frame from {node_id}: protocol={protocol.name}, payload_size={len(payload)}")

            # Skip handshake messages (shouldn't happen after promotion)
            if protocol == ProtocolType.HANDSHAKE:
                log.warning(f"[{self._node_id}] Unexpected handshake from {node_id}")
                continue

            # Route to handler
            handler = self._handlers.get(protocol)
            if handler:
                log.debug(f"[{self._node_id}] Routing {len(payload)} bytes from {node_id} to handler {handler} for {protocol.name}")
                try:
                    import sys
                    incoming_msg = Incoming(
                        from_node=node_id,
                        from_address=conn.address,
                        data=payload,
                    )
                    await handler.send(incoming_msg, sender=self._ctx.self_ref)
                    log.debug(f"[{self._node_id}] Successfully sent Incoming to handler")
                except Exception as e:
                    log.error(f"[{self._node_id}] Failed to send Incoming to handler: {e}", exc_info=True)
            else:
                log.warning(f"[{self._node_id}] No handler for protocol {protocol.name}")

    # =========================================================================
    # Connection Management
    # =========================================================================

    async def _connect_to(
        self,
        node_id: str,
        address: tuple[str, int],
        ctx: Context,
    ) -> None:
        """Initiate outgoing connection."""
        # Already connected?
        if node_id in self._connections:
            log.debug(f"[{self._node_id}] Already connected to {node_id}")
            return

        # Already connecting?
        for pending in self._pending.values():
            if pending.target_node_id == node_id:
                log.debug(f"[{self._node_id}] Already connecting to {node_id}")
                return

        log.debug(f"[{self._node_id}] Connecting to {node_id} at {address}")
        client = await ctx.spawn(tcp.Client)
        self._pending[client] = PendingConnection(
            connection=client,
            address=address,
            is_outgoing=True,
            target_node_id=node_id,
        )
        await client.send(tcp.Connect(address[0], address[1]))

    async def _connect_to_address(
        self,
        address: tuple[str, int],
        ctx: Context,
    ) -> None:
        """Initiate outgoing connection to address (node_id unknown).

        Used for seed connections where we don't know the node_id yet.
        The node_id is discovered during handshake.
        """
        # Check if already connected to this address
        for conn in self._connections.values():
            if conn.address == address:
                log.debug(f"[{self._node_id}] Already connected to {address}")
                return

        # Check if already connecting to this address
        for pending in self._pending.values():
            if pending.address == address:
                log.debug(f"[{self._node_id}] Already connecting to {address}")
                return

        log.debug(f"[{self._node_id}] Connecting to seed at {address}")
        client = await ctx.spawn(tcp.Client)
        self._pending[client] = PendingConnection(
            connection=client,
            address=address,
            is_outgoing=True,
            target_node_id=None,  # Unknown for seed connections
        )
        await client.send(tcp.Connect(address[0], address[1]))

    async def _disconnect(self, node_id: str) -> None:
        """Disconnect from a node."""
        conn = self._connections.pop(node_id, None)
        if conn:
            self._connection_to_node.pop(conn.connection, None)
            await conn.connection.send(tcp.Close())
            log.info(f"[{self._node_id}] Disconnected from {node_id}")

    # =========================================================================
    # Sending
    # =========================================================================

    async def _send_to_node(
        self,
        protocol: ProtocolType,
        to_node: str,
        data: bytes,
    ) -> None:
        """Send framed data to a specific node."""
        conn = self._connections.get(to_node)
        if not conn:
            log.warning(f"[{self._node_id}] Cannot send to {to_node}: not connected. Available connections: {list(self._connections.keys())}")
            return

        await self._send_frame(conn.connection, protocol, data)

    async def _broadcast(
        self,
        protocol: ProtocolType,
        data: bytes,
        exclude: tuple[str, ...],
    ) -> None:
        """Broadcast to all connected nodes."""
        frame = self._encode_frame(protocol, data)
        for node_id, conn in self._connections.items():
            if node_id not in exclude:
                await conn.connection.send(tcp.Write(frame))

    async def _send_frame(
        self,
        connection: LocalRef,
        protocol: ProtocolType,
        payload: bytes,
    ) -> None:
        """Send a framed message."""
        frame = self._encode_frame(protocol, payload)
        await connection.send(tcp.Write(frame))

    # =========================================================================
    # Wire Format
    # =========================================================================

    def _encode_frame(self, protocol: ProtocolType, payload: bytes) -> bytes:
        """Encode: [length: 4B BE][protocol: 1B][payload]."""
        length = 1 + len(payload)
        return struct.pack(">IB", length, protocol.value) + payload

    def _decode_frame(
        self, data: bytes
    ) -> tuple[ProtocolType, bytes, bytes] | None:
        """Decode frame. Returns (protocol, payload, remaining) or None if incomplete."""
        if len(data) < self.HEADER_SIZE:
            return None

        length = struct.unpack(">I", data[:4])[0]
        if length > self.MAX_MESSAGE_SIZE:
            raise ValueError(f"Message too large: {length}")

        total_size = 4 + length
        if len(data) < total_size:
            return None

        protocol = ProtocolType(data[4])
        payload = data[5:total_size]
        remaining = data[total_size:]

        return protocol, payload, remaining

    # =========================================================================
    # Handshake Encoding
    # =========================================================================

    def _encode_handshake(self, msg: _Handshake | _HandshakeAck) -> bytes:
        """Encode handshake message using msgpack."""
        match msg:
            case _Handshake(node_id, address):
                return msgpack.packb({
                    "t": 1,
                    "n": node_id,
                    "a": list(address),
                }, use_bin_type=True)
            case _HandshakeAck(node_id, address, accepted):
                return msgpack.packb({
                    "t": 2,
                    "n": node_id,
                    "a": list(address),
                    "ok": accepted,
                }, use_bin_type=True)

    def _decode_handshake(self, data: bytes) -> _Handshake | _HandshakeAck:
        """Decode handshake message."""
        d = msgpack.unpackb(data, raw=False)
        if d["t"] == 1:
            return _Handshake(d["n"], tuple(d["a"]))
        else:
            return _HandshakeAck(d["n"], tuple(d["a"]), d.get("ok", True))

    # =========================================================================
    # Notifications
    # =========================================================================

    async def _notify_handlers(
        self, event: PeerConnected | PeerDisconnected
    ) -> None:
        """Notify all registered handlers of connection event."""
        for handler in self._handlers.values():
            await handler.send(event, sender=self._ctx.self_ref)
