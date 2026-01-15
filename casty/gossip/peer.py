"""
PeerActor - manages gossip connection with a single remote peer.

State machine:
    disconnected → connecting → connected → syncing
"""

import asyncio

from casty import Actor, LocalRef, Context
from casty.io import tcp

from .codec import GossipCodec
from .config import GossipConfig
from .messages import (
    Handshake,
    HandshakeAck,
    GossipPush,
    GossipPushAck,
    SyncRequest,
    SyncResponse,
    SyncData,
    Ping,
    Pong,
    SendPush,
    StartSync,
    Reconnect,
    PeerConnected,
    PeerDisconnected,
    IncomingUpdates,
    GetDigest,
    ComputeDiff,
    GetEntries,
    WireMessage,
)


class PeerActor(Actor):
    """Actor managing gossip with a single remote peer.

    Uses tcp.Client for connection management.
    Handles message framing/deframing using GossipCodec.

    States:
    - receive (disconnected): Initial/reconnecting
    - _connecting: TCP connection in progress
    - _connected: Active connection
    - _syncing: Anti-entropy sync in progress

    Example:
        peer = await ctx.spawn(
            PeerActor,
            peer_address=("192.168.1.10", 9000),
            our_node_id="node-1",
            our_address=("0.0.0.0", 9000),
            coordinator=gossip_ref,
            state_store=state_store_ref,
        )
    """

    def __init__(
        self,
        peer_address: tuple[str, int],
        our_node_id: str,
        our_address: tuple[str, int],
        coordinator: LocalRef,
        state_store: LocalRef,
        config: GossipConfig | None = None,
    ):
        self._peer_address = peer_address
        self._our_node_id = our_node_id
        self._our_address = our_address
        self._coordinator = coordinator
        self._state_store = state_store
        self._config = config or GossipConfig()

        # Connection state
        self._client: LocalRef | None = None
        self._connection: LocalRef | None = None
        self._peer_node_id: str | None = None

        # Receive buffer for framing
        self._recv_buffer = bytearray()

        # Reconnection state
        self._reconnect_attempts = 0

        # Ping state
        self._ping_sequence = 0
        self._pending_pings: dict[int, float] = {}

    async def on_start(self) -> None:
        """Start connection attempt."""
        await self._connect()

    async def on_stop(self) -> None:
        """Clean up connection."""
        if self._connection:
            await self._connection.send(tcp.Close())

    # =========================================================================
    # STATE: DISCONNECTED (default receive)
    # =========================================================================

    async def receive(self, msg, ctx: Context) -> None:
        """Disconnected state - handle connection results."""
        match msg:
            case tcp.Connected(connection, remote, local):
                self._connection = connection
                await connection.send(tcp.Register(handler=ctx.self_ref))
                # Send handshake
                await self._send_message(
                    Handshake(
                        node_id=self._our_node_id,
                        address=self._our_address,
                    )
                )
                ctx.become(self._connecting)

            case tcp.CommandFailed(cmd, cause):
                await self._handle_connect_failed(cause, ctx)

            case Reconnect():
                await self._connect()

            # Ignore other messages while disconnected
            case SendPush() | StartSync():
                pass

    # =========================================================================
    # STATE: CONNECTING
    # =========================================================================

    async def _connecting(self, msg, ctx: Context) -> None:
        """Connecting state - waiting for handshake response."""
        match msg:
            case tcp.Received(data):
                messages = self._decode_messages(data)
                for m in messages:
                    match m:
                        case HandshakeAck(node_id, address, known_peers):
                            self._peer_node_id = node_id
                            self._reconnect_attempts = 0
                            await self._coordinator.send(
                                PeerConnected(
                                    node_id=node_id,
                                    address=self._peer_address,
                                ),
                                sender=ctx.self_ref,
                            )
                            ctx.become(self._connected)

            case tcp.Closed() | tcp.PeerClosed() | tcp.ErrorClosed():
                await self._handle_disconnect(ctx)

            case tcp.CommandFailed():
                await self._handle_disconnect(ctx)

            # Buffer commands while connecting
            case SendPush() | StartSync():
                pass

    # =========================================================================
    # STATE: CONNECTED
    # =========================================================================

    async def _connected(self, msg, ctx: Context) -> None:
        """Connected state - normal gossip operations."""
        match msg:
            # Commands from PeerManager
            case SendPush(updates):
                await self._send_message(
                    GossipPush(
                        sender_id=self._our_node_id,
                        sender_address=self._our_address,
                        updates=updates,
                    )
                )

            case StartSync(digest):
                await self._send_message(
                    SyncRequest(
                        sender_id=self._our_node_id,
                        sender_address=self._our_address,
                        digest=digest,
                    )
                )
                ctx.become(self._syncing)

            # Incoming data
            case tcp.Received(data):
                messages = self._decode_messages(data)
                for m in messages:
                    await self._handle_gossip_message(m, ctx)

            # Connection events
            case tcp.Closed() | tcp.PeerClosed() | tcp.ErrorClosed():
                await self._handle_disconnect(ctx)

            case tcp.CommandFailed():
                pass  # Write failure, connection may still be alive

    # =========================================================================
    # STATE: SYNCING
    # =========================================================================

    async def _syncing(self, msg, ctx: Context) -> None:
        """Syncing state - anti-entropy in progress."""
        match msg:
            # Incoming data
            case tcp.Received(data):
                messages = self._decode_messages(data)
                for m in messages:
                    match m:
                        case SyncResponse(sender_id, entries_for_you, keys_i_need):
                            # Apply received entries
                            if entries_for_you:
                                await self._coordinator.send(
                                    IncomingUpdates(
                                        updates=entries_for_you,
                                        source=sender_id,
                                    ),
                                    sender=ctx.self_ref,
                                )
                            # Send requested entries (direct to state_store)
                            if keys_i_need:
                                entries = await self._state_store.ask(
                                    GetEntries(keys_i_need)
                                )
                                await self._send_message(
                                    SyncData(
                                        sender_id=self._our_node_id,
                                        entries=entries,
                                    )
                                )
                            # Return to connected state
                            ctx.become(self._connected)

                        case _:
                            # Handle other messages normally
                            await self._handle_gossip_message(m, ctx)

            # Buffer commands while syncing
            case SendPush():
                pass

            case StartSync():
                pass  # Already syncing

            # Connection events
            case tcp.Closed() | tcp.PeerClosed() | tcp.ErrorClosed():
                await self._handle_disconnect(ctx)

    # =========================================================================
    # MESSAGE HANDLING
    # =========================================================================

    async def _handle_gossip_message(self, msg: WireMessage, ctx: Context) -> None:
        """Handle decoded gossip messages."""
        match msg:
            case GossipPush(sender_id, sender_address, updates):
                # Apply updates
                if updates:
                    await self._coordinator.send(
                        IncomingUpdates(
                            updates=updates,
                            source=sender_id,
                        ),
                        sender=ctx.self_ref,
                    )
                # Send ack with our digest (direct to state_store)
                digest = await self._state_store.ask(GetDigest())
                await self._send_message(
                    GossipPushAck(
                        sender_id=self._our_node_id,
                        received_count=len(updates),
                        digest=digest,
                    )
                )

            case GossipPushAck():
                pass  # Could track delivery confirmation

            case SyncRequest(sender_id, sender_address, digest):
                # Compute diff and respond (direct to state_store)
                diff = await self._state_store.ask(ComputeDiff(digest))
                entries = ()
                if diff.keys_peer_needs:
                    entries = await self._state_store.ask(
                        GetEntries(diff.keys_peer_needs)
                    )
                await self._send_message(
                    SyncResponse(
                        sender_id=self._our_node_id,
                        entries_for_you=entries,
                        keys_i_need=diff.keys_i_need,
                    )
                )

            case SyncData(sender_id, entries):
                # Apply received entries
                if entries:
                    await self._coordinator.send(
                        IncomingUpdates(
                            updates=entries,
                            source=sender_id,
                        ),
                        sender=ctx.self_ref,
                    )

            case Ping(sender_id, sequence):
                await self._send_message(
                    Pong(
                        sender_id=self._our_node_id,
                        sequence=sequence,
                    )
                )

            case Pong(sender_id, sequence):
                self._pending_pings.pop(sequence, None)

            case Handshake(node_id, address):
                # Incoming connection handshake (we're the server side)
                self._peer_node_id = node_id
                await self._send_message(
                    HandshakeAck(
                        node_id=self._our_node_id,
                        address=self._our_address,
                    )
                )

    # =========================================================================
    # HELPERS
    # =========================================================================

    async def _connect(self) -> None:
        """Initiate TCP connection to peer."""
        self._client = await self._ctx.spawn(tcp.Client)
        await self._client.send(
            tcp.Connect(
                host=self._peer_address[0],
                port=self._peer_address[1],
            )
        )

    async def _send_message(self, msg: WireMessage) -> None:
        """Send a wire message to the peer."""
        if self._connection is None:
            return

        data = GossipCodec.encode(msg)
        await self._connection.send(tcp.Write(data=data))

    def _decode_messages(self, data: bytes) -> list[WireMessage]:
        """Decode messages from received data."""
        self._recv_buffer.extend(data)
        messages, remaining = GossipCodec.decode_all(bytes(self._recv_buffer))
        self._recv_buffer = bytearray(remaining)
        return messages

    async def _handle_connect_failed(self, cause: Exception, ctx: Context) -> None:
        """Handle connection failure with backoff."""
        self._reconnect_attempts += 1

        if self._reconnect_attempts >= self._config.max_reconnect_attempts:
            # Give up
            await self._coordinator.send(
                PeerDisconnected(
                    node_id=self._peer_node_id or "unknown",
                    reason="max_reconnect_attempts",
                ),
                sender=ctx.self_ref,
            )
            return

        # Schedule reconnect with exponential backoff
        delay = min(
            self._config.reconnect_interval * (2 ** (self._reconnect_attempts - 1)),
            30.0,  # Max 30 seconds
        )
        await self._schedule_reconnect(delay)

    async def _handle_disconnect(self, ctx: Context) -> None:
        """Handle disconnection."""
        self._connection = None
        self._recv_buffer.clear()

        if self._peer_node_id:
            await self._coordinator.send(
                PeerDisconnected(
                    node_id=self._peer_node_id,
                    reason="disconnect",
                ),
                sender=ctx.self_ref,
            )

        ctx.unbecome()  # Return to disconnected state

        # Schedule reconnect
        await self._schedule_reconnect(self._config.reconnect_interval)

    async def _schedule_reconnect(self, delay: float) -> None:
        """Schedule a reconnection attempt."""

        async def reconnect_later():
            await asyncio.sleep(delay)
            await self._ctx.self_ref.send(Reconnect())

        asyncio.create_task(reconnect_later())
