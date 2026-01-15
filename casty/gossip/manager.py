"""
GossipManager Actor - central coordinator for gossip protocol.
"""

import asyncio
import logging
from typing import Any

from casty import Actor, LocalRef, Context
from casty.io import tcp
from casty.transport import Incoming, PeerConnected, PeerDisconnected, Send, ProtocolType

from .codec import GossipCodec
from .config import GossipConfig
from .messages import (
    Publish,
    Delete,
    Subscribe,
    Unsubscribe,
    GetState,
    GetPeers,
    StateChanged,
    PeerJoined,
    PeerLeft,
    GossipStarted,
    GossipStopped,
    LocalPut,
    LocalDelete,
    ApplyRemoteUpdates,
    GetDigest,
    GetEntries,
    GetPendingUpdates,
    ComputeDiff,
    AddPeer,
    IncomingUpdates,
    Handshake,
    HandshakeAck,
    GossipPush,
    SyncRequest,
    SyncResponse,
    SyncData,
    Ping,
    Pong,
    GossipPushAck,
    IncomingGossipData,
    OutgoingGossipData,
)
from .patterns import PatternMatcher
from .peer_manager import PeerManager
from .state_store import StateStore

logger = logging.getLogger(__name__)


class GossipManager(Actor):
    """Central coordinator for gossip protocol.

    Manages StateStore and PeerManager children.
    Handles incoming TCP connections.
    Routes messages between components.
    Provides external API (publish, subscribe, get).

    Example:
        gossip = await system.spawn(
            GossipManager,
            node_id="node-1",
            bind_address=("0.0.0.0", 9000),
            seeds=[("192.168.1.10", 9000)],
        )

        await gossip.send(Publish("cluster/leader", b"node-1"))
        value = await gossip.ask(GetState("cluster/leader"))
    """

    def __init__(
        self,
        node_id: str,
        bind_address: tuple[str, int],
        seeds: list[tuple[str, int]] | None = None,
        config: GossipConfig | None = None,
        transport: LocalRef | None = None,
    ):
        self._node_id = node_id
        self._bind_address = bind_address
        self._seeds = seeds or []
        self._config = config or GossipConfig()

        # External transport (Cluster actor) for embedded mode
        # When set, gossip shares TCP port with cluster
        self._transport = transport

        # Child actors (set in on_start)
        self._state_store: LocalRef | None = None
        self._peer_manager: LocalRef | None = None
        self._server: LocalRef | None = None

        # Pattern matcher for subscriptions
        self._subscriptions = PatternMatcher()

        # Incoming connection handlers: connection -> recv_buffer
        self._incoming_buffers: dict[LocalRef, bytearray] = {}
        self._incoming_nodes: dict[LocalRef, str] = {}

    async def on_start(self) -> None:
        """Initialize child actors and start gossip."""
        ctx = self._ctx

        # Spawn state store
        self._state_store = await ctx.spawn(
            StateStore,
            node_id=self._node_id,
            parent=ctx.self_ref,
            config=self._config,
        )

        if self._transport:
            # Embedded mode - share TCP port with Cluster
            # PeerManager and seeds are initialized immediately
            self._peer_manager = await ctx.spawn(
                PeerManager,
                node_id=self._node_id,
                address=self._bind_address,
                coordinator=ctx.self_ref,
                state_store=self._state_store,
                config=self._config,
                transport=self._transport,  # Pass transport to PeerManager
            )

            # Connect to seeds
            for seed in self._seeds:
                await self._peer_manager.send(AddPeer(address=seed, is_seed=True))

            # Notify parent immediately (address already known)
            logger.info(f"[{self._node_id}] Gossip started in embedded mode")
            if ctx.parent:
                await ctx.parent.send(
                    GossipStarted(
                        node_id=self._node_id,
                        address=self._bind_address,
                    ),
                    sender=ctx.self_ref,
                )
        else:
            # Standalone mode - create own TCP server
            # Note: PeerManager is spawned after tcp.Bound (see receive method)
            # This ensures we have the correct address when port=0 is used
            self._server = await ctx.spawn(tcp.Server)
            await self._server.send(tcp.Bind(
                host=self._bind_address[0],
                port=self._bind_address[1],
            ))

    async def on_stop(self) -> None:
        """Clean shutdown."""
        if self._ctx.parent:
            await self._ctx.parent.send(
                GossipStopped(node_id=self._node_id),
                sender=self._ctx.self_ref,
            )

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            # TCP server events
            case tcp.Bound(host, port):
                # Update bind address with actual port (important when port=0)
                self._bind_address = (host, port)
                logger.info(f"[{self._node_id}] Gossip listening on {host}:{port}")

                # Now spawn peer manager with correct address
                self._peer_manager = await ctx.spawn(
                    PeerManager,
                    node_id=self._node_id,
                    address=self._bind_address,
                    coordinator=ctx.self_ref,
                    state_store=self._state_store,
                    config=self._config,
                )

                # Connect to seeds
                for seed in self._seeds:
                    await self._peer_manager.send(AddPeer(address=seed, is_seed=True))
                # Notify parent
                if ctx.parent:
                    await ctx.parent.send(
                        GossipStarted(
                            node_id=self._node_id,
                            address=(host, port),
                        ),
                        sender=ctx.self_ref,
                    )

            case tcp.Accepted(connection, remote_address):
                await self._handle_incoming_connection(connection, remote_address, ctx)

            case tcp.CommandFailed(cmd, cause):
                logger.error(f"[{self._node_id}] TCP command failed: {cmd} - {cause}")

            # Incoming data from accepted connections
            case tcp.Received(data):
                sender = ctx.sender
                if sender:
                    await self._handle_incoming_data(sender, data, ctx)

            case tcp.Closed() | tcp.PeerClosed() | tcp.ErrorClosed():
                sender = ctx.sender
                if sender:
                    self._cleanup_incoming(sender)

            # External API
            case Publish(key, value, ttl):
                await self._state_store.send(LocalPut(key=key, value=value, ttl=ttl))

            case Delete(key):
                await self._state_store.send(LocalDelete(key=key))

            case Subscribe(pattern, subscriber):
                self._subscriptions.add_pattern(pattern, subscriber)

            case Unsubscribe(pattern, subscriber):
                self._subscriptions.remove_pattern(pattern, subscriber)

            case GetState(key):
                result = await self._state_store.ask(GetState(key=key))
                ctx.reply(result)

            case GetPeers():
                result = await self._peer_manager.ask(GetPeers())
                ctx.reply(result)

            # Internal events from StateStore
            case StateChanged() as event:
                await self._notify_subscribers(event)

            # Internal events from PeerManager
            case PeerJoined(node_id, address):
                logger.info(f"[{self._node_id}] Peer joined: {node_id} at {address}")
                # Notify subscribers
                await self._notify_subscribers(
                    StateChanged(
                        key=f"_gossip/peers/{node_id}",
                        value=f"{address[0]}:{address[1]}".encode(),
                        version=None,
                        source="system",
                    )
                )

            case PeerLeft(node_id, reason):
                logger.info(f"[{self._node_id}] Peer left: {node_id} ({reason})")
                await self._notify_subscribers(
                    StateChanged(
                        key=f"_gossip/peers/{node_id}",
                        value=None,
                        version=None,
                        source="system",
                    )
                )

            # Internal events from PeerActor
            case IncomingUpdates(updates, source):
                await self._state_store.send(
                    ApplyRemoteUpdates(entries=updates, source=source)
                )

            # Forward queries to appropriate child
            case GetDigest():
                result = await self._state_store.ask(msg)
                ctx.reply(result)

            case GetEntries():
                result = await self._state_store.ask(msg)
                ctx.reply(result)

            case GetPendingUpdates():
                result = await self._state_store.ask(msg)
                ctx.reply(result)

            case ComputeDiff():
                result = await self._state_store.ask(msg)
                ctx.reply(result)

            # TransportMux events: incoming gossip data from TransportMux
            case Incoming(from_node, from_address, data):
                await self._handle_embedded_gossip_data(from_node, data, ctx)

            # TransportMux events: peer connected/disconnected
            case PeerConnected(node_id, address):
                logger.debug(f"[{self._node_id}] TransportMux peer connected: {node_id}")
                if self._peer_manager:
                    await self._peer_manager.send(AddPeer(address=address, node_id=node_id))

            case PeerDisconnected(node_id, reason):
                logger.debug(f"[{self._node_id}] TransportMux peer disconnected: {node_id}")

            # Legacy: incoming gossip data from Cluster transport (fallback)
            case IncomingGossipData(from_node, data):
                await self._handle_embedded_gossip_data(from_node, data, ctx)

            # Embedded mode: PeerManager wants to send data
            case OutgoingGossipData(to_node, data):
                if self._transport:
                    # Send via TransportMux
                    await self._transport.send(
                        Send(ProtocolType.GOSSIP, to_node, data),
                        sender=ctx.self_ref,
                    )

    async def _handle_incoming_connection(
        self,
        connection: LocalRef,
        remote_address: tuple[str, int],
        ctx: Context,
    ) -> None:
        """Handle incoming TCP connection."""
        # Register to receive data
        await connection.send(tcp.Register(handler=ctx.self_ref))
        self._incoming_buffers[connection] = bytearray()
        logger.debug(f"[{self._node_id}] Incoming connection from {remote_address}")

    async def _handle_incoming_data(
        self,
        connection: LocalRef,
        data: bytes,
        ctx: Context,
    ) -> None:
        """Handle data from incoming connection."""
        buffer = self._incoming_buffers.get(connection)
        if buffer is None:
            return

        buffer.extend(data)
        messages, remaining = GossipCodec.decode_all(bytes(buffer))
        self._incoming_buffers[connection] = bytearray(remaining)

        for msg in messages:
            await self._handle_wire_message(connection, msg, ctx)

    async def _handle_wire_message(
        self,
        connection: LocalRef,
        msg: Any,
        ctx: Context,
    ) -> None:
        """Handle wire protocol message from incoming connection."""
        match msg:
            case Handshake(node_id, address):
                self._incoming_nodes[connection] = node_id
                # Send ack
                ack = HandshakeAck(
                    node_id=self._node_id,
                    address=self._bind_address,
                )
                await connection.send(tcp.Write(data=GossipCodec.encode(ack)))
                logger.debug(f"[{self._node_id}] Handshake from {node_id} at {address}")
                # Create PeerActor to connect back for bidirectional gossip
                if self._peer_manager:
                    await self._peer_manager.send(AddPeer(address=address, node_id=node_id))

            case GossipPush(sender_id, sender_address, updates):
                # Apply updates
                if updates:
                    await self._state_store.send(
                        ApplyRemoteUpdates(entries=updates, source=sender_id)
                    )
                # Send ack
                digest = await self._state_store.ask(GetDigest())
                ack = GossipPushAck(
                    sender_id=self._node_id,
                    received_count=len(updates),
                    digest=digest,
                )
                await connection.send(tcp.Write(data=GossipCodec.encode(ack)))

            case SyncRequest(sender_id, sender_address, digest):
                # Compute diff
                diff = await self._state_store.ask(ComputeDiff(remote_digest=digest))
                entries = ()
                if diff.keys_peer_needs:
                    entries = await self._state_store.ask(
                        GetEntries(keys=diff.keys_peer_needs)
                    )
                response = SyncResponse(
                    sender_id=self._node_id,
                    entries_for_you=entries,
                    keys_i_need=diff.keys_i_need,
                )
                await connection.send(tcp.Write(data=GossipCodec.encode(response)))

            case SyncResponse(sender_id, entries_for_you, keys_i_need):
                # Apply received entries
                if entries_for_you:
                    await self._state_store.send(
                        ApplyRemoteUpdates(entries=entries_for_you, source=sender_id)
                    )
                # Send requested entries
                if keys_i_need:
                    entries = await self._state_store.ask(GetEntries(keys=keys_i_need))
                    data_msg = SyncData(
                        sender_id=self._node_id,
                        entries=entries,
                    )
                    await connection.send(tcp.Write(data=GossipCodec.encode(data_msg)))

            case SyncData(sender_id, entries):
                if entries:
                    await self._state_store.send(
                        ApplyRemoteUpdates(entries=entries, source=sender_id)
                    )

            case Ping(sender_id, sequence):
                pong = Pong(sender_id=self._node_id, sequence=sequence)
                await connection.send(tcp.Write(data=GossipCodec.encode(pong)))

            case Pong():
                pass

    def _cleanup_incoming(self, connection: LocalRef) -> None:
        """Clean up incoming connection state."""
        self._incoming_buffers.pop(connection, None)
        node_id = self._incoming_nodes.pop(connection, None)
        if node_id:
            logger.debug(f"[{self._node_id}] Incoming connection closed: {node_id}")

    async def _notify_subscribers(self, event: StateChanged) -> None:
        """Notify matching subscribers of state change."""
        handlers = self._subscriptions.get_matching(event.key)
        for handler in handlers:
            try:
                await handler.send(event, sender=self._ctx.self_ref)
            except Exception as e:
                logger.warning(f"[{self._node_id}] Failed to notify subscriber: {e}")

    async def _handle_embedded_gossip_data(
        self,
        from_node: str,
        data: bytes,
        ctx: Context,
    ) -> None:
        """Handle gossip data from Cluster transport (embedded mode)."""
        messages, _ = GossipCodec.decode_all(data)
        for msg in messages:
            await self._handle_embedded_wire_message(from_node, msg, ctx)

    async def _handle_embedded_wire_message(
        self,
        from_node: str,
        msg: Any,
        ctx: Context,
    ) -> None:
        """Handle wire protocol message in embedded mode.

        Similar to _handle_wire_message but sends responses via transport.
        """
        match msg:
            case Handshake(node_id, address):
                # Send ack via transport
                ack = HandshakeAck(
                    node_id=self._node_id,
                    address=self._bind_address,
                )
                await self._send_via_transport(node_id, GossipCodec.encode(ack))
                logger.debug(f"[{self._node_id}] Handshake from {node_id} at {address}")
                # Notify PeerManager about the peer
                if self._peer_manager:
                    await self._peer_manager.send(AddPeer(address=address, node_id=node_id))

            case GossipPush(sender_id, sender_address, updates):
                # Apply updates
                if updates:
                    await self._state_store.send(
                        ApplyRemoteUpdates(entries=updates, source=sender_id)
                    )
                # Send ack via transport
                digest = await self._state_store.ask(GetDigest())
                ack = GossipPushAck(
                    sender_id=self._node_id,
                    received_count=len(updates),
                    digest=digest,
                )
                await self._send_via_transport(sender_id, GossipCodec.encode(ack))

            case SyncRequest(sender_id, sender_address, digest):
                # Compute diff
                diff = await self._state_store.ask(ComputeDiff(remote_digest=digest))
                entries = ()
                if diff.keys_peer_needs:
                    entries = await self._state_store.ask(
                        GetEntries(keys=diff.keys_peer_needs)
                    )
                response = SyncResponse(
                    sender_id=self._node_id,
                    entries_for_you=entries,
                    keys_i_need=diff.keys_i_need,
                )
                await self._send_via_transport(sender_id, GossipCodec.encode(response))

            case SyncResponse(sender_id, entries_for_you, keys_i_need):
                # Apply received entries
                if entries_for_you:
                    await self._state_store.send(
                        ApplyRemoteUpdates(entries=entries_for_you, source=sender_id)
                    )
                # Send requested entries
                if keys_i_need:
                    entries = await self._state_store.ask(GetEntries(keys=keys_i_need))
                    data_msg = SyncData(
                        sender_id=self._node_id,
                        entries=entries,
                    )
                    await self._send_via_transport(sender_id, GossipCodec.encode(data_msg))

            case SyncData(sender_id, entries):
                if entries:
                    await self._state_store.send(
                        ApplyRemoteUpdates(entries=entries, source=sender_id)
                    )

            case Ping(sender_id, sequence):
                pong = Pong(sender_id=self._node_id, sequence=sequence)
                await self._send_via_transport(sender_id, GossipCodec.encode(pong))

            case Pong():
                pass

            case GossipPushAck():
                pass  # Acknowledgment received

            case HandshakeAck():
                pass  # Already handled

    async def _send_via_transport(self, to_node: str, data: bytes) -> None:
        """Send gossip data via TransportMux (embedded mode)."""
        if self._transport:
            await self._transport.send(
                Send(ProtocolType.GOSSIP, to_node, data),
                sender=self._ctx.self_ref,
            )
