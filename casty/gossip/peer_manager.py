"""
PeerManager Actor - manages peer connections and schedules gossip rounds.
"""

import asyncio
import random

from casty import Actor, LocalRef, Context

from .config import GossipConfig
from .codec import GossipCodec
from .messages import (
    AddPeer,
    RemovePeer,
    GossipRound,
    AntiEntropyRound,
    PeerConnected,
    PeerDisconnected,
    BroadcastUpdates,
    GetPeers,
    SendPush,
    StartSync,
    GetDigest,
    GetPendingUpdates,
    PeerJoined,
    PeerLeft,
    OutgoingGossipData,
    GossipPush,
    SyncRequest,
)
# Note: GetDigest and GetPendingUpdates are sent directly to state_store
from .peer import PeerActor


class PeerManager(Actor):
    """Manages peer connections and gossip scheduling.

    Responsibilities:
    - Track known peers and their status
    - Spawn PeerActor for each peer
    - Schedule periodic gossip rounds
    - Select random peers for gossip fanout

    Example:
        manager = await ctx.spawn(
            PeerManager,
            node_id="node-1",
            address=("0.0.0.0", 9000),
            coordinator=gossip_ref,
            state_store=state_store_ref,
        )
        await manager.send(AddPeer(("192.168.1.10", 9000)))
    """

    def __init__(
        self,
        node_id: str,
        address: tuple[str, int],
        coordinator: LocalRef,
        state_store: LocalRef,
        config: GossipConfig | None = None,
        transport: LocalRef | None = None,
    ):
        self._node_id = node_id
        self._address = address
        self._coordinator = coordinator
        self._state_store = state_store
        self._config = config or GossipConfig()

        # External transport (Cluster actor) for embedded mode
        # When set, gossip uses Cluster's TCP connections instead of PeerActors
        self._transport = transport

        # Peers: node_id -> PeerActor ref (only used in standalone mode)
        self._peers: dict[str, LocalRef] = {}
        self._peer_addresses: dict[str, tuple[str, int]] = {}
        self._address_to_node: dict[tuple[str, int], str] = {}

        # Pending connections (address -> PeerActor ref)
        self._pending: dict[tuple[str, int], LocalRef] = {}

        # For random peer selection (used in both modes)
        self._peer_list: list[str] = []

        # Scheduling tasks
        self._gossip_task: asyncio.Task | None = None
        self._anti_entropy_task: asyncio.Task | None = None

    async def on_start(self) -> None:
        """Start gossip scheduling."""
        self._schedule_gossip_round()
        self._schedule_anti_entropy()

    async def on_stop(self) -> None:
        """Cancel scheduled tasks."""
        if self._gossip_task:
            self._gossip_task.cancel()
        if self._anti_entropy_task:
            self._anti_entropy_task.cancel()

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            case AddPeer(address, is_seed, node_id):
                await self._add_peer(address, node_id, ctx)

            case RemovePeer(node_id):
                await self._remove_peer(node_id, ctx)

            case GossipRound():
                await self._do_gossip_round()
                self._schedule_gossip_round()

            case AntiEntropyRound():
                await self._do_anti_entropy()
                self._schedule_anti_entropy()

            case PeerConnected(node_id, address):
                await self._handle_peer_connected(node_id, address, ctx)

            case PeerDisconnected(node_id, reason):
                await self._handle_peer_disconnected(node_id, reason, ctx)

            case BroadcastUpdates(updates):
                await self._broadcast_to_peers(updates)

            case GetPeers():
                ctx.reply(list(self._peers.keys()))

    async def _add_peer(
        self,
        address: tuple[str, int],
        node_id: str | None,
        ctx: Context,
    ) -> None:
        """Add a new peer connection."""
        # Don't connect to self
        if address == self._address:
            return

        if self._transport:
            # Embedded mode: just track the peer, Cluster handles connections
            if node_id and node_id not in self._peer_list:
                self._peer_list.append(node_id)
                self._peer_addresses[node_id] = address
                self._address_to_node[address] = node_id
                # Notify coordinator
                await self._coordinator.send(
                    PeerJoined(node_id=node_id, address=address),
                    sender=ctx.self_ref,
                )
            return

        # Standalone mode: spawn PeerActor
        # Already connected or connecting
        if address in self._address_to_node or address in self._pending:
            return

        # Spawn PeerActor
        peer = await ctx.spawn(
            PeerActor,
            peer_address=address,
            our_node_id=self._node_id,
            our_address=self._address,
            coordinator=ctx.self_ref,
            state_store=self._state_store,
            config=self._config,
        )
        self._pending[address] = peer

    async def _remove_peer(self, node_id: str, ctx: Context) -> None:
        """Remove a peer."""
        peer = self._peers.pop(node_id, None)
        if peer:
            await ctx.stop_child(peer)

        address = self._peer_addresses.pop(node_id, None)
        if address:
            self._address_to_node.pop(address, None)

        if node_id in self._peer_list:
            self._peer_list.remove(node_id)

    async def _handle_peer_connected(
        self,
        node_id: str,
        address: tuple[str, int],
        ctx: Context,
    ) -> None:
        """Handle successful peer connection."""
        # Move from pending to connected
        peer = self._pending.pop(address, None)
        if peer is None:
            # This might be an incoming connection
            return

        # Check for duplicate (another connection from same node)
        if node_id in self._peers:
            # Keep existing, close new
            await ctx.stop_child(peer)
            return

        self._peers[node_id] = peer
        self._peer_addresses[node_id] = address
        self._address_to_node[address] = node_id
        self._peer_list.append(node_id)

        # Notify coordinator
        await self._coordinator.send(
            PeerJoined(node_id=node_id, address=address),
            sender=ctx.self_ref,
        )

    async def _handle_peer_disconnected(
        self,
        node_id: str,
        reason: str,
        ctx: Context,
    ) -> None:
        """Handle peer disconnection."""
        # Remove from tracking
        peer = self._peers.pop(node_id, None)
        address = self._peer_addresses.pop(node_id, None)

        if address:
            self._address_to_node.pop(address, None)
            self._pending.pop(address, None)

        if node_id in self._peer_list:
            self._peer_list.remove(node_id)

        # Notify coordinator
        await self._coordinator.send(
            PeerLeft(node_id=node_id, reason=reason),
            sender=ctx.self_ref,
        )

    async def _do_gossip_round(self) -> None:
        """Execute one gossip round (push phase)."""
        if not self._peer_list:
            return

        # Get pending updates (direct to state_store)
        updates = await self._state_store.ask(
            GetPendingUpdates(max_count=self._config.max_push_entries)
        )

        if not updates:
            return

        # Select random peers (fanout)
        targets = random.sample(
            self._peer_list,
            min(self._config.fanout, len(self._peer_list)),
        )

        if self._transport:
            # Embedded mode: send via coordinator which forwards to Cluster
            push_msg = GossipPush(
                sender_id=self._node_id,
                sender_address=self._address,
                updates=updates,
            )
            data = GossipCodec.encode(push_msg)
            for node_id in targets:
                await self._coordinator.send(
                    OutgoingGossipData(to_node=node_id, data=data),
                    sender=self._ctx.self_ref,
                )
        else:
            # Standalone mode: send via PeerActors
            for node_id in targets:
                peer = self._peers.get(node_id)
                if peer:
                    await peer.send(SendPush(updates=updates))

    async def _do_anti_entropy(self) -> None:
        """Execute anti-entropy synchronization."""
        if not self._peer_list:
            return

        # Pick one random peer
        node_id = random.choice(self._peer_list)

        # Get digest (direct to state_store)
        digest = await self._state_store.ask(GetDigest())

        if self._transport:
            # Embedded mode: send via coordinator
            sync_req = SyncRequest(
                sender_id=self._node_id,
                sender_address=self._address,
                digest=digest,
            )
            data = GossipCodec.encode(sync_req)
            await self._coordinator.send(
                OutgoingGossipData(to_node=node_id, data=data),
                sender=self._ctx.self_ref,
            )
        else:
            # Standalone mode: send via PeerActor
            peer = self._peers.get(node_id)
            if peer:
                await peer.send(StartSync(digest=digest))

    async def _broadcast_to_peers(self, updates: tuple) -> None:
        """Broadcast updates to all peers."""
        if self._transport:
            # Embedded mode: send via coordinator
            push_msg = GossipPush(
                sender_id=self._node_id,
                sender_address=self._address,
                updates=updates,
            )
            data = GossipCodec.encode(push_msg)
            for node_id in self._peer_list:
                await self._coordinator.send(
                    OutgoingGossipData(to_node=node_id, data=data),
                    sender=self._ctx.self_ref,
                )
        else:
            # Standalone mode: send via PeerActors
            for peer in self._peers.values():
                await peer.send(SendPush(updates=updates))

    def _schedule_gossip_round(self) -> None:
        """Schedule next gossip round."""
        detached = self._ctx.detach(asyncio.sleep(self._config.gossip_interval)).then(lambda _: GossipRound())
        self._gossip_task = detached.task

    def _schedule_anti_entropy(self) -> None:
        """Schedule next anti-entropy round."""
        detached = self._ctx.detach(asyncio.sleep(self._config.anti_entropy_interval)).then(lambda _: AntiEntropyRound())
        self._anti_entropy_task = detached.task
