"""SwimCoordinator - Main orchestrator for SWIM+ protocol.

The SwimCoordinator is the central actor that:
- Manages the membership list
- Orchestrates failure detection rounds (round-robin)
- Integrates with the existing gossip system for dissemination
- Manages child actors (members, probes, suspicion, adaptive timer)
- Handles TCP communication for the SWIM protocol
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import Actor, Context
from casty.transport import Incoming, PeerConnected, PeerDisconnected, Send, ProtocolType, ConnectToAddress

from .config import SwimConfig
from .state import MemberInfo, MemberState, ProbeList
from .messages import (
    # Public API
    Subscribe,
    Unsubscribe,
    GetMembers,
    Leave,
    Join,
    # Membership events
    MemberJoined,
    MemberLeft,
    MemberSuspected,
    MemberFailed,
    MemberAlive,
    MemberStateChanged,
    MembershipEvent,
    # Probe messages
    ProbeResult,
    Tick,
    StartProbe,
    IncomingMessage,
    # Suspicion messages
    StartSuspicion,
    SuspicionTimeout,
    ConfirmSuspicion,
    # Adaptive timer
    RecordProbeResult,
    PeriodAdjusted,
    # Wire protocol
    Ping,
    Ack,
    PingReq,
    IndirectAck,
    Nack,
    GossipType,
    GossipEntry,
)
from .member import MemberActor, _MarkSuspected, _MarkDead
from .probe import ProbeActor, _ProbeCompleted
from .codec import SwimCodec

if TYPE_CHECKING:
    from casty import LocalRef
    from casty.io.tcp import Connection


# Internal messages
@dataclass(frozen=True)
class _TcpReceived:
    """Data received from TCP connection."""

    from_address: tuple[str, int]
    data: bytes


@dataclass(frozen=True)
class _TcpConnected:
    """TCP connection established."""

    address: tuple[str, int]
    connection: Any  # ActorRef[Connection]


@dataclass(frozen=True)
class _TcpDisconnected:
    """TCP connection lost."""

    address: tuple[str, int]


# Coordinator message type
CoordinatorMessage = (
    # Public API
    Subscribe
    | Unsubscribe
    | GetMembers
    | Leave
    | Join
    # Membership events
    | MemberStateChanged
    | MemberJoined
    | MemberLeft
    | MemberSuspected
    | MemberFailed
    | MemberAlive
    # Probe
    | ProbeResult
    | Tick
    | _ProbeCompleted
    # Network
    | IncomingMessage
    | _TcpReceived
    | _TcpConnected
    | _TcpDisconnected
    # Transport events (from TransportMux)
    | Incoming
    | PeerConnected
    | PeerDisconnected
    # Suspicion
    | SuspicionTimeout
    # Adaptive
    | PeriodAdjusted
)


class SwimCoordinator(Actor[CoordinatorMessage]):
    """Main SWIM+ protocol coordinator.

    Manages the entire SWIM+ protocol including:
    - Round-robin failure detection
    - Member state management
    - Gossip integration for dissemination
    - Lifeguard suspicion mechanism
    - Protocol period adaptation
    """

    def __init__(
        self,
        node_id: str,
        config: SwimConfig,
        gossip_manager: LocalRef[Any] | None = None,
        transport: LocalRef[Any] | None = None,
    ) -> None:
        self.node_id = node_id
        self.config = config
        self.gossip_manager = gossip_manager
        self._transport = transport  # TransportMux reference

        # Local state
        self.incarnation = 0
        self.members: dict[str, MemberInfo] = {}
        self.member_actors: dict[str, LocalRef[Any]] = {}
        self.probe_list = ProbeList()
        self.sequence = 0

        # Child actors
        self.suspicion_actor: LocalRef[Any] | None = None
        self.adaptive_timer: LocalRef[Any] | None = None

        # Network
        self.tcp_server: LocalRef[Any] | None = None
        self.connections: dict[tuple[str, int], LocalRef[Any]] = {}
        self.receive_buffers: dict[tuple[str, int], bytes] = {}

        # Subscribers
        self.subscribers: set[LocalRef[Any]] = set()

        # Protocol state
        self.running = False
        self.tick_task: asyncio.Task | None = None
        self.current_period = config.protocol_period
        self.current_probe: LocalRef[Any] | None = None
        self.gossip_seq = 0

    async def on_start(self) -> None:
        """Initialize the coordinator."""
        # Add self to members
        self.members[self.node_id] = MemberInfo(
            node_id=self.node_id,
            address=self.config.bind_address,
            state=MemberState.ALIVE,
            incarnation=self.incarnation,
        )

        # Start protocol
        self.running = True
        self.tick_task = asyncio.create_task(self._tick_loop())

        # Connect to seeds
        for seed in self.config.seeds:
            await self._connect_to_peer(seed)

    async def on_stop(self) -> None:
        """Cleanup on stop."""
        self.running = False
        if self.tick_task:
            self.tick_task.cancel()

    async def receive(self, msg: CoordinatorMessage, ctx: Context) -> None:
        """Handle coordinator messages."""
        match msg:
            # Public API
            case Subscribe(subscriber):
                self.subscribers.add(subscriber)

            case Unsubscribe(subscriber):
                self.subscribers.discard(subscriber)

            case GetMembers():
                ctx.reply(dict(self.members))

            case Leave():
                await self._handle_leave(ctx)

            case Join():
                for seed in self.config.seeds:
                    await self._connect_to_peer(seed)

            # Membership events (from member actors)
            case MemberStateChanged():
                await self._handle_member_state_changed(msg, ctx)

            case MemberJoined() | MemberLeft() | MemberSuspected() | MemberFailed() | MemberAlive():
                await self._broadcast_event(msg)

            # Probe results
            case ProbeResult():
                await self._handle_probe_result(msg, ctx)

            case Tick():
                await self._handle_tick(ctx)

            case _ProbeCompleted():
                self.current_probe = None

            # Network
            case IncomingMessage():
                await self._handle_incoming_message(msg, ctx)

            case _TcpReceived(from_address, data):
                await self._handle_tcp_data(from_address, data, ctx)

            case _TcpConnected(address, connection):
                self.connections[address] = connection

            case _TcpDisconnected(address):
                self.connections.pop(address, None)
                self.receive_buffers.pop(address, None)

            # TransportMux events
            case Incoming(from_node, from_address, data):
                await self._handle_transport_incoming(from_node, from_address, data, ctx)

            case PeerConnected(node_id, address):
                await self._handle_peer_connected(node_id, address, ctx)

            case PeerDisconnected(node_id, reason):
                await self._handle_peer_disconnected(node_id, reason, ctx)

            # Suspicion
            case SuspicionTimeout(node_id):
                await self._handle_suspicion_timeout(node_id, ctx)

            # Adaptive timer
            case PeriodAdjusted(new_period):
                self.current_period = new_period

    async def _tick_loop(self) -> None:
        """Periodic tick for protocol rounds."""
        while self.running:
            await asyncio.sleep(self.current_period)
            if self.running:
                await self._ctx.self_ref.send(Tick())

    async def _handle_tick(self, ctx: Context) -> None:
        """Handle protocol tick - start a new probe round."""
        if self.current_probe is not None:
            # Previous probe still running
            return

        # Get next target from round-robin list
        target_id = self.probe_list.next_target()
        if target_id is None or target_id == self.node_id:
            return

        target_info = self.members.get(target_id)
        if target_info is None or target_info.state == MemberState.DEAD:
            return

        # Select intermediaries for indirect probing
        intermediaries = self._select_intermediaries(target_id)

        # Increment sequence
        self.sequence += 1

        # Spawn probe actor
        self.current_probe = await ctx.spawn(
            ProbeActor,
            target_node_id=target_id,
            target_address=target_info.address,
            sequence=self.sequence,
            config=self.config,
            coordinator=ctx.self_ref,
            send_wire_message=self._send_wire_message,
            intermediaries=intermediaries,
            my_node_id=self.node_id,
            my_incarnation=self.incarnation,
        )

    def _select_intermediaries(
        self, exclude: str
    ) -> list[tuple[str, tuple[str, int]]]:
        """Select K intermediaries for indirect probing."""
        candidates = [
            (node_id, info.address)
            for node_id, info in self.members.items()
            if node_id != self.node_id
            and node_id != exclude
            and info.state == MemberState.ALIVE
        ]

        k = min(self.config.ping_req_fanout, len(candidates))
        return random.sample(candidates, k) if candidates else []

    async def _handle_probe_result(
        self, result: ProbeResult, ctx: Context
    ) -> None:
        """Handle probe result from ProbeActor."""
        # Record for adaptive timer
        if self.adaptive_timer:
            await self.adaptive_timer.send(
                RecordProbeResult(success=result.success, latency_ms=result.latency_ms)
            )

        if result.success:
            # Update member incarnation if higher
            member = self.members.get(result.target)
            if member and result.incarnation:
                if result.incarnation > member.incarnation:
                    member.incarnation = result.incarnation
                member.update_last_seen()
        else:
            # Probe failed - start suspicion
            await self._start_suspicion(result.target, ctx)

    async def _start_suspicion(self, node_id: str, ctx: Context) -> None:
        """Start Lifeguard suspicion for a member."""
        member_actor = self.member_actors.get(node_id)
        if member_actor:
            await member_actor.send(_MarkSuspected(self.node_id))

        # Publish suspicion via gossip
        await self._publish_gossip(
            GossipType.SUSPECT,
            node_id,
            self.members.get(node_id, MemberInfo(node_id, ("", 0))).incarnation,
        )

        # Send Nack (buddy notification) to suspected node
        nack = Nack(
            suspected=node_id,
            reporters=(self.node_id,),
            first_suspected_at=time.time(),
        )
        await self._send_wire_message(node_id, nack)

    async def _handle_suspicion_timeout(self, node_id: str, ctx: Context) -> None:
        """Suspicion timeout expired - declare member dead."""
        member_actor = self.member_actors.get(node_id)
        if member_actor:
            await member_actor.send(_MarkDead())

        # Remove from probe list
        self.probe_list.remove_member(node_id)

        # Publish dead via gossip
        member = self.members.get(node_id)
        if member:
            await self._publish_gossip(GossipType.DEAD, node_id, member.incarnation)

    async def _handle_member_state_changed(
        self, msg: MemberStateChanged, ctx: Context
    ) -> None:
        """Handle member state change notification."""
        member = self.members.get(msg.node_id)
        if member:
            member.state = msg.new_state
            member.incarnation = msg.incarnation

    async def _handle_leave(self, ctx: Context) -> None:
        """Handle graceful leave request."""
        # Broadcast leave via gossip
        self.incarnation += 1
        await self._publish_gossip(
            GossipType.LEAVE, self.node_id, self.incarnation
        )

        # Stop running
        self.running = False

    async def _handle_incoming_message(
        self, msg: IncomingMessage, ctx: Context
    ) -> None:
        """Handle incoming wire protocol message."""
        match msg.message:
            case Ping(source, source_incarnation, seq, piggyback):
                await self._handle_ping(
                    source, source_incarnation, seq, piggyback, msg.from_address, ctx
                )

            case Ack():
                # Forward to current probe if active
                if self.current_probe:
                    await self.current_probe.send(msg)

            case PingReq(source, target, seq):
                await self._handle_ping_req(source, target, seq, msg.from_address, ctx)

            case IndirectAck():
                # Forward to current probe if active
                if self.current_probe:
                    await self.current_probe.send(msg)

            case Nack(suspected, reporters, first_suspected_at):
                await self._handle_nack(suspected, reporters, first_suspected_at, ctx)

    async def _handle_ping(
        self,
        source: str,
        source_incarnation: int,
        seq: int,
        piggyback: tuple[GossipEntry, ...],
        from_address: tuple[str, int],
        ctx: Context,
    ) -> None:
        """Handle incoming Ping - respond with Ack."""
        # Process piggyback gossip
        for entry in piggyback:
            await self._process_gossip_entry(entry, ctx)

        # Update source member info
        await self._ensure_member(source, from_address, source_incarnation, ctx)

        # Send Ack back to the source node
        ack = Ack(
            source=self.node_id,
            source_incarnation=self.incarnation,
            sequence=seq,
            piggyback=self._get_piggyback_entries(),
        )
        await self._send_wire_message(source, ack)

    async def _handle_ping_req(
        self,
        source: str,
        target: str,
        seq: int,
        from_address: tuple[str, int],
        ctx: Context,
    ) -> None:
        """Handle indirect probe request."""
        # Check if we know about the target
        if target not in self.members:
            return

        # Send Ping to target
        ping = Ping(
            source=self.node_id,
            source_incarnation=self.incarnation,
            sequence=seq,
            piggyback=(),
        )
        await self._send_wire_message(target, ping)

        # TODO: Track pending indirect acks and forward to original source

    async def _handle_nack(
        self,
        suspected: str,
        reporters: tuple[str, ...],
        first_suspected_at: float,
        ctx: Context,
    ) -> None:
        """Handle Nack (buddy notification)."""
        if suspected == self.node_id:
            # We are being suspected - refute by incrementing incarnation
            self.incarnation += 1
            await self._publish_gossip(
                GossipType.ALIVE, self.node_id, self.incarnation
            )

    async def _ensure_member(
        self,
        node_id: str,
        address: tuple[str, int],
        incarnation: int,
        ctx: Context,
    ) -> None:
        """Ensure member exists in our membership list."""
        if node_id == self.node_id:
            return

        if node_id not in self.members:
            # New member discovered
            self.members[node_id] = MemberInfo(
                node_id=node_id,
                address=address,
                state=MemberState.ALIVE,
                incarnation=incarnation,
            )
            self.probe_list.add_member(node_id)

            # Spawn member actor
            member_actor = await ctx.spawn(
                MemberActor,
                node_id=node_id,
                address=address,
                incarnation=incarnation,
                coordinator=ctx.self_ref,
                suspicion_actor=self.suspicion_actor,
            )
            self.member_actors[node_id] = member_actor

            # Broadcast join event
            await self._broadcast_event(MemberJoined(node_id, address))
        else:
            # Update existing member
            member = self.members[node_id]
            if incarnation > member.incarnation:
                member.incarnation = incarnation
                member.address = address
            member.update_last_seen()

    async def _process_gossip_entry(
        self, entry: GossipEntry, ctx: Context
    ) -> None:
        """Process a gossip entry from piggyback."""
        node_id = entry.node_id
        if node_id == self.node_id:
            # Gossip about ourselves
            if entry.type == GossipType.SUSPECT:
                # Someone suspects us - refute
                if entry.incarnation >= self.incarnation:
                    self.incarnation = entry.incarnation + 1
                    await self._publish_gossip(
                        GossipType.ALIVE, self.node_id, self.incarnation
                    )
            return

        member = self.members.get(node_id)
        if member is None and entry.type in (GossipType.ALIVE, GossipType.JOIN):
            # New member
            if entry.address:
                await self._ensure_member(
                    node_id, entry.address, entry.incarnation, ctx
                )
            return

        if member is None:
            return

        # Only process if incarnation is >= current
        if entry.incarnation < member.incarnation:
            return

        match entry.type:
            case GossipType.ALIVE | GossipType.JOIN:
                if member.state != MemberState.ALIVE:
                    member.state = MemberState.ALIVE
                member.incarnation = entry.incarnation
                member.update_last_seen()

            case GossipType.SUSPECT:
                if member.state == MemberState.ALIVE:
                    member.state = MemberState.SUSPECTED
                    # Start local suspicion
                    await self._start_suspicion(node_id, ctx)

            case GossipType.DEAD | GossipType.LEAVE:
                if member.state != MemberState.DEAD:
                    member.state = MemberState.DEAD
                    self.probe_list.remove_member(node_id)
                    await self._broadcast_event(MemberFailed(node_id))

    def _get_piggyback_entries(self) -> tuple[GossipEntry, ...]:
        """Get gossip entries for piggybacking."""
        # For now, return empty - will integrate with gossip manager
        return ()

    async def _publish_gossip(
        self, gossip_type: GossipType, node_id: str, incarnation: int
    ) -> None:
        """Publish gossip entry via gossip manager."""
        if self.gossip_manager is None:
            return

        self.gossip_seq += 1
        # TODO: Integrate with gossip manager using Publish message

    async def _broadcast_event(self, event: MembershipEvent) -> None:
        """Broadcast membership event to subscribers."""
        for subscriber in self.subscribers:
            await subscriber.send(event)

    async def _connect_to_peer(self, address: tuple[str, int]) -> None:
        """Connect to a peer node via TransportMux."""
        if self._transport:
            await self._transport.send(ConnectToAddress(address))

    async def _send_wire_message(
        self, to_node: str, message: Any
    ) -> None:
        """Send a wire protocol message to a node via TransportMux."""
        data = SwimCodec.encode(message)
        await self._send_via_transport(to_node, data)

    async def _handle_tcp_data(
        self, from_address: tuple[str, int], data: bytes, ctx: Context
    ) -> None:
        """Handle raw TCP data - decode and dispatch messages."""
        # Append to receive buffer
        buffer = self.receive_buffers.get(from_address, b"") + data
        self.receive_buffers[from_address] = buffer

        # Decode all complete messages
        messages, remaining = SwimCodec.decode_all(buffer)
        self.receive_buffers[from_address] = remaining

        # Dispatch each message
        for message in messages:
            await self._ctx.self_ref.send(
                IncomingMessage(
                    from_node="",  # Will be filled from message
                    from_address=from_address,
                    message=message,
                )
            )

    async def _handle_transport_incoming(
        self,
        from_node: str,
        from_address: tuple[str, int],
        data: bytes,
        ctx: Context,
    ) -> None:
        """Handle incoming SWIM data from TransportMux."""
        # Decode SWIM messages
        messages, _ = SwimCodec.decode_all(data)

        # Dispatch each message
        for message in messages:
            await self._ctx.self_ref.send(
                IncomingMessage(
                    from_node=from_node,
                    from_address=from_address,
                    message=message,
                )
            )

    async def _handle_peer_connected(
        self,
        node_id: str,
        address: tuple[str, int],
        ctx: Context,
    ) -> None:
        """Handle peer connected event from TransportMux."""
        # Ensure member is in our list
        await self._ensure_member(node_id, address, 0, ctx)

    async def _handle_peer_disconnected(
        self,
        node_id: str,
        reason: str,
        ctx: Context,
    ) -> None:
        """Handle peer disconnected event from TransportMux."""
        # The actual failure handling is done via SWIM protocol
        # This is just informational
        pass

    async def _send_via_transport(
        self,
        to_node: str,
        data: bytes,
    ) -> None:
        """Send SWIM data via TransportMux."""
        if self._transport:
            await self._transport.send(Send(ProtocolType.SWIM, to_node, data))
