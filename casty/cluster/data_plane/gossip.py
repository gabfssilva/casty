"""SWIM gossip protocol for failure detection.

Implements the Scalable Weakly-consistent Infection-style Process
Group Membership (SWIM) protocol for detecting node failures with
low false-positive rates and bounded detection time.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Callable, Awaitable

if TYPE_CHECKING:
    from ..transport import Transport

logger = logging.getLogger(__name__)


class MemberStatus(Enum):
    """Status of a cluster member."""

    ALIVE = auto()
    SUSPECT = auto()
    DEAD = auto()


@dataclass
class MemberInfo:
    """Information about a cluster member.

    Attributes:
        node_id: Unique identifier for the node
        address: Network address as (host, port)
        status: Current membership status
        incarnation: Monotonic version number for conflict resolution
        last_seen: Timestamp of last successful contact
    """

    node_id: str
    address: tuple[str, int]
    status: MemberStatus = MemberStatus.ALIVE
    incarnation: int = 0
    last_seen: float = field(default_factory=time.time)


@dataclass
class GossipConfig:
    """Configuration for the SWIM gossip protocol.

    Attributes:
        probe_interval: Time between probe cycles (seconds)
        probe_timeout: Timeout for direct probe response (seconds)
        indirect_probes: Number of indirect probes to attempt
        suspicion_multiplier: Multiplier for suspicion timeout
        gossip_fanout: Number of nodes to gossip to per round
        dead_node_timeout: Time before removing dead nodes (seconds)
    """

    probe_interval: float = 1.0
    probe_timeout: float = 0.5
    indirect_probes: int = 3
    suspicion_multiplier: int = 5
    gossip_fanout: int = 3
    dead_node_timeout: float = 60.0


@dataclass
class GossipMessage:
    """Base class for gossip protocol messages."""

    sender_id: str
    incarnation: int


@dataclass
class Ping(GossipMessage):
    """Direct probe message."""

    pass


@dataclass
class PingReq(GossipMessage):
    """Indirect probe request.

    Asks recipient to probe target on behalf of sender.
    """

    target_id: str


@dataclass
class Ack(GossipMessage):
    """Acknowledgment response to Ping or PingReq."""

    # For PingReq acks, this is the original target
    target_id: str | None = None


@dataclass
class MembershipUpdate:
    """Membership state change to be gossiped.

    Attributes:
        node_id: The node whose status changed
        status: New status
        incarnation: Version number for conflict resolution
        address: Network address (included for new members)
    """

    node_id: str
    status: MemberStatus
    incarnation: int
    address: tuple[str, int] | None = None


class SWIMProtocol:
    """SWIM gossip protocol implementation.

    Features:
    - Direct probing with configurable timeout
    - Indirect probing through random peers
    - Suspicion mechanism to reduce false positives
    - Piggybacked membership updates for efficiency

    Usage:
        gossip = SWIMProtocol(node_id="node-1", config=GossipConfig())
        gossip.on_member_alive = handle_alive
        gossip.on_member_suspect = handle_suspect
        gossip.on_member_dead = handle_dead

        await gossip.start()
        # ... later
        await gossip.stop()
    """

    def __init__(
        self,
        node_id: str,
        address: tuple[str, int],
        config: GossipConfig | None = None,
    ):
        self._node_id = node_id
        self._address = address
        self._config = config or GossipConfig()

        self._members: dict[str, MemberInfo] = {}
        self._incarnation = 0
        self._running = False
        self._probe_task: asyncio.Task | None = None

        # Pending indirect probe requests
        self._pending_acks: dict[str, asyncio.Future[bool]] = {}

        # Suspicion timers
        self._suspect_timers: dict[str, asyncio.Task] = {}

        # Updates to piggyback on messages
        self._pending_updates: list[MembershipUpdate] = []

        # Callbacks
        self.on_member_alive: Callable[[str], Awaitable[None]] | None = None
        self.on_member_suspect: Callable[[str], Awaitable[None]] | None = None
        self.on_member_dead: Callable[[str], Awaitable[None]] | None = None

        # Transport will be set externally
        self._transport: Transport | None = None

    @property
    def node_id(self) -> str:
        """Get this node's ID."""
        return self._node_id

    @property
    def incarnation(self) -> int:
        """Get current incarnation number."""
        return self._incarnation

    def set_transport(self, transport: Transport) -> None:
        """Set the transport for sending messages."""
        self._transport = transport

    def add_member(
        self,
        node_id: str,
        address: tuple[str, int],
        incarnation: int = 0,
    ) -> None:
        """Add a new member to the cluster.

        Args:
            node_id: Unique identifier for the node
            address: Network address as (host, port)
            incarnation: Initial incarnation number
        """
        if node_id == self._node_id:
            return  # Don't track ourselves

        if node_id in self._members:
            # Update existing member if incarnation is higher
            member = self._members[node_id]
            if incarnation > member.incarnation:
                member.incarnation = incarnation
                member.address = address
                member.status = MemberStatus.ALIVE
                member.last_seen = time.time()
        else:
            self._members[node_id] = MemberInfo(
                node_id=node_id,
                address=address,
                status=MemberStatus.ALIVE,
                incarnation=incarnation,
                last_seen=time.time(),
            )
            logger.debug(f"Added member {node_id} at {address}")

        # Queue update for gossip
        self._queue_update(MembershipUpdate(
            node_id=node_id,
            status=MemberStatus.ALIVE,
            incarnation=incarnation,
            address=address,
        ))

    def remove_member(self, node_id: str) -> None:
        """Remove a member from the cluster.

        Args:
            node_id: The node to remove
        """
        if node_id in self._members:
            del self._members[node_id]
            logger.debug(f"Removed member {node_id}")

        # Cancel any pending suspicion timer
        if node_id in self._suspect_timers:
            self._suspect_timers[node_id].cancel()
            del self._suspect_timers[node_id]

    def get_alive_members(self) -> list[str]:
        """Get list of alive member IDs.

        Returns:
            List of node IDs with ALIVE status
        """
        return [
            node_id
            for node_id, member in self._members.items()
            if member.status == MemberStatus.ALIVE
        ]

    def get_all_members(self) -> dict[str, MemberInfo]:
        """Get all members with their status.

        Returns:
            Dictionary of node_id -> MemberInfo
        """
        return dict(self._members)

    async def start(self) -> None:
        """Start the gossip protocol."""
        if self._running:
            return

        self._running = True
        self._probe_task = asyncio.create_task(self._probe_loop())
        logger.info(f"SWIM protocol started for {self._node_id}")

    async def stop(self) -> None:
        """Stop the gossip protocol."""
        self._running = False

        if self._probe_task:
            self._probe_task.cancel()
            try:
                await self._probe_task
            except asyncio.CancelledError:
                pass
            self._probe_task = None

        # Cancel all suspicion timers
        for timer in self._suspect_timers.values():
            timer.cancel()
        self._suspect_timers.clear()

        # Cancel all pending acks
        for fut in self._pending_acks.values():
            fut.cancel()
        self._pending_acks.clear()

        logger.info(f"SWIM protocol stopped for {self._node_id}")

    async def _probe_loop(self) -> None:
        """Main probe loop - runs periodically."""
        while self._running:
            try:
                await self._probe_cycle()
                await asyncio.sleep(self._config.probe_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in probe cycle: {e}")
                await asyncio.sleep(self._config.probe_interval)

    async def _probe_cycle(self) -> None:
        """Execute one probe cycle.

        Selects a random member and probes it. If direct probe fails,
        attempts indirect probing through other members.
        """
        alive_members = self.get_alive_members()
        if not alive_members:
            return

        # Select random member to probe
        target_id = random.choice(alive_members)
        target = self._members[target_id]

        logger.debug(f"Probing {target_id}")

        # Try direct probe
        if await self._direct_probe(target_id, target.address):
            self._mark_alive(target_id)
            return

        # Direct probe failed, try indirect
        logger.debug(f"Direct probe to {target_id} failed, trying indirect")

        other_members = [m for m in alive_members if m != target_id]
        if not other_members:
            # No other members to ask
            self._mark_suspect(target_id)
            return

        # Select random members for indirect probe
        k = min(self._config.indirect_probes, len(other_members))
        intermediaries = random.sample(other_members, k)

        # Send indirect probe requests in parallel
        results = await asyncio.gather(
            *[
                self._indirect_probe(inter_id, target_id)
                for inter_id in intermediaries
            ],
            return_exceptions=True,
        )

        # Check if any indirect probe succeeded
        if any(r is True for r in results):
            self._mark_alive(target_id)
        else:
            self._mark_suspect(target_id)

    async def _direct_probe(
        self,
        target_id: str,
        address: tuple[str, int],
    ) -> bool:
        """Send direct probe to a node.

        Args:
            target_id: ID of node to probe
            address: Network address of target

        Returns:
            True if probe succeeded (got ACK), False otherwise
        """
        if not self._transport:
            return False

        try:
            # Create a future to wait for ACK
            ack_key = f"direct:{target_id}"
            fut: asyncio.Future[bool] = asyncio.get_event_loop().create_future()
            self._pending_acks[ack_key] = fut

            # Send ping
            ping = Ping(sender_id=self._node_id, incarnation=self._incarnation)
            await self._transport.send_gossip(address, ping, self._get_updates())

            # Wait for ACK with timeout
            try:
                result = await asyncio.wait_for(
                    fut,
                    timeout=self._config.probe_timeout,
                )
                return result
            except asyncio.TimeoutError:
                return False
            finally:
                self._pending_acks.pop(ack_key, None)

        except Exception as e:
            logger.debug(f"Direct probe to {target_id} failed: {e}")
            return False

    async def _indirect_probe(
        self,
        intermediary_id: str,
        target_id: str,
    ) -> bool:
        """Request indirect probe through an intermediary.

        Args:
            intermediary_id: ID of node to send PingReq to
            target_id: ID of node to be probed

        Returns:
            True if target responded (via intermediary), False otherwise
        """
        if not self._transport:
            return False

        intermediary = self._members.get(intermediary_id)
        if not intermediary:
            return False

        try:
            # Create a future to wait for indirect ACK
            ack_key = f"indirect:{target_id}:{intermediary_id}"
            fut: asyncio.Future[bool] = asyncio.get_event_loop().create_future()
            self._pending_acks[ack_key] = fut

            # Send ping request
            ping_req = PingReq(
                sender_id=self._node_id,
                incarnation=self._incarnation,
                target_id=target_id,
            )
            await self._transport.send_gossip(
                intermediary.address,
                ping_req,
                self._get_updates(),
            )

            # Wait for ACK with timeout
            try:
                result = await asyncio.wait_for(
                    fut,
                    timeout=self._config.probe_timeout * 2,  # More time for indirect
                )
                return result
            except asyncio.TimeoutError:
                return False
            finally:
                self._pending_acks.pop(ack_key, None)

        except Exception as e:
            logger.debug(f"Indirect probe via {intermediary_id} failed: {e}")
            return False

    async def handle_ping(
        self,
        ping: Ping,
        from_address: tuple[str, int],
    ) -> None:
        """Handle incoming Ping message.

        Args:
            ping: The ping message
            from_address: Address of sender
        """
        if not self._transport:
            return

        # Update member info
        if ping.sender_id in self._members:
            member = self._members[ping.sender_id]
            member.last_seen = time.time()
            if ping.incarnation > member.incarnation:
                member.incarnation = ping.incarnation

        # Send ACK
        ack = Ack(sender_id=self._node_id, incarnation=self._incarnation)
        await self._transport.send_gossip(from_address, ack, self._get_updates())

    async def handle_ping_req(
        self,
        ping_req: PingReq,
        from_address: tuple[str, int],
    ) -> None:
        """Handle incoming PingReq message.

        Probes the target on behalf of the requester.

        Args:
            ping_req: The ping request message
            from_address: Address of requester
        """
        if not self._transport:
            return

        target = self._members.get(ping_req.target_id)
        if not target:
            return

        # Probe the target
        success = await self._direct_probe(ping_req.target_id, target.address)

        # Send ACK back to requester with result
        ack = Ack(
            sender_id=self._node_id,
            incarnation=self._incarnation,
            target_id=ping_req.target_id if success else None,
        )
        await self._transport.send_gossip(from_address, ack, self._get_updates())

    async def handle_ack(
        self,
        ack: Ack,
        from_address: tuple[str, int],
    ) -> None:
        """Handle incoming Ack message.

        Args:
            ack: The acknowledgment message
            from_address: Address of sender
        """
        # Update member info
        if ack.sender_id in self._members:
            member = self._members[ack.sender_id]
            member.last_seen = time.time()
            if ack.incarnation > member.incarnation:
                member.incarnation = ack.incarnation

        # Resolve pending direct probe
        direct_key = f"direct:{ack.sender_id}"
        if direct_key in self._pending_acks:
            self._pending_acks[direct_key].set_result(True)

        # Resolve pending indirect probe if target is specified
        if ack.target_id:
            # Find matching indirect probe request
            for key, fut in list(self._pending_acks.items()):
                if key.startswith(f"indirect:{ack.target_id}:"):
                    if not fut.done():
                        fut.set_result(True)
                    break

    async def handle_membership_updates(
        self,
        updates: list[MembershipUpdate],
    ) -> None:
        """Process piggybacked membership updates.

        Args:
            updates: List of membership updates to process
        """
        for update in updates:
            if update.node_id == self._node_id:
                # Someone thinks we're suspect/dead, refute it
                if update.status in (MemberStatus.SUSPECT, MemberStatus.DEAD):
                    self._refute(update.incarnation)
                continue

            member = self._members.get(update.node_id)

            if update.status == MemberStatus.ALIVE:
                if member:
                    if update.incarnation > member.incarnation:
                        member.incarnation = update.incarnation
                        member.status = MemberStatus.ALIVE
                        member.last_seen = time.time()
                        # Cancel suspicion timer if exists
                        self._cancel_suspect_timer(update.node_id)
                elif update.address:
                    # New member
                    self.add_member(
                        update.node_id,
                        update.address,
                        update.incarnation,
                    )
                    if self.on_member_alive:
                        asyncio.create_task(self.on_member_alive(update.node_id))

            elif update.status == MemberStatus.SUSPECT:
                if member and update.incarnation >= member.incarnation:
                    if member.status == MemberStatus.ALIVE:
                        self._mark_suspect(update.node_id, update.incarnation)

            elif update.status == MemberStatus.DEAD:
                if member:
                    await self._mark_dead(update.node_id)

    def _mark_alive(self, node_id: str) -> None:
        """Mark a node as alive."""
        member = self._members.get(node_id)
        if not member:
            return

        was_suspect = member.status == MemberStatus.SUSPECT
        member.status = MemberStatus.ALIVE
        member.last_seen = time.time()

        # Cancel suspicion timer
        self._cancel_suspect_timer(node_id)

        if was_suspect:
            logger.info(f"Node {node_id} is now ALIVE (was suspect)")
            if self.on_member_alive:
                asyncio.create_task(self.on_member_alive(node_id))

    def _mark_suspect(
        self,
        node_id: str,
        incarnation: int | None = None,
    ) -> None:
        """Mark a node as suspect."""
        member = self._members.get(node_id)
        if not member:
            return

        if member.status != MemberStatus.ALIVE:
            return  # Already suspect or dead

        member.status = MemberStatus.SUSPECT
        if incarnation is not None:
            member.incarnation = incarnation

        logger.info(f"Node {node_id} is now SUSPECT")

        # Queue update for gossip
        self._queue_update(MembershipUpdate(
            node_id=node_id,
            status=MemberStatus.SUSPECT,
            incarnation=member.incarnation,
        ))

        if self.on_member_suspect:
            asyncio.create_task(self.on_member_suspect(node_id))

        # Start suspicion timer
        timeout = (
            self._config.probe_interval
            * self._config.suspicion_multiplier
        )
        self._suspect_timers[node_id] = asyncio.create_task(
            self._suspicion_timeout(node_id, timeout)
        )

    async def _mark_dead(self, node_id: str) -> None:
        """Mark a node as dead."""
        member = self._members.get(node_id)
        if not member:
            return

        if member.status == MemberStatus.DEAD:
            return  # Already dead

        member.status = MemberStatus.DEAD
        logger.warning(f"Node {node_id} is now DEAD")

        # Cancel suspicion timer
        self._cancel_suspect_timer(node_id)

        # Queue update for gossip
        self._queue_update(MembershipUpdate(
            node_id=node_id,
            status=MemberStatus.DEAD,
            incarnation=member.incarnation,
        ))

        if self.on_member_dead:
            await self.on_member_dead(node_id)

        # Schedule removal of dead node
        asyncio.create_task(self._remove_dead_node(node_id))

    async def _suspicion_timeout(self, node_id: str, timeout: float) -> None:
        """Timer that marks a node dead after suspicion timeout."""
        try:
            await asyncio.sleep(timeout)
            member = self._members.get(node_id)
            if member and member.status == MemberStatus.SUSPECT:
                await self._mark_dead(node_id)
        except asyncio.CancelledError:
            pass

    async def _remove_dead_node(self, node_id: str) -> None:
        """Remove a dead node after timeout."""
        await asyncio.sleep(self._config.dead_node_timeout)
        member = self._members.get(node_id)
        if member and member.status == MemberStatus.DEAD:
            self.remove_member(node_id)

    def _cancel_suspect_timer(self, node_id: str) -> None:
        """Cancel suspicion timer for a node."""
        if node_id in self._suspect_timers:
            self._suspect_timers[node_id].cancel()
            del self._suspect_timers[node_id]

    def _refute(self, claimed_incarnation: int) -> None:
        """Refute a suspicion/death claim about ourselves.

        Increments incarnation to prove we're alive.
        """
        if claimed_incarnation >= self._incarnation:
            self._incarnation = claimed_incarnation + 1
            logger.info(f"Refuting suspicion, new incarnation: {self._incarnation}")

            # Queue alive update with new incarnation
            self._queue_update(MembershipUpdate(
                node_id=self._node_id,
                status=MemberStatus.ALIVE,
                incarnation=self._incarnation,
                address=self._address,
            ))

    def _queue_update(self, update: MembershipUpdate) -> None:
        """Queue a membership update for piggybacking."""
        # Limit queue size, remove old updates
        max_updates = 50
        if len(self._pending_updates) >= max_updates:
            self._pending_updates = self._pending_updates[-(max_updates // 2):]

        self._pending_updates.append(update)

    def _get_updates(self) -> list[MembershipUpdate]:
        """Get pending updates to piggyback on a message.

        Returns a subset of updates and marks them for eventual removal.
        """
        # Return up to fanout updates
        count = min(self._config.gossip_fanout * 2, len(self._pending_updates))
        if count == 0:
            return []

        # Prioritize recent updates
        updates = self._pending_updates[-count:]
        return updates

    def __repr__(self) -> str:
        alive = len(self.get_alive_members())
        total = len(self._members)
        return f"SWIMProtocol(node={self._node_id}, members={alive}/{total})"
