"""SWIM membership protocol handler mixin."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from casty import Context
from casty.actor import on
from casty.swim import MemberJoined, MemberFailed, MemberLeft, MemberSuspected, MemberAlive
from casty.transport import ConnectTo, ProtocolType
from ..messages import NodeJoined, NodeFailed, NodeLeft

if TYPE_CHECKING:
    from ..state import ClusterState

log = logging.getLogger(__name__)


class SwimHandlers:
    """Handles SWIM membership protocol events."""

    state: ClusterState

    @on(MemberJoined)
    async def handle_member_joined(self, msg: MemberJoined, ctx: Context) -> None:
        """Handle SWIM member joined event."""
        if msg.node_id == self.state.node_id:
            return

        log.info(f"Member joined: {msg.node_id} at {msg.address}")
        self.state.hash_ring.add_node(msg.node_id)

        # Tell TransportMux to connect
        if self.state.mux:
            await self.state.mux.send(ConnectTo(msg.node_id, msg.address))

        # Notify ReplicationCoordinator of node recovery (for hinted handoff replay)
        if self.state.replication_coordinator:
            await self.state.replication_coordinator.send(NodeJoined(msg.node_id, msg.address))

        await self._broadcast_event(NodeJoined(msg.node_id, msg.address))

    @on(MemberFailed)
    async def handle_member_failed(self, msg: MemberFailed, ctx: Context) -> None:
        """Handle SWIM member failed event."""
        from ..remote_ref import RemoteActorError

        log.warning(f"Member failed: {msg.node_id}")
        self.state.hash_ring.remove_node(msg.node_id)

        # Remove actor locations for this node
        for actor_name in list(self.state.actor_locations.keys()):
            if self.state.actor_locations.get(actor_name) == msg.node_id:
                del self.state.actor_locations[actor_name]
                from ..messages import ActorUnregistered
                await self._broadcast_event(ActorUnregistered(actor_name, msg.node_id))

        # Fail pending asks to this node
        for request_id, pending in list(self.state.pending_asks.items()):
            if pending.target_node == msg.node_id:
                if not pending.future.done():
                    pending.future.set_exception(
                        RemoteActorError(pending.target_actor, pending.target_node, "Node failed")
                    )
                del self.state.pending_asks[request_id]

        # Notify ReplicationCoordinator of node failure (for hinted handoff storage)
        if self.state.replication_coordinator:
            await self.state.replication_coordinator.send(NodeFailed(msg.node_id))

        # Check for singleton failover
        await self._check_singleton_failover(msg.node_id, ctx)

        await self._broadcast_event(NodeFailed(msg.node_id))

    @on(MemberLeft)
    async def handle_member_left(self, msg: MemberLeft, ctx: Context) -> None:
        """Handle SWIM member left event."""
        await self.handle_member_failed(msg, ctx)
        await self._broadcast_event(NodeLeft(msg.node_id))

    @on(MemberSuspected)
    async def handle_member_suspected(self, msg: MemberSuspected, ctx: Context) -> None:
        """Handle SWIM member suspected event.

        A suspected member might still be alive - SWIM is investigating.
        We don't take action yet, just log. If confirmed dead, MemberFailed follows.
        """
        log.debug(f"Member suspected: {msg.node_id}")

    @on(MemberAlive)
    async def handle_member_alive(self, msg: MemberAlive, ctx: Context) -> None:
        """Handle SWIM member alive event.

        A previously suspected member has been confirmed alive.
        No action needed - the member is still in the cluster.
        """
        log.debug(f"Member confirmed alive: {msg.node_id}")

    async def _check_singleton_failover(self, failed_node: str, ctx: Context) -> None:
        """Check if we should take over any singletons from a failed node."""
        # This is implemented in singletons.py mixin
        pass

    async def _broadcast_event(self, event) -> None:
        """Broadcast cluster event to subscribers."""
        # This is implemented in cluster.py
        pass
