"""Data plane router for entity message routing.

Routes messages to actors based on consistent hashing and handles
replication to backup nodes.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .hash_ring import HashRing, HashRingConfig
from .gossip import SWIMProtocol, GossipConfig, MemberStatus

if TYPE_CHECKING:
    from ..transport import Transport

from ...persistence import ReplicationConfig

logger = logging.getLogger(__name__)


@dataclass
class EntityPlacement:
    """Placement information for an entity.

    Attributes:
        entity_type: Type of the entity (actor class name)
        entity_id: Unique identifier for the entity
        primary: Node ID of the primary owner
        replicas: Node IDs of replica holders (excluding primary)
    """

    entity_type: str
    entity_id: str
    primary: str
    replicas: list[str]

    @property
    def all_nodes(self) -> list[str]:
        """Get all nodes holding this entity (primary + replicas)."""
        return [self.primary] + self.replicas


@dataclass
class RoutingResult:
    """Result of a routing operation.

    Attributes:
        success: Whether the operation succeeded
        response: Response from the target (for ask operations)
        error: Error message if failed
    """

    success: bool
    response: Any = None
    error: str | None = None


class DataPlaneRouter:
    """Router for data plane message routing.

    Combines consistent hashing with gossip-based failure detection
    to route messages to the appropriate nodes.

    Features:
    - Consistent hash-based entity placement
    - Automatic failover to replicas
    - Configurable replication
    - Health-aware routing

    Usage:
        router = DataPlaneRouter(
            node_id="node-1",
            address=("localhost", 8001),
        )
        router.set_transport(transport)
        await router.start()

        # Get placement for an entity
        placement = router.get_placement("User", "user-123")

        # Route a message
        result = await router.route_message("User", "user-123", msg)
    """

    def __init__(
        self,
        node_id: str,
        address: tuple[str, int],
        *,
        hash_config: HashRingConfig | None = None,
        gossip_config: GossipConfig | None = None,
        default_replication_factor: int = 3,
    ):
        self._node_id = node_id
        self._address = address
        self._default_rf = default_replication_factor

        # Hash ring for entity placement
        self._hash_ring = HashRing(hash_config)
        self._hash_ring.add_node(node_id)  # Add ourselves

        # Gossip for failure detection
        self._gossip = SWIMProtocol(node_id, address, gossip_config)
        self._gossip.on_member_alive = self._on_member_alive
        self._gossip.on_member_dead = self._on_member_dead

        # Transport for message sending
        self._transport: Transport | None = None

        # Node addresses
        self._addresses: dict[str, tuple[str, int]] = {node_id: address}

        # Entity type configurations
        self._entity_configs: dict[str, int] = {}  # entity_type -> replication_factor

        # Running state
        self._running = False

    @property
    def node_id(self) -> str:
        """Get this node's ID."""
        return self._node_id

    @property
    def hash_ring(self) -> HashRing:
        """Get the hash ring."""
        return self._hash_ring

    @property
    def gossip(self) -> SWIMProtocol:
        """Get the gossip protocol."""
        return self._gossip

    def set_transport(self, transport: Transport) -> None:
        """Set the transport for message sending.

        Args:
            transport: Transport instance
        """
        self._transport = transport
        self._gossip.set_transport(transport)

    def configure_entity_type(
        self,
        entity_type: str,
        replication_factor: int,
    ) -> None:
        """Configure replication for an entity type.

        Args:
            entity_type: Name of the entity type
            replication_factor: Number of replicas (including primary)
        """
        self._entity_configs[entity_type] = replication_factor
        logger.debug(f"Configured {entity_type} with RF={replication_factor}")

    def add_node(self, node_id: str, address: tuple[str, int]) -> None:
        """Add a node to the cluster.

        Args:
            node_id: Unique identifier for the node
            address: Network address as (host, port)
        """
        if node_id == self._node_id:
            return

        self._addresses[node_id] = address
        moves = self._hash_ring.add_node(node_id)
        self._gossip.add_member(node_id, address)

        logger.info(f"Added node {node_id} at {address}, {len(moves)} key ranges affected")

    def remove_node(self, node_id: str) -> None:
        """Remove a node from the cluster.

        Args:
            node_id: The node to remove
        """
        if node_id == self._node_id:
            return

        moves = self._hash_ring.remove_node(node_id)
        self._gossip.remove_member(node_id)
        self._addresses.pop(node_id, None)

        logger.info(f"Removed node {node_id}, {len(moves)} key ranges affected")

    def get_placement(
        self,
        entity_type: str,
        entity_id: str,
    ) -> EntityPlacement:
        """Get placement information for an entity.

        Args:
            entity_type: Type of the entity
            entity_id: Unique identifier for the entity

        Returns:
            EntityPlacement with primary and replica nodes
        """
        # Compute key for hashing
        key = f"{entity_type}:{entity_id}"

        # Get replication factor for this entity type
        rf = self._entity_configs.get(entity_type, self._default_rf)

        # Get preference list from hash ring
        nodes = self._hash_ring.get_preference_list(key, n=rf)

        if not nodes:
            # No nodes available - return ourselves as fallback
            return EntityPlacement(
                entity_type=entity_type,
                entity_id=entity_id,
                primary=self._node_id,
                replicas=[],
            )

        return EntityPlacement(
            entity_type=entity_type,
            entity_id=entity_id,
            primary=nodes[0],
            replicas=nodes[1:],
        )

    def is_responsible(
        self,
        entity_type: str,
        entity_id: str,
    ) -> bool:
        """Check if this node is responsible for an entity.

        A node is responsible if it's the primary or a replica.

        Args:
            entity_type: Type of the entity
            entity_id: Unique identifier for the entity

        Returns:
            True if this node holds the entity
        """
        placement = self.get_placement(entity_type, entity_id)
        return self._node_id in placement.all_nodes

    def is_primary(
        self,
        entity_type: str,
        entity_id: str,
    ) -> bool:
        """Check if this node is the primary for an entity.

        Args:
            entity_type: Type of the entity
            entity_id: Unique identifier for the entity

        Returns:
            True if this node is the primary owner
        """
        placement = self.get_placement(entity_type, entity_id)
        return self._node_id == placement.primary

    def get_healthy_nodes(self) -> list[str]:
        """Get list of healthy node IDs.

        Returns:
            List of node IDs that are alive
        """
        alive = self._gossip.get_alive_members()
        return [self._node_id] + alive

    def get_node_address(self, node_id: str) -> tuple[str, int] | None:
        """Get network address for a node.

        Args:
            node_id: The node to look up

        Returns:
            Address as (host, port) or None if unknown
        """
        return self._addresses.get(node_id)

    async def route_message(
        self,
        entity_type: str,
        entity_id: str,
        message: Any,
        *,
        prefer_local: bool = True,
    ) -> RoutingResult:
        """Route a message to an entity's primary node.

        Args:
            entity_type: Type of the entity
            entity_id: Unique identifier for the entity
            message: Message to send
            prefer_local: If True, handle locally if we're responsible

        Returns:
            RoutingResult indicating success/failure
        """
        placement = self.get_placement(entity_type, entity_id)

        # Check if we can handle locally
        if prefer_local and self._node_id == placement.primary:
            return RoutingResult(
                success=True,
                response=None,  # Local handling - no routing needed
            )

        # Need to route to remote node
        if not self._transport:
            return RoutingResult(
                success=False,
                error="No transport configured",
            )

        # Try primary first
        address = self.get_node_address(placement.primary)
        if address:
            try:
                await self._transport.send_entity_message(
                    address,
                    entity_type,
                    entity_id,
                    message,
                )
                return RoutingResult(success=True)
            except Exception as e:
                logger.warning(f"Failed to route to primary {placement.primary}: {e}")

        # Primary failed, try replicas
        for replica_id in placement.replicas:
            address = self.get_node_address(replica_id)
            if not address:
                continue

            try:
                await self._transport.send_entity_message(
                    address,
                    entity_type,
                    entity_id,
                    message,
                )
                return RoutingResult(success=True)
            except Exception as e:
                logger.warning(f"Failed to route to replica {replica_id}: {e}")

        return RoutingResult(
            success=False,
            error=f"Failed to route to any node for {entity_type}:{entity_id}",
        )

    async def route_ask(
        self,
        entity_type: str,
        entity_id: str,
        message: Any,
        *,
        timeout: float = 5.0,
    ) -> RoutingResult:
        """Route a request-response message to an entity.

        Args:
            entity_type: Type of the entity
            entity_id: Unique identifier for the entity
            message: Message to send
            timeout: Response timeout in seconds

        Returns:
            RoutingResult with response or error
        """
        placement = self.get_placement(entity_type, entity_id)

        # Check if we can handle locally
        if self._node_id == placement.primary:
            return RoutingResult(
                success=True,
                response=None,  # Local handling marker
            )

        if not self._transport:
            return RoutingResult(
                success=False,
                error="No transport configured",
            )

        # Try primary first
        address = self.get_node_address(placement.primary)
        if address:
            try:
                response = await self._transport.ask_entity(
                    address,
                    entity_type,
                    entity_id,
                    message,
                    timeout=timeout,
                )
                return RoutingResult(success=True, response=response)
            except Exception as e:
                logger.warning(f"Ask to primary {placement.primary} failed: {e}")

        # Primary failed, try replicas
        for replica_id in placement.replicas:
            address = self.get_node_address(replica_id)
            if not address:
                continue

            try:
                response = await self._transport.ask_entity(
                    address,
                    entity_type,
                    entity_id,
                    message,
                    timeout=timeout,
                )
                return RoutingResult(success=True, response=response)
            except Exception as e:
                logger.warning(f"Ask to replica {replica_id} failed: {e}")

        return RoutingResult(
            success=False,
            error=f"Failed to route ask to any node for {entity_type}:{entity_id}",
        )

    async def replicate_state(
        self,
        entity_type: str,
        entity_id: str,
        state: bytes,
        *,
        config: ReplicationConfig | None = None,
    ) -> bool:
        """Replicate entity state to backup nodes.

        Args:
            entity_type: Type of the entity
            entity_id: Unique identifier for the entity
            state: Serialized state to replicate
            config: Replication configuration

        Returns:
            True if replication succeeded according to config
        """
        if not self._transport:
            return False

        # Use default config if not provided
        if config is None:
            config = ReplicationConfig()

        placement = self.get_placement(entity_type, entity_id)

        # Only primary should replicate
        if self._node_id != placement.primary:
            return True  # Not our responsibility

        if not placement.replicas:
            return True  # No replicas configured

        # Determine if we need to wait for ACKs
        wait_ack = config.is_sync

        # Replicate to each replica
        tasks = []
        for replica_id in placement.replicas:
            address = self.get_node_address(replica_id)
            if address:
                tasks.append(
                    self._transport.send_state_update(
                        address,
                        entity_type,
                        entity_id,
                        state,
                        wait_ack=wait_ack,
                        timeout=config.timeout,
                    )
                )

        if not tasks:
            return True  # No reachable replicas

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # For async mode, always succeed if we sent
        if not wait_ack:
            return True

        # Count successes for sync mode
        successes = sum(1 for r in results if r is True)
        required = config.required_acks(len(tasks))

        return successes >= required

    async def start(self) -> None:
        """Start the router and its components."""
        if self._running:
            return

        self._running = True
        await self._gossip.start()
        logger.info(f"DataPlaneRouter started for {self._node_id}")

    async def stop(self) -> None:
        """Stop the router and its components."""
        self._running = False
        await self._gossip.stop()
        logger.info(f"DataPlaneRouter stopped for {self._node_id}")

    async def _on_member_alive(self, node_id: str) -> None:
        """Handle member becoming alive."""
        logger.info(f"Member {node_id} is alive")
        # Node is already in hash ring from add_node
        # This is called when a suspect node comes back

    async def _on_member_dead(self, node_id: str) -> None:
        """Handle member failure."""
        logger.warning(f"Member {node_id} is dead, removing from hash ring")
        self._hash_ring.remove_node(node_id)
        # Don't remove from addresses yet - gossip will clean up later

    def __repr__(self) -> str:
        nodes = self._hash_ring.node_count
        healthy = len(self.get_healthy_nodes())
        return f"DataPlaneRouter(node={self._node_id}, nodes={healthy}/{nodes})"
