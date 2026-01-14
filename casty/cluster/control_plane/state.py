"""Cluster state managed by the control plane.

This state is replicated via Raft consensus across all nodes.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal

from .commands import (
    ControlPlaneCommand,
    JoinCluster,
    LeaveCluster,
    NodeFailed,
    NodeRecovered,
    RegisterSingleton,
    UnregisterSingleton,
    TransferSingleton,
    RegisterEntityType,
    UnregisterEntityType,
    UpdateEntityTypeConfig,
    RegisterNamedActor,
    UnregisterNamedActor,
)


NodeStatus = Literal["joining", "active", "leaving", "failed"]
SingletonStatus = Literal["active", "orphan", "migrating"]


@dataclass
class NodeInfo:
    """Information about a cluster node.

    Attributes:
        node_id: Unique identifier
        address: Network address as (host, port)
        status: Current node status
        joined_at: Timestamp when node joined
        failed_at: Timestamp when node was marked failed (if applicable)
    """

    node_id: str
    address: tuple[str, int]
    status: NodeStatus = "joining"
    joined_at: float = field(default_factory=time.time)
    failed_at: float | None = None


@dataclass
class SingletonInfo:
    """Information about a singleton actor.

    Attributes:
        name: Unique name of the singleton
        node_id: Node currently hosting the singleton
        actor_class: Fully qualified class name
        version: Monotonic version for conflict resolution
        status: Current singleton status
    """

    name: str
    node_id: str
    actor_class: str
    version: int
    status: SingletonStatus = "active"


@dataclass
class EntityTypeInfo:
    """Information about a sharded entity type.

    Attributes:
        entity_type: Name of the entity type
        actor_class: Fully qualified class name
        replication_factor: Number of replicas (including primary)
        registered_at: When the entity type was registered
    """

    entity_type: str
    actor_class: str
    replication_factor: int
    registered_at: float = field(default_factory=time.time)


@dataclass
class NamedActorInfo:
    """Information about a named actor.

    Attributes:
        name: Unique name
        node_id: Node hosting the actor
        actor_id: Local actor ID on that node
        registered_at: When the actor was registered
    """

    name: str
    node_id: str
    actor_id: str
    registered_at: float = field(default_factory=time.time)


class ClusterState:
    """Replicated cluster state.

    This state is maintained consistently across all nodes via Raft.
    It includes:
    - Cluster membership
    - Singleton registry
    - Entity type configurations
    - Named actor registry

    Usage:
        state = ClusterState()
        state.apply(JoinCluster(node_id="node-1", ...))
        state.apply(RegisterSingleton(name="cache", ...))

        # Query state
        active = state.get_active_members()
        orphans = state.get_orphan_singletons()
    """

    def __init__(self):
        self._members: dict[str, NodeInfo] = {}
        self._singletons: dict[str, SingletonInfo] = {}
        self._entity_types: dict[str, EntityTypeInfo] = {}
        self._named_actors: dict[str, NamedActorInfo] = {}

    def apply(self, command: ControlPlaneCommand) -> None:
        """Apply a command to update state.

        Args:
            command: The command to apply
        """
        match command:
            # Membership commands
            case JoinCluster(node_id, address, timestamp):
                self._apply_join(node_id, address, timestamp)

            case LeaveCluster(node_id, timestamp):
                self._apply_leave(node_id, timestamp)

            case NodeFailed(node_id, detected_by, timestamp):
                self._apply_failed(node_id, timestamp)

            case NodeRecovered(node_id, address, timestamp):
                self._apply_recovered(node_id, address, timestamp)

            # Singleton commands
            case RegisterSingleton(name, node_id, actor_class, version):
                self._apply_register_singleton(name, node_id, actor_class, version)

            case UnregisterSingleton(name):
                self._apply_unregister_singleton(name)

            case TransferSingleton(name, from_node, to_node):
                self._apply_transfer_singleton(name, from_node, to_node)

            # Entity type commands
            case RegisterEntityType(entity_type, actor_class, replication_factor):
                self._apply_register_entity_type(
                    entity_type, actor_class, replication_factor
                )

            case UnregisterEntityType(entity_type):
                self._apply_unregister_entity_type(entity_type)

            case UpdateEntityTypeConfig(entity_type, replication_factor):
                self._apply_update_entity_type(entity_type, replication_factor)

            # Named actor commands
            case RegisterNamedActor(name, node_id, actor_id):
                self._apply_register_named_actor(name, node_id, actor_id)

            case UnregisterNamedActor(name):
                self._apply_unregister_named_actor(name)

    # Membership apply methods

    def _apply_join(
        self,
        node_id: str,
        address: tuple[str, int],
        timestamp: float,
    ) -> None:
        """Apply a join command."""
        if node_id in self._members:
            # Node rejoining - update address and status
            member = self._members[node_id]
            member.address = address
            member.status = "active"
            member.failed_at = None
        else:
            # New node
            self._members[node_id] = NodeInfo(
                node_id=node_id,
                address=address,
                status="active",
                joined_at=timestamp,
            )

    def _apply_leave(self, node_id: str, timestamp: float) -> None:
        """Apply a leave command."""
        if node_id in self._members:
            member = self._members[node_id]
            member.status = "leaving"
            # Mark singletons on this node as orphan
            self._orphan_singletons_on_node(node_id)

    def _apply_failed(self, node_id: str, timestamp: float) -> None:
        """Apply a node failed command."""
        if node_id in self._members:
            member = self._members[node_id]
            member.status = "failed"
            member.failed_at = timestamp
            # Mark singletons on this node as orphan
            self._orphan_singletons_on_node(node_id)

    def _apply_recovered(
        self,
        node_id: str,
        address: tuple[str, int],
        timestamp: float,
    ) -> None:
        """Apply a node recovered command."""
        if node_id in self._members:
            member = self._members[node_id]
            member.address = address
            member.status = "active"
            member.failed_at = None

    def _orphan_singletons_on_node(self, node_id: str) -> None:
        """Mark all singletons on a node as orphan."""
        for singleton in self._singletons.values():
            if singleton.node_id == node_id and singleton.status == "active":
                singleton.status = "orphan"

    # Singleton apply methods

    def _apply_register_singleton(
        self,
        name: str,
        node_id: str,
        actor_class: str,
        version: int,
    ) -> None:
        """Apply a register singleton command."""
        existing = self._singletons.get(name)
        if existing and existing.version >= version:
            # Existing registration has higher or equal version
            return

        self._singletons[name] = SingletonInfo(
            name=name,
            node_id=node_id,
            actor_class=actor_class,
            version=version,
            status="active",
        )

    def _apply_unregister_singleton(self, name: str) -> None:
        """Apply an unregister singleton command."""
        self._singletons.pop(name, None)

    def _apply_transfer_singleton(
        self,
        name: str,
        from_node: str,
        to_node: str,
    ) -> None:
        """Apply a transfer singleton command."""
        singleton = self._singletons.get(name)
        if singleton and singleton.node_id == from_node:
            singleton.node_id = to_node
            singleton.version += 1
            singleton.status = "active"

    # Entity type apply methods

    def _apply_register_entity_type(
        self,
        entity_type: str,
        actor_class: str,
        replication_factor: int,
    ) -> None:
        """Apply a register entity type command."""
        self._entity_types[entity_type] = EntityTypeInfo(
            entity_type=entity_type,
            actor_class=actor_class,
            replication_factor=replication_factor,
        )

    def _apply_unregister_entity_type(self, entity_type: str) -> None:
        """Apply an unregister entity type command."""
        self._entity_types.pop(entity_type, None)

    def _apply_update_entity_type(
        self,
        entity_type: str,
        replication_factor: int,
    ) -> None:
        """Apply an update entity type command."""
        if entity_type in self._entity_types:
            self._entity_types[entity_type].replication_factor = replication_factor

    # Named actor apply methods

    def _apply_register_named_actor(
        self,
        name: str,
        node_id: str,
        actor_id: str,
    ) -> None:
        """Apply a register named actor command."""
        self._named_actors[name] = NamedActorInfo(
            name=name,
            node_id=node_id,
            actor_id=actor_id,
        )

    def _apply_unregister_named_actor(self, name: str) -> None:
        """Apply an unregister named actor command."""
        self._named_actors.pop(name, None)

    # Query methods

    def get_member(self, node_id: str) -> NodeInfo | None:
        """Get info about a specific member.

        Args:
            node_id: The node to look up

        Returns:
            NodeInfo or None if not found
        """
        return self._members.get(node_id)

    def get_all_members(self) -> dict[str, NodeInfo]:
        """Get all cluster members.

        Returns:
            Dictionary of node_id -> NodeInfo
        """
        return dict(self._members)

    def get_active_members(self) -> list[str]:
        """Get IDs of all active members.

        Returns:
            List of active node IDs
        """
        return [
            node_id
            for node_id, info in self._members.items()
            if info.status == "active"
        ]

    def get_failed_members(self) -> list[str]:
        """Get IDs of all failed members.

        Returns:
            List of failed node IDs
        """
        return [
            node_id
            for node_id, info in self._members.items()
            if info.status == "failed"
        ]

    def get_singleton(self, name: str) -> SingletonInfo | None:
        """Get info about a singleton.

        Args:
            name: Singleton name

        Returns:
            SingletonInfo or None if not found
        """
        return self._singletons.get(name)

    def get_all_singletons(self) -> dict[str, SingletonInfo]:
        """Get all registered singletons.

        Returns:
            Dictionary of name -> SingletonInfo
        """
        return dict(self._singletons)

    def get_orphan_singletons(self) -> list[str]:
        """Get names of orphan singletons.

        Orphan singletons are those whose host node has failed.

        Returns:
            List of orphan singleton names
        """
        return [
            name
            for name, info in self._singletons.items()
            if info.status == "orphan"
        ]

    def get_singletons_on_node(self, node_id: str) -> list[str]:
        """Get singletons hosted on a specific node.

        Args:
            node_id: The node to query

        Returns:
            List of singleton names on that node
        """
        return [
            name
            for name, info in self._singletons.items()
            if info.node_id == node_id
        ]

    def get_entity_type(self, entity_type: str) -> EntityTypeInfo | None:
        """Get info about an entity type.

        Args:
            entity_type: Entity type name

        Returns:
            EntityTypeInfo or None if not found
        """
        return self._entity_types.get(entity_type)

    def get_all_entity_types(self) -> dict[str, EntityTypeInfo]:
        """Get all registered entity types.

        Returns:
            Dictionary of entity_type -> EntityTypeInfo
        """
        return dict(self._entity_types)

    def get_named_actor(self, name: str) -> NamedActorInfo | None:
        """Get info about a named actor.

        Args:
            name: Actor name

        Returns:
            NamedActorInfo or None if not found
        """
        return self._named_actors.get(name)

    def get_all_named_actors(self) -> dict[str, NamedActorInfo]:
        """Get all registered named actors.

        Returns:
            Dictionary of name -> NamedActorInfo
        """
        return dict(self._named_actors)

    def get_named_actors_on_node(self, node_id: str) -> list[str]:
        """Get named actors on a specific node.

        Args:
            node_id: The node to query

        Returns:
            List of named actor names on that node
        """
        return [
            name
            for name, info in self._named_actors.items()
            if info.node_id == node_id
        ]

    # Serialization

    def to_dict(self) -> dict:
        """Serialize state to dictionary for snapshotting.

        Returns:
            Dictionary representation of state
        """
        return {
            "members": {
                node_id: {
                    "node_id": info.node_id,
                    "address": list(info.address),
                    "status": info.status,
                    "joined_at": info.joined_at,
                    "failed_at": info.failed_at,
                }
                for node_id, info in self._members.items()
            },
            "singletons": {
                name: {
                    "name": info.name,
                    "node_id": info.node_id,
                    "actor_class": info.actor_class,
                    "version": info.version,
                    "status": info.status,
                }
                for name, info in self._singletons.items()
            },
            "entity_types": {
                et: {
                    "entity_type": info.entity_type,
                    "actor_class": info.actor_class,
                    "replication_factor": info.replication_factor,
                    "registered_at": info.registered_at,
                }
                for et, info in self._entity_types.items()
            },
            "named_actors": {
                name: {
                    "name": info.name,
                    "node_id": info.node_id,
                    "actor_id": info.actor_id,
                    "registered_at": info.registered_at,
                }
                for name, info in self._named_actors.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> ClusterState:
        """Deserialize state from dictionary.

        Args:
            data: Dictionary representation

        Returns:
            ClusterState instance
        """
        state = cls()

        for node_id, info in data.get("members", {}).items():
            state._members[node_id] = NodeInfo(
                node_id=info["node_id"],
                address=tuple(info["address"]),
                status=info["status"],
                joined_at=info["joined_at"],
                failed_at=info.get("failed_at"),
            )

        for name, info in data.get("singletons", {}).items():
            state._singletons[name] = SingletonInfo(
                name=info["name"],
                node_id=info["node_id"],
                actor_class=info["actor_class"],
                version=info["version"],
                status=info["status"],
            )

        for et, info in data.get("entity_types", {}).items():
            state._entity_types[et] = EntityTypeInfo(
                entity_type=info["entity_type"],
                actor_class=info["actor_class"],
                replication_factor=info["replication_factor"],
                registered_at=info.get("registered_at", 0),
            )

        for name, info in data.get("named_actors", {}).items():
            state._named_actors[name] = NamedActorInfo(
                name=info["name"],
                node_id=info["node_id"],
                actor_id=info["actor_id"],
                registered_at=info.get("registered_at", 0),
            )

        return state

    def __repr__(self) -> str:
        return (
            f"ClusterState("
            f"members={len(self._members)}, "
            f"singletons={len(self._singletons)}, "
            f"entity_types={len(self._entity_types)}, "
            f"named_actors={len(self._named_actors)})"
        )
