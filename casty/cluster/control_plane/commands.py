"""Raft commands for cluster control plane.

These commands are replicated via Raft consensus to ensure
all nodes have a consistent view of cluster state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


# Cluster Membership Commands


@dataclass(frozen=True, slots=True)
class JoinCluster:
    """Command to add a node to the cluster.

    Attributes:
        node_id: Unique identifier for the joining node
        address: Network address as (host, port)
        timestamp: When the join was requested
    """

    node_id: str
    address: tuple[str, int]
    timestamp: float


@dataclass(frozen=True, slots=True)
class LeaveCluster:
    """Command for a node to gracefully leave the cluster.

    Attributes:
        node_id: The node that is leaving
        timestamp: When the leave was requested
    """

    node_id: str
    timestamp: float


@dataclass(frozen=True, slots=True)
class NodeFailed:
    """Command to mark a node as failed.

    Issued when gossip detects a node is dead.

    Attributes:
        node_id: The failed node
        detected_by: Node that detected the failure
        timestamp: When the failure was detected
    """

    node_id: str
    detected_by: str
    timestamp: float


@dataclass(frozen=True, slots=True)
class NodeRecovered:
    """Command to mark a previously failed node as recovered.

    Attributes:
        node_id: The recovered node
        address: Updated network address
        timestamp: When recovery was detected
    """

    node_id: str
    address: tuple[str, int]
    timestamp: float


# Singleton Commands


@dataclass(frozen=True, slots=True)
class RegisterSingleton:
    """Command to register a singleton actor.

    Singletons are cluster-wide unique actors. Only one instance
    exists across the entire cluster.

    Attributes:
        name: Unique name for the singleton
        node_id: Node hosting the singleton
        actor_class: Fully qualified class name
        version: Monotonic version for conflict resolution
    """

    name: str
    node_id: str
    actor_class: str
    version: int


@dataclass(frozen=True, slots=True)
class UnregisterSingleton:
    """Command to unregister a singleton actor.

    Attributes:
        name: Name of the singleton to remove
    """

    name: str


@dataclass(frozen=True, slots=True)
class TransferSingleton:
    """Command to transfer a singleton to another node.

    Used during rebalancing or when the current host fails.

    Attributes:
        name: Name of the singleton
        from_node: Current host node
        to_node: Target host node
    """

    name: str
    from_node: str
    to_node: str


# Entity Type Commands


@dataclass(frozen=True, slots=True)
class RegisterEntityType:
    """Command to register a sharded entity type.

    Entity types define how actors are distributed across the cluster.

    Attributes:
        entity_type: Name of the entity type
        actor_class: Fully qualified class name
        replication_factor: Number of replicas (including primary)
    """

    entity_type: str
    actor_class: str
    replication_factor: int


@dataclass(frozen=True, slots=True)
class UnregisterEntityType:
    """Command to unregister an entity type.

    Attributes:
        entity_type: Name of the entity type to remove
    """

    entity_type: str


@dataclass(frozen=True, slots=True)
class UpdateEntityTypeConfig:
    """Command to update entity type configuration.

    Attributes:
        entity_type: Name of the entity type
        replication_factor: New replication factor
    """

    entity_type: str
    replication_factor: int


# Named Actor Commands


@dataclass(frozen=True, slots=True)
class RegisterNamedActor:
    """Command to register a named actor.

    Named actors can be looked up by name from any node.

    Attributes:
        name: Unique name for the actor
        node_id: Node hosting the actor
        actor_id: Local actor ID on that node
    """

    name: str
    node_id: str
    actor_id: str


@dataclass(frozen=True, slots=True)
class UnregisterNamedActor:
    """Command to unregister a named actor.

    Attributes:
        name: Name of the actor to unregister
    """

    name: str


# Type alias for all control plane commands
ControlPlaneCommand = (
    JoinCluster
    | LeaveCluster
    | NodeFailed
    | NodeRecovered
    | RegisterSingleton
    | UnregisterSingleton
    | TransferSingleton
    | RegisterEntityType
    | UnregisterEntityType
    | UpdateEntityTypeConfig
    | RegisterNamedActor
    | UnregisterNamedActor
)


def get_command_type(cmd: ControlPlaneCommand) -> str:
    """Get the type name of a command.

    Args:
        cmd: The command

    Returns:
        String name of the command type
    """
    return type(cmd).__name__
