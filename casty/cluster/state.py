"""Cluster state container - shared state across mixins."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from casty.cluster.config import ClusterConfig
from casty.cluster.hash_ring import HashRing

if TYPE_CHECKING:
    from casty import LocalRef, Actor
    from casty.cluster.messages import ClusterEvent
    from casty.replication import ReplicationMetadata
    from casty.merge import MergeableActor


@dataclass(frozen=True, slots=True)
class _RegistrationTimeout:
    """Internal: Timeout for sharded type registration."""
    request_id: str


@dataclass
class PendingAsk:
    """Tracks an in-flight ask request."""

    request_id: str
    target_actor: str
    target_node: str
    future: asyncio.Future[Any]


@dataclass
class PendingRegistration:
    """Tracks a sharded type registration waiting for acks."""

    entity_type: str
    required_acks: int
    received_acks: set[str]  # node_ids that have acked
    reply_future: asyncio.Future[Any] | None  # The reply_to from the ask envelope


@dataclass
class ClusterState:
    """Mutable state for the Cluster actor, shared across mixins."""

    node_id: str
    config: ClusterConfig

    # =========================================================================
    # Child Actors
    # =========================================================================

    mux: LocalRef[Any] | None = None
    swim: LocalRef[Any] | None = None
    gossip: LocalRef[Any] | None = None
    scheduler: LocalRef[Any] | None = None
    replication_coordinator: LocalRef[Any] | None = None

    # =========================================================================
    # Actor Registry
    # =========================================================================

    local_actors: dict[str, LocalRef[Any]] = field(default_factory=dict)
    actor_locations: dict[str, str] = field(default_factory=dict)  # actor_name -> node_id

    # =========================================================================
    # Sharding State
    # =========================================================================

    hash_ring: HashRing = field(default_factory=HashRing)
    sharded_types: dict[str, type[Actor[Any]]] = field(default_factory=dict)
    local_entities: dict[str, dict[str, LocalRef[Any]]] = field(default_factory=dict)
    entity_wrappers: dict[str, dict[str, Any]] = field(default_factory=dict)  # For merge tracking

    # =========================================================================
    # Singleton State
    # =========================================================================

    singleton_types: dict[str, type[Actor[Any]]] = field(default_factory=dict)  # name -> actor class
    local_singletons: dict[str, LocalRef[Any]] = field(default_factory=dict)  # name -> ref (if owned)
    singleton_locations: dict[str, str] = field(default_factory=dict)  # name -> node_id (from gossip)

    # =========================================================================
    # Replication State
    # =========================================================================

    replicated_types: dict[str, Any] = field(default_factory=dict)  # type_name -> ReplicationMetadata
    # WAL-based replication wrappers: entity_type -> entity_id -> ReplicatedActorWrapper
    replicated_wrappers: dict[str, dict[str, Any]] = field(default_factory=dict)

    # =========================================================================
    # Request Tracking
    # =========================================================================

    pending_asks: dict[str, PendingAsk] = field(default_factory=dict)
    request_counter: int = 0

    # Sharded type registration tracking (for consistency levels)
    pending_registrations: dict[str, PendingRegistration] = field(default_factory=dict)

    # =========================================================================
    # Event Subscribers
    # =========================================================================

    subscribers: set[LocalRef[Any]] = field(default_factory=set)

    # =========================================================================
    # Server State
    # =========================================================================

    bound_address: tuple[str, int] | None = None

    @classmethod
    def create(cls, config: ClusterConfig | None = None) -> ClusterState:
        """Factory method to create ClusterState with auto-generated node_id if needed."""
        cfg = config or ClusterConfig()
        node_id = cfg.node_id or f"node-{uuid4().hex[:8]}"
        state = cls(node_id=node_id, config=cfg)
        state.hash_ring.add_node(node_id)
        return state
