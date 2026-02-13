"""Topology subscription protocol.

Provides ``TopologySnapshot``, ``SubscribeTopology``, and
``UnsubscribeTopology`` — the public protocol for subscribing to
cluster topology changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from casty.cluster_state import Member, NodeAddress, ServiceEntry

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from casty.replication import ShardAllocation


@dataclass(frozen=True)
class TopologySnapshot:
    """Full cluster topology pushed to subscribers.

    Parameters
    ----------
    members
        Current cluster members with their statuses.
    leader
        Address of the current cluster leader, or ``None``.
    shard_allocations
        Per-shard-type mapping of shard_id to allocation (primary + replicas).
    allocation_epoch
        Monotonically increasing epoch for allocation consistency.
    unreachable
        Nodes currently marked unreachable by the failure detector.
    registry
        Service registry entries replicated across the cluster.
    """

    members: frozenset[Member]
    leader: NodeAddress | None
    shard_allocations: dict[str, dict[int, ShardAllocation]]
    allocation_epoch: int
    unreachable: frozenset[NodeAddress] = field(
        default_factory=lambda: frozenset[NodeAddress]()
    )
    registry: frozenset[ServiceEntry] = field(
        default_factory=lambda: frozenset[ServiceEntry]()
    )


@dataclass(frozen=True)
class SubscribeTopology:
    """Request to receive topology updates.

    Parameters
    ----------
    reply_to
        Ref that will receive ``TopologySnapshot`` messages.
    """

    reply_to: ActorRef[TopologySnapshot]


@dataclass(frozen=True)
class UnsubscribeTopology:
    """Request to stop receiving topology updates.

    Parameters
    ----------
    subscriber
        The ref to remove from the subscriber list.
    """

    subscriber: ActorRef[TopologySnapshot]
