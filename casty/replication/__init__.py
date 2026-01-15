"""Replication system for Casty distributed actors.

This module provides coordinator-based replication for high availability
of sharded, singleton, and named actors across a distributed cluster.

Features:
- WAL-based replication with automatic read detection
- Tunable consistency levels (Eventual, Quorum, Strong, AtLeast)
- Coordinator-based parallel writes to replicas
- Hinted handoff for offline replicas
- Automatic failover via deterministic hash ring rebalancing
- Type-safe message-based coordination
"""

from .coordinator import ReplicationCoordinator
from .metadata import ReplicationMetadata, WriteCoordination, WriteHint
from .wal_entry import ReplicationWALEntry, WALGapError
from .replicated_actor import ReplicatedActorWrapper, ReplicatedActorState

__all__ = [
    "ReplicationCoordinator",
    "ReplicationMetadata",
    "WriteCoordination",
    "WriteHint",
    # WAL-based replication
    "ReplicationWALEntry",
    "WALGapError",
    "ReplicatedActorWrapper",
    "ReplicatedActorState",
]
