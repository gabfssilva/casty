"""Replication system for Casty distributed actors.

This module provides coordinator-based replication for high availability
of sharded, singleton, and named actors across a distributed cluster.

Features:
- Tunable consistency levels (Eventual, Quorum, Strong, AtLeast)
- Coordinator-based parallel writes to replicas
- Hinted handoff for offline replicas
- Automatic failover via deterministic hash ring rebalancing
- Type-safe message-based coordination
"""

from .coordinator import ReplicationCoordinator
from .metadata import ReplicationMetadata, WriteCoordination, WriteHint

__all__ = [
    "ReplicationCoordinator",
    "ReplicationMetadata",
    "WriteCoordination",
    "WriteHint",
]
