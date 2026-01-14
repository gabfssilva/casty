"""Casty distributed cluster support.

This module provides distributed actor capabilities:
- DistributedActorSystem for cluster deployment
- Control plane with Raft consensus
- Data plane with consistent hashing and gossip
"""

from .distributed import DistributedActorSystem
from .control_plane import (
    RaftManager,
    RaftConfig,
    ClusterState,
)
from .data_plane import (
    HashRing,
    HashRingConfig,
    SWIMProtocol,
    GossipConfig,
    DataPlaneRouter,
)
from .transport import Transport, MessageType
from .serialize import serialize, deserialize, register_type

__all__ = [
    # Distributed system
    "DistributedActorSystem",
    # Control plane
    "RaftManager",
    "RaftConfig",
    "ClusterState",
    # Data plane
    "HashRing",
    "HashRingConfig",
    "SWIMProtocol",
    "GossipConfig",
    "DataPlaneRouter",
    # Transport
    "Transport",
    "MessageType",
    # Serialize
    "serialize",
    "deserialize",
    "register_type",
]
