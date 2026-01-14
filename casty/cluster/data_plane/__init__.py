"""Data plane for Casty cluster.

Provides:
- Consistent hashing for entity distribution
- SWIM gossip for failure detection
- Direct TCP routing for messages
"""

from .hash_ring import HashRing, HashRingConfig, VirtualNode
from .gossip import (
    SWIMProtocol,
    GossipConfig,
    MemberInfo,
    MemberStatus,
    MembershipUpdate,
    Ping,
    PingReq,
    Ack,
)
from .router import DataPlaneRouter, EntityPlacement, RoutingResult

__all__ = [
    # Hash ring
    "HashRing",
    "HashRingConfig",
    "VirtualNode",
    # Gossip
    "SWIMProtocol",
    "GossipConfig",
    "MemberInfo",
    "MemberStatus",
    "MembershipUpdate",
    "Ping",
    "PingReq",
    "Ack",
    # Router
    "DataPlaneRouter",
    "EntityPlacement",
    "RoutingResult",
]
