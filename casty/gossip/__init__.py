"""
Gossip Protocol - Eventually consistent state dissemination.

This module provides a pure actor-based gossip protocol for propagating
state across a cluster of nodes using the existing TCP actor infrastructure.

Features:
- Eventually consistent state replication
- Hybrid push-pull dissemination (epidemic + anti-entropy)
- Vector clocks for causal ordering
- Last-Writer-Wins conflict resolution
- Pattern-based subscriptions

Example:
    from casty import ActorSystem
    from casty.gossip import GossipManager, Publish, Subscribe, GetState

    async with ActorSystem() as system:
        gossip = await system.spawn(
            GossipManager,
            node_id="node-1",
            bind_address=("0.0.0.0", 9000),
            seeds=[("192.168.1.10", 9000)],
        )

        # Publish state
        await gossip.send(Publish("cluster/leader", b"node-1"))

        # Query state
        value = await gossip.ask(GetState("cluster/leader"))

        # Subscribe to changes
        await gossip.send(Subscribe("cluster/*", handler_ref))
"""

# Configuration
from .config import GossipConfig, GossipConfigPresets

# Core data structures
from .versioning import VectorClock, VersionedValue
from .state import StateEntry, StateDigest, DigestDiff, EntryStatus

# Messages (Commands and Events)
from .messages import (
    # Commands
    Publish,
    Delete,
    Subscribe,
    Unsubscribe,
    GetState,
    GetPeers,
    # Events
    StateChanged,
    PeerJoined,
    PeerLeft,
    GossipStarted,
    GossipStopped,
    # Internal transport messages
    IncomingGossipData,
    OutgoingGossipData,
    # Type aliases
    GossipCommand,
    GossipEvent,
)

# Main actor
from .manager import GossipManager

# Pattern matching
from .patterns import matches, PatternMatcher

__all__ = [
    # Configuration
    "GossipConfig",
    "GossipConfigPresets",
    # Data structures
    "VectorClock",
    "VersionedValue",
    "StateEntry",
    "StateDigest",
    "DigestDiff",
    "EntryStatus",
    # Commands
    "Publish",
    "Delete",
    "Subscribe",
    "Unsubscribe",
    "GetState",
    "GetPeers",
    # Events
    "StateChanged",
    "PeerJoined",
    "PeerLeft",
    "GossipStarted",
    "GossipStopped",
    # Internal transport messages
    "IncomingGossipData",
    "OutgoingGossipData",
    # Type aliases
    "GossipCommand",
    "GossipEvent",
    # Main actor
    "GossipManager",
    # Pattern matching
    "matches",
    "PatternMatcher",
]
