"""
Casty Cluster - 100% Actor-based cluster coordination.

This module provides a Cluster actor that manages:
- TCP server for single-port communication
- SWIM membership protocol for failure detection
- Gossip-based actor registry for distributed naming
- Remote actor messaging (send/ask)

Example:
    from casty import ActorSystem
    from casty.cluster import Cluster, ClusterConfig, LocalRegister, GetRemoteRef

    async with ActorSystem() as system:
        # Create cluster node
        cluster = await system.spawn(
            Cluster,
            config=ClusterConfig.development().with_seeds(["192.168.1.10:7946"]),
        )

        # Register a local actor
        worker = await system.spawn(Worker, name="worker-1")
        await cluster.send(LocalRegister("worker-1", worker))

        # Get reference to remote actor
        remote_ref = await cluster.ask(GetRemoteRef("worker-2", "node-2"))
        await remote_ref.send(DoWork())
        result = await remote_ref.ask(GetStatus())
"""

# Configuration
from .config import ClusterConfig

# Main actor
from .cluster import Cluster

# Clustered ActorSystem
from .clustered_system import ClusteredActorSystem

# Remote reference
from .remote_ref import RemoteRef, RemoteActorError

# Messages
from .messages import (
    # Internal API
    RemoteSend,
    RemoteAsk,
    LocalRegister,
    LocalUnregister,
    GetRemoteRef,
    GetMembers,
    GetLocalActors,
    Subscribe,
    Unsubscribe,
    # Events
    NodeJoined,
    NodeLeft,
    NodeFailed,
    ActorRegistered,
    ActorUnregistered,
    ClusterEvent,
)

# Consistent hashing
from .hash_ring import HashRing

# Consistency levels
from .consistency import (
    ShardConsistency,
    Strong,
    Eventual,
    Quorum,
    AtLeast,
)

# Replication configuration
from .replication_config import Replication

# Development utilities
from .development import DevelopmentCluster

__all__ = [
    # Configuration
    "ClusterConfig",
    # Main actor
    "Cluster",
    # Clustered ActorSystem
    "ClusteredActorSystem",
    # Remote reference
    "RemoteRef",
    "RemoteActorError",
    # API Messages
    "RemoteSend",
    "RemoteAsk",
    "LocalRegister",
    "LocalUnregister",
    "GetRemoteRef",
    "GetMembers",
    "GetLocalActors",
    "Subscribe",
    "Unsubscribe",
    # Events
    "NodeJoined",
    "NodeLeft",
    "NodeFailed",
    "ActorRegistered",
    "ActorUnregistered",
    "ClusterEvent",
    # Consistent hashing
    "HashRing",
    # Consistency levels
    "ShardConsistency",
    "Strong",
    "Eventual",
    "Quorum",
    "AtLeast",
    # Replication configuration
    "Replication",
    # Development utilities
    "DevelopmentCluster",
]
