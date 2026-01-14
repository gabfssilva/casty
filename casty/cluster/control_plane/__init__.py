"""Control plane for Casty cluster.

Uses Raft consensus for:
- Cluster membership
- Singleton registry
- Named actor registry
- Entity type configuration
"""

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
    get_command_type,
)
from .state import (
    ClusterState,
    NodeInfo,
    SingletonInfo,
    EntityTypeInfo,
    NamedActorInfo,
)
from .raft import (
    RaftManager,
    RaftConfig,
    RaftRole,
    LogEntry,
    VoteRequest,
    VoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
)

__all__ = [
    # Commands
    "ControlPlaneCommand",
    "JoinCluster",
    "LeaveCluster",
    "NodeFailed",
    "NodeRecovered",
    "RegisterSingleton",
    "UnregisterSingleton",
    "TransferSingleton",
    "RegisterEntityType",
    "UnregisterEntityType",
    "UpdateEntityTypeConfig",
    "RegisterNamedActor",
    "UnregisterNamedActor",
    "get_command_type",
    # State
    "ClusterState",
    "NodeInfo",
    "SingletonInfo",
    "EntityTypeInfo",
    "NamedActorInfo",
    # Raft
    "RaftManager",
    "RaftConfig",
    "RaftRole",
    "LogEntry",
    "VoteRequest",
    "VoteResponse",
    "AppendEntriesRequest",
    "AppendEntriesResponse",
]
