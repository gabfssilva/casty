"""Raft protocol data types and RPC messages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class LogEntry:
    """A single log entry in the Raft log."""

    term: int
    index: int
    command: Any


@dataclass(frozen=True, slots=True)
class AppendEntriesRequest:
    """AppendEntries RPC request from leader to followers."""

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse:
    """AppendEntries RPC response from follower to leader."""

    term: int
    success: bool
    follower_id: str
    match_index: int  # Highest index replicated (for leader tracking)


@dataclass(frozen=True, slots=True)
class InstallSnapshotRequest:
    """InstallSnapshot RPC request from leader to follower."""

    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    data: bytes  # Serialized state machine snapshot
    done: bool  # True if this is the final chunk


@dataclass(frozen=True, slots=True)
class InstallSnapshotResponse:
    """InstallSnapshot RPC response from follower to leader."""

    term: int
    success: bool
    follower_id: str


@dataclass(frozen=True, slots=True)
class RequestVoteRequest:
    """RequestVote RPC request from candidate to voters."""

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True, slots=True)
class RequestVoteResponse:
    """RequestVote RPC response from voter to candidate."""

    term: int
    vote_granted: bool
    voter_id: str


@dataclass
class RaftLog:
    """The Raft replicated log with commit tracking."""

    entries: list[LogEntry] = field(default_factory=list)
    commit_index: int = 0  # Index of highest committed entry
    last_applied: int = 0  # Index of highest applied entry

    @property
    def last_log_index(self) -> int:
        """Index of the last log entry (0 if empty)."""
        return self.entries[-1].index if self.entries else 0

    @property
    def last_log_term(self) -> int:
        """Term of the last log entry (0 if empty)."""
        return self.entries[-1].term if self.entries else 0

    def get_entry(self, index: int) -> LogEntry | None:
        """Get entry by index, returning None if not found."""
        if index <= 0 or not self.entries:
            return None
        # Convert from 1-based index to list position
        first_index = self.entries[0].index
        pos = index - first_index
        if 0 <= pos < len(self.entries):
            return self.entries[pos]
        return None

    def get_term_at(self, index: int) -> int:
        """Get term at given index, 0 if not found."""
        entry = self.get_entry(index)
        return entry.term if entry else 0

    def append(self, entry: LogEntry) -> None:
        """Append a single entry to the log."""
        self.entries.append(entry)

    def truncate_from(self, index: int) -> None:
        """Remove all entries starting from the given index."""
        if not self.entries or index <= 0:
            return
        first_index = self.entries[0].index
        pos = index - first_index
        if 0 <= pos < len(self.entries):
            self.entries = self.entries[:pos]

    def get_entries_from(self, start_index: int) -> list[LogEntry]:
        """Get all entries starting from the given index."""
        if not self.entries or start_index <= 0:
            return []
        first_index = self.entries[0].index
        pos = start_index - first_index
        if pos < 0:
            return list(self.entries)
        if pos >= len(self.entries):
            return []
        return list(self.entries[pos:])

    def install_snapshot(self, last_included_index: int, last_included_term: int) -> None:
        """Discard log entries covered by snapshot."""
        # Keep only entries after the snapshot
        first_index = self.entries[0].index if self.entries else 0
        pos = last_included_index - first_index + 1
        if pos > 0:
            self.entries = self.entries[pos:] if pos < len(self.entries) else []
        # Update commit/applied if needed
        self.commit_index = max(self.commit_index, last_included_index)
        self.last_applied = max(self.last_applied, last_included_index)


@dataclass
class Snapshot:
    """A snapshot of the state machine for log compaction."""

    last_included_index: int
    last_included_term: int
    data: bytes  # Serialized state machine state


# Singleton types


@dataclass(frozen=True, slots=True)
class SingletonCommand:
    """Command to manage singleton actors in the cluster (replicated via Raft)."""

    action: Literal["register", "unregister", "orphan"]
    name: str
    node_id: str
    actor_cls_name: str  # Fully qualified class name for auto-recreate
    kwargs: tuple[tuple[str, Any], ...]  # Constructor kwargs as tuple of tuples (for hashability)
    timestamp: float


@dataclass
class SingletonEntry:
    """State of a singleton actor in the cluster."""

    name: str
    node_id: str
    actor_cls_name: str
    kwargs: dict[str, Any]
    status: Literal["active", "orphan"]


# Sharding types


@dataclass(frozen=True, slots=True)
class ShardedMessage:
    """Message wrapper for sharded entity communication via Raft.

    Encapsulates a message that needs to be routed to a specific entity
    within a sharded actor type. This message is appended to the Raft log
    to ensure consistent ordering across the cluster.
    """

    entity_type: str  # The sharded actor type name (e.g., "account")
    entity_id: str  # The specific entity ID (e.g., "user-1")
    payload: Any  # The actual message to deliver
    reply_to: str | None = None  # Node ID for ask responses (bypasses Raft)
    request_id: str | None = None  # For correlating ask responses


@dataclass(frozen=True, slots=True)
class ShardAllocationCommand:
    """Command to allocate a shard to a node via Raft consensus.

    Used to maintain consistent shard-to-node mappings across the cluster.
    """

    action: Literal["allocate", "deallocate"]
    entity_type: str
    shard_id: int
    node_id: str


@dataclass
class ShardedEntityConfig:
    """Configuration for a sharded entity type."""

    entity_type: str
    actor_cls_name: str
    num_shards: int
    kwargs: dict[str, Any]  # Default constructor kwargs


@dataclass(frozen=True, slots=True)
class RegisterShardedEntityCommand:
    """Command to register a sharded entity type across the cluster via Raft."""

    entity_type: str
    actor_cls_name: str
    num_shards: int
    kwargs: tuple[tuple[str, Any], ...]  # Constructor kwargs as tuple for hashability


# State replication types


@dataclass(frozen=True, slots=True)
class EntityStateUpdate:
    """Command to replicate entity state across the cluster via Raft.

    Flow:
    1. Primary processes message, updates state
    2. Primary creates EntityStateUpdate with new state
    3. EntityStateUpdate goes to Raft log
    4. All nodes apply (backups store in _backup_states)

    Versioning:
    - version is monotonic per (entity_type, entity_id)
    - Nodes only apply if version > local version
    - Prevents out-of-order application after partitions
    """

    entity_type: str
    entity_id: str
    state: bytes  # Serialized state via msgpack
    version: int  # Monotonic version for ordering
    primary_node: str  # Node that originated the update
    timestamp: float  # Timestamp from primary


@dataclass(frozen=True, slots=True)
class EntityStateDelete:
    """Command to remove entity state via Raft.

    Used when entity is explicitly removed or
    during garbage collection of inactive entities.
    """

    entity_type: str
    entity_id: str
    version: int
    reason: Literal["explicit", "gc", "migration"]
