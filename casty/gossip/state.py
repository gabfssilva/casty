"""
Gossip state data structures.
"""

from dataclasses import dataclass
from enum import Enum, auto

from .versioning import VectorClock


class EntryStatus(Enum):
    """Status of a state entry for tombstone-based deletion."""

    ACTIVE = auto()
    DELETED = auto()


@dataclass(frozen=True, slots=True)
class StateEntry:
    """Single entry in the gossip state.

    Keys are hierarchical strings: "cluster/nodes/node-1" or "app/config/timeout".

    Attributes:
        key: Hierarchical key string.
        value: Serialized application data.
        version: Vector clock for this entry.
        status: Whether entry is active or deleted (tombstone).
        ttl: Optional time-to-live in seconds.
        origin: Node that created/updated this entry.
        timestamp: Wall clock time of last update.
    """

    key: str
    value: bytes
    version: VectorClock
    status: EntryStatus = EntryStatus.ACTIVE
    ttl: float | None = None
    origin: str = ""
    timestamp: float = 0.0

    def is_deleted(self) -> bool:
        """Check if this entry is a tombstone."""
        return self.status == EntryStatus.DELETED

    def with_status(self, status: EntryStatus) -> "StateEntry":
        """Return copy with new status."""
        return StateEntry(
            key=self.key,
            value=self.value,
            version=self.version,
            status=status,
            ttl=self.ttl,
            origin=self.origin,
            timestamp=self.timestamp,
        )


@dataclass(frozen=True, slots=True)
class StateDigest:
    """Compact representation of state for synchronization.

    Contains (key, version) pairs without values.
    Used in pull phase to identify what data to request.

    Attributes:
        entries: Tuple of (key, VectorClock) pairs.
        node_id: Node that created this digest.
        generation: Increments on each digest creation.
    """

    entries: tuple[tuple[str, VectorClock], ...]
    node_id: str
    generation: int = 0

    def keys(self) -> set[str]:
        """Get set of all keys in digest."""
        return {key for key, _ in self.entries}

    def get_version(self, key: str) -> VectorClock | None:
        """Get version for a specific key."""
        for k, v in self.entries:
            if k == key:
                return v
        return None


@dataclass(frozen=True, slots=True)
class DigestDiff:
    """Difference between two digests.

    Lists keys that need synchronization in each direction.

    Attributes:
        keys_i_need: Keys the remote has that I need.
        keys_peer_needs: Keys I have that the remote needs.
    """

    keys_i_need: tuple[str, ...]
    keys_peer_needs: tuple[str, ...]

    def is_empty(self) -> bool:
        """Check if no synchronization is needed."""
        return not self.keys_i_need and not self.keys_peer_needs
