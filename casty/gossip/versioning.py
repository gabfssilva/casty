"""
Vector Clock implementation for causal ordering.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class VectorClock:
    """Logical clock for causal ordering of events.

    Maps node_id -> logical timestamp.
    Immutable to ensure safe sharing across actors.

    Example:
        clock = VectorClock()
        clock = clock.increment("node-1")  # {"node-1": 1}
        clock = clock.increment("node-1")  # {"node-1": 2}

        other = VectorClock().increment("node-2")
        merged = clock.merge(other)  # {"node-1": 2, "node-2": 1}
    """

    entries: tuple[tuple[str, int], ...] = ()

    def get(self, node_id: str) -> int:
        """Get the timestamp for a node (0 if not present)."""
        for nid, ts in self.entries:
            if nid == node_id:
                return ts
        return 0

    def increment(self, node_id: str) -> "VectorClock":
        """Return new clock with node_id's entry incremented."""
        new_entries: list[tuple[str, int]] = []
        found = False

        for nid, ts in self.entries:
            if nid == node_id:
                new_entries.append((nid, ts + 1))
                found = True
            else:
                new_entries.append((nid, ts))

        if not found:
            new_entries.append((node_id, 1))

        # Sort for deterministic ordering
        new_entries.sort(key=lambda x: x[0])
        return VectorClock(entries=tuple(new_entries))

    def merge(self, other: "VectorClock") -> "VectorClock":
        """Return new clock with maximum entries from both."""
        merged: dict[str, int] = {}

        for nid, ts in self.entries:
            merged[nid] = ts

        for nid, ts in other.entries:
            merged[nid] = max(merged.get(nid, 0), ts)

        entries = tuple(sorted(merged.items(), key=lambda x: x[0]))
        return VectorClock(entries=entries)

    def happens_before(self, other: "VectorClock") -> bool:
        """True if self causally precedes other (self < other).

        self < other iff:
        - For all nodes n: self[n] <= other[n]
        - Exists node n: self[n] < other[n]
        """
        all_nodes = {nid for nid, _ in self.entries} | {nid for nid, _ in other.entries}

        at_least_one_less = False
        for node_id in all_nodes:
            self_ts = self.get(node_id)
            other_ts = other.get(node_id)
            if self_ts > other_ts:
                return False
            if self_ts < other_ts:
                at_least_one_less = True

        return at_least_one_less

    def concurrent_with(self, other: "VectorClock") -> bool:
        """True if neither clock causally precedes the other.

        Concurrent iff not (self < other) and not (other < self).
        """
        return not self.happens_before(other) and not other.happens_before(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        return self.entries == other.entries

    def __hash__(self) -> int:
        return hash(self.entries)

    def to_dict(self) -> dict[str, int]:
        """Convert to dictionary for serialization."""
        return dict(self.entries)

    @classmethod
    def from_dict(cls, data: dict[str, int]) -> "VectorClock":
        """Create from dictionary."""
        entries = tuple(sorted(data.items(), key=lambda x: x[0]))
        return cls(entries=entries)


@dataclass(frozen=True, slots=True)
class VersionedValue:
    """A value tagged with version metadata.

    Enables conflict detection and last-writer-wins resolution.

    Attributes:
        value: The actual value (any serializable type).
        clock: Vector clock at time of write.
        origin_node: Node that created this value.
        timestamp: Wall clock time for tie-breaking.
    """

    value: Any
    clock: VectorClock
    origin_node: str
    timestamp: float
