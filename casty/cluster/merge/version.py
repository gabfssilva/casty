"""Vector Clock for distributed causality tracking.

This module provides a vector clock implementation for tracking
causality and detecting concurrent modifications in a distributed system.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from casty.cluster.serializable import serializable


@serializable
@dataclass(slots=True)
class VectorClock:
    """Vector clock for tracking causality in distributed systems.

    Each node maintains its own counter. The version is a dictionary
    mapping node_id to its logical timestamp.

    Example:
        clock = VectorClock()
        clock = clock.increment("node-a")  # {node-a: 1}
        clock = clock.increment("node-a")  # {node-a: 2}

        other = VectorClock().increment("node-b")  # {node-b: 1}

        # These are concurrent (neither dominates)
        clock.concurrent(other)  # True

        # Merge them
        merged = clock.merge(other)  # {node-a: 2, node-b: 1}
    """

    clock: dict[str, int] = field(default_factory=dict)

    def increment(self, node_id: str) -> VectorClock:
        """Increment counter for a node, returning new VectorClock.

        Args:
            node_id: The node making a modification

        Returns:
            New VectorClock with incremented counter for node_id
        """
        new_clock = dict(self.clock)
        new_clock[node_id] = new_clock.get(node_id, 0) + 1
        return VectorClock(new_clock)

    def merge(self, other: VectorClock) -> VectorClock:
        """Merge two vector clocks by taking max of each counter.

        Args:
            other: The other vector clock to merge with

        Returns:
            New VectorClock with max of each node's counter
        """
        all_nodes = set(self.clock) | set(other.clock)
        return VectorClock({
            node: max(self.clock.get(node, 0), other.clock.get(node, 0))
            for node in all_nodes
        })

    def dominates(self, other: VectorClock) -> bool:
        """Check if self dominates (is descendant of) other.

        Self dominates other if all counters in self are >= other,
        and at least one counter is strictly greater.

        Args:
            other: The vector clock to compare against

        Returns:
            True if self is a descendant of other
        """
        all_nodes = set(self.clock) | set(other.clock)
        all_gte = all(
            self.clock.get(node, 0) >= other.clock.get(node, 0)
            for node in all_nodes
        )
        any_gt = any(
            self.clock.get(node, 0) > other.clock.get(node, 0)
            for node in all_nodes
        )
        return all_gte and any_gt

    def concurrent(self, other: VectorClock) -> bool:
        """Check if self and other are concurrent (neither dominates).

        Concurrent clocks indicate divergent modifications that
        require a merge to reconcile.

        Args:
            other: The vector clock to compare against

        Returns:
            True if neither clock dominates the other
        """
        return not self.dominates(other) and not other.dominates(self) and self != other

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return False
        all_nodes = set(self.clock) | set(other.clock)
        return all(
            self.clock.get(node, 0) == other.clock.get(node, 0)
            for node in all_nodes
        )

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.clock.items())))

    def __repr__(self) -> str:
        items = ", ".join(f"{k}:{v}" for k, v in sorted(self.clock.items()))
        return f"VectorClock({{{items}}})"

    def to_dict(self) -> dict[str, int]:
        """Convert to plain dict for serialization."""
        return dict(self.clock)

    @classmethod
    def from_dict(cls, data: dict[str, int]) -> VectorClock:
        """Create from plain dict after deserialization."""
        return cls(dict(data))
