"""Version tracking for mergeable actors.

This module provides version tracking using Lamport timestamps
for detecting concurrent modifications that require merge.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ActorVersion:
    """Version tracking for mergeable actors.

    Uses a simple Lamport timestamp with node_id to detect
    concurrent modifications that require merging.

    Attributes:
        version: Lamport timestamp (increments on each mutation)
        node_id: Node that made the last modification
    """

    version: int
    node_id: str

    def happens_before(self, other: ActorVersion) -> bool:
        """Check if this version causally precedes another.

        Returns True if this version definitely happened before other.
        """
        return self.version < other.version

    def concurrent_with(self, other: ActorVersion) -> bool:
        """Check if this version is concurrent with another.

        Concurrent versions indicate divergent modifications that
        require a merge to reconcile.

        Returns True if both versions have the same timestamp
        but different node_ids (concurrent modification).
        """
        return self.version == other.version and self.node_id != other.node_id

    def needs_merge(self, other: ActorVersion) -> bool:
        """Check if merging with another version is needed.

        Merge is needed when:
        1. Versions are concurrent (same timestamp, different nodes)
        2. Neither version happens-before the other but they differ

        Returns True if a merge operation is required.
        """
        if self.version == other.version and self.node_id == other.node_id:
            return False  # Identical versions, no merge needed
        if self.happens_before(other) or other.happens_before(self):
            return False  # Clear ordering, no merge needed
        return True  # Concurrent or incomparable, merge required

    def merge_version(self, other: ActorVersion, my_node_id: str) -> ActorVersion:
        """Create a new version after merging with another.

        The merged version has a timestamp greater than both inputs,
        attributed to the node performing the merge.

        Args:
            other: The version being merged with
            my_node_id: The node performing the merge

        Returns:
            New ActorVersion representing the merged state
        """
        return ActorVersion(
            version=max(self.version, other.version) + 1,
            node_id=my_node_id,
        )

    def increment(self, node_id: str | None = None) -> ActorVersion:
        """Create an incremented version.

        Args:
            node_id: Override node_id, or keep current if None

        Returns:
            New ActorVersion with incremented timestamp
        """
        return ActorVersion(
            version=self.version + 1,
            node_id=node_id or self.node_id,
        )

    def __repr__(self) -> str:
        return f"ActorVersion({self.version}, {self.node_id!r})"
