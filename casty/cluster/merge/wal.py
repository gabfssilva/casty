"""Write-Ahead Log for actor state tracking.

This module provides a WAL implementation that stores deltas (changes)
instead of full state snapshots, enabling efficient three-way merge
by finding common ancestors.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from time import time
from typing import Any

from .version import VectorClock


@dataclass(slots=True)
class WALEntry:
    """Single entry in the Write-Ahead Log.

    Stores only the delta (what changed) along with the version
    at which the change occurred.
    """

    version: VectorClock
    delta: dict[str, Any]
    timestamp: float = field(default_factory=time)


@dataclass
class WAL:
    """Write-Ahead Log for tracking actor state changes.

    Stores deltas instead of full state snapshots for efficiency.
    Supports finding common ancestors for three-way merge.

    Example:
        wal = WAL(actor_id="counter-1", node_id="node-a")

        # Record a mutation
        new_version = wal.append({"count": 10})

        # Find common ancestor with another node's version
        base = wal.find_base(their_version)
        if base:
            base_version, base_state = base
            # Do three-way merge
    """

    actor_id: str
    node_id: str
    entries: list[WALEntry] = field(default_factory=list)
    base_snapshot: dict[str, Any] = field(default_factory=dict)
    base_version: VectorClock = field(default_factory=VectorClock)
    max_entries: int = 1000

    @property
    def current_version(self) -> VectorClock:
        """Get the current version (latest entry or base)."""
        if self.entries:
            return self.entries[-1].version
        return self.base_version

    def append(self, delta: dict[str, Any]) -> VectorClock:
        """Append a mutation to the WAL.

        Args:
            delta: Dictionary of changed fields {field: new_value}

        Returns:
            The new version after this mutation
        """
        new_version = self.current_version.increment(self.node_id)
        self.entries.append(WALEntry(version=new_version, delta=delta))

        if len(self.entries) > self.max_entries:
            self._compact()

        return new_version

    def get_state_at(self, target_version: VectorClock) -> dict[str, Any] | None:
        """Reconstruct state at a specific version.

        Args:
            target_version: The version to reconstruct

        Returns:
            The state at that version, or None if not found
        """
        # Start from base snapshot
        state = dict(self.base_snapshot)

        # Check if target is the base version
        if target_version == self.base_version:
            return state

        # Apply deltas up to target version
        for entry in self.entries:
            if target_version.dominates(entry.version) or target_version == entry.version:
                state.update(entry.delta)
            if entry.version == target_version:
                return state

        return None

    def get_current_state(self) -> dict[str, Any]:
        """Get the current state by applying all deltas."""
        state = dict(self.base_snapshot)
        for entry in self.entries:
            state.update(entry.delta)
        return state

    def find_base(self, their_version: VectorClock) -> tuple[VectorClock, dict[str, Any]] | None:
        """Find common ancestor for three-way merge.

        Walks backwards through the WAL to find the latest version
        that is an ancestor of both the current version and their version.

        Args:
            their_version: The other node's version

        Returns:
            Tuple of (base_version, base_state) or None if no common ancestor
        """
        my_version = self.current_version

        # Check entries in reverse order
        for entry in reversed(self.entries):
            # Base must be ancestor of both versions
            if my_version.dominates(entry.version) and their_version.dominates(entry.version):
                state = self.get_state_at(entry.version)
                if state is not None:
                    return (entry.version, state)

        # Check if base_snapshot is common ancestor
        if my_version.dominates(self.base_version) and their_version.dominates(self.base_version):
            return (self.base_version, dict(self.base_snapshot))

        # Also check if base versions are equal (initial state)
        if self.base_version == VectorClock() and their_version.dominates(self.base_version):
            return (self.base_version, dict(self.base_snapshot))

        return None

    def sync_to(self, version: VectorClock, state: dict[str, Any]) -> None:
        """Sync to external state (full sync).

        Used when no common ancestor is found and we need to
        accept the other node's state entirely.

        Args:
            version: The version to sync to
            state: The full state at that version
        """
        self.base_snapshot = dict(state)
        self.base_version = version
        self.entries = []

    def append_merged(self, version: VectorClock, state: dict[str, Any]) -> None:
        """Append result of a merge operation.

        After a three-way merge, record the merged state.

        Args:
            version: The merged version (should be merge of both clocks + increment)
            state: The full merged state
        """
        # Calculate delta from current state
        current = self.get_current_state()
        delta = {k: v for k, v in state.items() if current.get(k) != v}

        # Also include removed keys
        for k in current:
            if k not in state:
                delta[k] = None  # Mark as deleted

        self.entries.append(WALEntry(version=version, delta=delta))

        if len(self.entries) > self.max_entries:
            self._compact()

    def _compact(self) -> None:
        """Compact WAL by creating new base snapshot.

        Keeps the last half of entries and creates a new base
        from the state at the compaction point.
        """
        keep = self.max_entries // 2

        if len(self.entries) <= keep:
            return

        cut_point = len(self.entries) - keep
        cut_entry = self.entries[cut_point - 1]

        new_base_state = self.get_state_at(cut_entry.version)
        if new_base_state:
            self.base_snapshot = new_base_state
            self.base_version = cut_entry.version
            self.entries = self.entries[cut_point:]
