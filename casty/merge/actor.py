"""MergeableActor wrapper for three-way merge support.

This module provides a wrapper that adds version tracking and
snapshot management to any actor that implements the Mergeable protocol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .protocol import Mergeable
from .version import ActorVersion

if TYPE_CHECKING:
    from casty.actor import Actor


@dataclass(slots=True)
class MergeableState:
    """State managed by MergeableActor wrapper.

    Tracks versioning and snapshots for merge operations.
    """

    version: int = 0
    base_version: int = 0
    base_snapshot: dict[str, Any] | None = None
    node_id: str = ""


@dataclass
class MergeableActor[M]:
    """Wrapper that adds merge capabilities to an actor.

    Manages version tracking and base snapshots for three-way merge.
    The wrapped actor must implement the Mergeable protocol.

    Usage:
        # Wrap an actor instance
        account = Account()
        mergeable = MergeableActor(account)

        # Or use as part of cluster spawning
        # The cluster automatically wraps Mergeable actors

    The wrapper tracks:
    - version: Lamport timestamp incremented on each mutation
    - base_snapshot: State at last sync point (for three-way merge)
    - node_id: Node that owns this actor instance
    """

    actor: "Actor[M]"
    _state: MergeableState = field(default_factory=MergeableState)

    def __post_init__(self) -> None:
        """Validate that wrapped actor implements Mergeable."""
        if not isinstance(self.actor, Mergeable):
            raise TypeError(
                f"Actor {type(self.actor).__name__} must implement "
                f"__casty_merge__ to be wrapped by MergeableActor"
            )

    @property
    def version(self) -> int:
        """Current version (Lamport timestamp)."""
        return self._state.version

    @property
    def base_version(self) -> int:
        """Version at last snapshot."""
        return self._state.base_version

    @property
    def base_snapshot(self) -> dict[str, Any] | None:
        """State at last sync point."""
        return self._state.base_snapshot

    @property
    def node_id(self) -> str:
        """Node that owns this actor."""
        return self._state.node_id

    def set_node_id(self, node_id: str) -> None:
        """Set the owning node ID."""
        self._state.node_id = node_id

    def increment_version(self) -> None:
        """Increment version after state mutation."""
        self._state.version += 1

    def take_snapshot(self) -> None:
        """Take snapshot of current state for future merges."""
        self._state.base_snapshot = self.actor.get_state()
        self._state.base_version = self._state.version

    def get_actor_version(self) -> ActorVersion:
        """Get current version as ActorVersion."""
        return ActorVersion(
            version=self._state.version,
            node_id=self._state.node_id,
        )

    def get_merge_info(self) -> tuple[int, int, dict[str, Any], dict[str, Any] | None]:
        """Get all merge-related info at once.

        Returns:
            Tuple of (version, base_version, current_state, base_snapshot)
        """
        return (
            self._state.version,
            self._state.base_version,
            self.actor.get_state(),
            self._state.base_snapshot,
        )

    def execute_merge(
        self,
        base_state: dict[str, Any] | None,
        other_state: dict[str, Any],
        other_version: int,
    ) -> None:
        """Execute three-way merge operation.

        Args:
            base_state: State at common ancestor (None if unavailable)
            other_state: State from the other node
            other_version: Version from the other node
        """
        actor_cls = type(self.actor)

        # Reconstruct base
        base = actor_cls.__new__(actor_cls)
        if base_state is not None:
            try:
                base.__init__()  # type: ignore
            except TypeError:
                pass
            base.set_state(base_state)
        else:
            # No base available - use initial state
            try:
                base.__init__()  # type: ignore
            except TypeError:
                pass

        # Reconstruct other
        other = actor_cls.__new__(actor_cls)
        try:
            other.__init__()  # type: ignore
        except TypeError:
            pass
        other.set_state(other_state)

        # Call user's merge implementation
        self.actor.__casty_merge__(base, other)  # type: ignore

        # Update version to max + 1 to ensure ordering
        self._state.version = max(self._state.version, other_version) + 1

    def needs_merge(self, other_version: ActorVersion) -> bool:
        """Check if merge with another version is needed."""
        my_version = self.get_actor_version()
        return my_version.needs_merge(other_version)


def is_mergeable(actor: Any) -> bool:
    """Check if an actor implements the Mergeable protocol."""
    return isinstance(actor, Mergeable)
