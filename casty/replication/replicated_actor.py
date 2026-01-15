"""Replicated actor wrapper with state change detection.

This module provides the ReplicatedActorWrapper that wraps actors
with automatic state change detection for WAL-based replication.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from .wal_entry import ReplicationWALEntry, WALGapError

if TYPE_CHECKING:
    from casty import Actor, Context

log = logging.getLogger(__name__)

M = TypeVar("M")


@dataclass
class ReplicatedActorState:
    """Tracks replication state for a wrapped actor."""

    sequence: int = 0
    node_id: str = ""
    is_primary: bool = True
    last_entry: ReplicationWALEntry | None = None


class ReplicatedActorWrapper(Generic[M]):
    """Wraps an actor with state change detection for WAL-based replication.

    This wrapper intercepts message processing to detect state changes.
    When state changes:
    - A ReplicationWALEntry is created with the new state
    - The entry is returned for coordination/replication

    When state doesn't change (read-only):
    - No WAL entry is created
    - No replication happens (optimization!)

    For replicas (non-primary nodes):
    - Messages are not processed directly
    - State is applied via apply_wal_entry() from the primary's WAL

    State access:
    - If the actor implements get_state()/set_state(), those are used
    - Otherwise, state is inferred from public attributes (no underscore prefix)
    - This makes WAL-based replication transparent for any actor

    Attributes:
        actor: The wrapped actor instance
        entity_type: Actor type name for replication
        entity_id: Entity identifier
        _state: Internal replication state tracking
    """

    __slots__ = (
        "actor", "entity_type", "entity_id", "_state", "_ctx",
        "_has_explicit_get_state", "_has_explicit_set_state"
    )

    def __init__(
        self,
        actor: Actor[M],
        entity_type: str,
        entity_id: str,
        node_id: str,
        is_primary: bool = True,
    ) -> None:
        """Initialize the wrapper.

        Args:
            actor: The actor instance to wrap
            entity_type: Actor type name (e.g., "counters")
            entity_id: Entity identifier (e.g., "counter-1")
            node_id: This node's identifier
            is_primary: Whether this node is the primary for this entity
        """
        self.actor = actor
        self.entity_type = entity_type
        self.entity_id = entity_id
        self._state = ReplicatedActorState(
            node_id=node_id,
            is_primary=is_primary,
        )
        self._ctx: Context[Any] | None = None

        # Check if actor implements explicit state methods
        # If not, we'll infer state from public attributes
        self._has_explicit_get_state = (
            hasattr(actor, "get_state")
            and callable(getattr(actor, "get_state", None))
        )
        self._has_explicit_set_state = (
            hasattr(actor, "set_state")
            and callable(getattr(actor, "set_state", None))
        )

    @property
    def sequence(self) -> int:
        """Current WAL sequence number."""
        return self._state.sequence

    @property
    def node_id(self) -> str:
        """Node that owns this wrapper."""
        return self._state.node_id

    @property
    def is_primary(self) -> bool:
        """Whether this is the primary replica."""
        return self._state.is_primary

    @property
    def last_entry(self) -> ReplicationWALEntry | None:
        """Last WAL entry created (if any)."""
        return self._state.last_entry

    def set_primary(self, is_primary: bool) -> None:
        """Update primary status (e.g., after failover)."""
        self._state.is_primary = is_primary

    def _get_actor_state(self) -> dict[str, Any]:
        """Get actor state - use explicit get_state() if available, else infer from public attrs."""
        if self._has_explicit_get_state:
            return self.actor.get_state()

        # Infer state from public attributes (no underscore prefix)
        state = {}
        for key, value in vars(self.actor).items():
            if not key.startswith("_"):
                state[key] = value
        return state

    def _set_actor_state(self, state: dict[str, Any]) -> None:
        """Set actor state - use explicit set_state() if available, else set public attrs."""
        if self._has_explicit_set_state:
            self.actor.set_state(state)
            return

        # Set public attributes directly
        for key, value in state.items():
            if not key.startswith("_"):
                setattr(self.actor, key, value)

    async def process_message(
        self,
        msg: M,
        ctx: Context[Any],
    ) -> ReplicationWALEntry | None:
        """Process a message with state change detection.

        This method:
        1. Captures state before processing (if available)
        2. Calls actor.receive()
        3. Captures state after processing (if available)
        4. If state changed, creates a WAL entry

        Args:
            msg: The message to process
            ctx: The actor context

        Returns:
            ReplicationWALEntry if state changed (write), None if unchanged (read)
            Also returns None if actor doesn't implement get_state/set_state

        Raises:
            ValueError: If called on a non-primary replica
        """
        if not self._state.is_primary:
            raise ValueError(
                f"Cannot process message on replica: {self.entity_type}:{self.entity_id}. "
                "Replicas should receive WAL entries via apply_wal_entry()."
            )

        # Store context for potential use
        self._ctx = ctx

        # Capture state BEFORE processing
        state_before = self._get_actor_state()

        # Process the message
        await self.actor.receive(msg, ctx)

        # Capture state AFTER processing
        state_after = self._get_actor_state()

        # Compare states
        if state_before == state_after:
            # Read-only operation: no WAL entry, no replication
            log.debug(
                f"[ReplicatedActorWrapper] Read-only: {self.entity_type}:{self.entity_id} "
                f"msg={type(msg).__name__}"
            )
            self._state.last_entry = None
            return None

        # State changed: create WAL entry
        self._state.sequence += 1
        message_type = type(msg).__name__

        entry = ReplicationWALEntry.create(
            sequence=self._state.sequence,
            entity_type=self.entity_type,
            entity_id=self.entity_id,
            state_dict=state_after,
            message_type=message_type,
            node_id=self._state.node_id,
        )

        self._state.last_entry = entry

        log.debug(
            f"[ReplicatedActorWrapper] Write: {self.entity_type}:{self.entity_id} "
            f"seq={entry.sequence} msg={message_type}"
        )

        return entry

    def apply_wal_entry(self, entry: ReplicationWALEntry) -> None:
        """Apply a WAL entry from the primary (replica side).

        This method directly applies the state from a WAL entry
        without re-processing the original message.

        Args:
            entry: The WAL entry to apply

        Raises:
            WALGapError: If there's a gap in sequence numbers (need catch-up)
            ValueError: If checksum verification fails
        """
        # Check for gaps
        expected_seq = self._state.sequence + 1
        if entry.sequence < expected_seq:
            # Already applied (duplicate or old entry)
            log.debug(
                f"[ReplicatedActorWrapper] Skipping old entry: "
                f"{self.entity_type}:{self.entity_id} seq={entry.sequence} "
                f"(current={self._state.sequence})"
            )
            return

        if entry.sequence > expected_seq:
            # Gap detected - need catch-up
            log.warning(
                f"[ReplicatedActorWrapper] Gap detected: "
                f"{self.entity_type}:{self.entity_id} "
                f"expected={expected_seq}, got={entry.sequence}"
            )
            raise WALGapError(expected_seq, entry.sequence)

        # Apply state directly (this also verifies checksum)
        state_dict = entry.get_state_dict()
        self._set_actor_state(state_dict)

        # Update sequence
        self._state.sequence = entry.sequence
        self._state.last_entry = entry

        log.debug(
            f"[ReplicatedActorWrapper] Applied: {self.entity_type}:{self.entity_id} "
            f"seq={entry.sequence} from node={entry.node_id}"
        )

    def apply_catch_up_entries(self, entries: list[ReplicationWALEntry]) -> int:
        """Apply multiple WAL entries for catch-up.

        Entries are sorted by sequence and applied in order.
        Duplicates and old entries are skipped.

        Args:
            entries: List of WAL entries to apply

        Returns:
            Number of entries actually applied
        """
        # Sort by sequence
        sorted_entries = sorted(entries, key=lambda e: e.sequence)

        applied = 0
        for entry in sorted_entries:
            if entry.sequence <= self._state.sequence:
                continue  # Skip already applied

            if entry.sequence != self._state.sequence + 1:
                # Still have a gap - stop here
                log.warning(
                    f"[ReplicatedActorWrapper] Catch-up incomplete: "
                    f"{self.entity_type}:{self.entity_id} "
                    f"stopped at seq={self._state.sequence}, entry={entry.sequence}"
                )
                break

            # Apply entry
            state_dict = entry.get_state_dict()
            self._set_actor_state(state_dict)
            self._state.sequence = entry.sequence
            applied += 1

        log.info(
            f"[ReplicatedActorWrapper] Catch-up: {self.entity_type}:{self.entity_id} "
            f"applied {applied} entries, now at seq={self._state.sequence}"
        )

        return applied

    def get_state_snapshot(self) -> dict[str, Any]:
        """Get current actor state for debugging/inspection.

        Returns:
            Actor state dict (uses explicit get_state() or infers from public attrs)
        """
        return self._get_actor_state()

    def __repr__(self) -> str:
        return (
            f"ReplicatedActorWrapper("
            f"{self.entity_type}:{self.entity_id}, "
            f"seq={self._state.sequence}, "
            f"primary={self._state.is_primary})"
        )
