"""SWIM+ protocol state structures.

This module defines the state structures used by SWIM+ actors:
- MemberState: State of a cluster member
- MemberInfo: Full information about a member
- SuspicionState: Lifeguard suspicion tracking
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from math import log, log10


class MemberState(Enum):
    """State of a cluster member."""

    ALIVE = "alive"
    SUSPECTED = "suspected"
    DEAD = "dead"


@dataclass
class MemberInfo:
    """Information about a cluster member."""

    node_id: str
    address: tuple[str, int]
    state: MemberState = MemberState.ALIVE
    incarnation: int = 0
    last_seen: float = field(default_factory=time.time)

    def update_last_seen(self) -> None:
        """Update last seen timestamp."""
        self.last_seen = time.time()

    def is_alive(self) -> bool:
        """Check if member is alive."""
        return self.state == MemberState.ALIVE

    def is_suspected(self) -> bool:
        """Check if member is suspected."""
        return self.state == MemberState.SUSPECTED

    def is_dead(self) -> bool:
        """Check if member is dead."""
        return self.state == MemberState.DEAD


@dataclass
class SuspicionState:
    """Lifeguard suspicion state for a member.

    Implements adaptive timeout based on independent confirmations.
    The timeout decreases logarithmically as more nodes confirm the suspicion.
    """

    node_id: str
    first_reporter: str
    started_at: float = field(default_factory=time.time)
    confirmation_count: int = 0
    confirmers: set[str] = field(default_factory=set)

    # Timeout parameters (set by SuspicionActor)
    min_timeout: float = 1.0
    max_timeout: float = 10.0
    confirmation_threshold: int = 5

    def add_confirmation(self, reporter: str) -> bool:
        """Add an independent confirmation.

        Returns True if this is a new confirmation.
        """
        if reporter in self.confirmers:
            return False

        self.confirmers.add(reporter)
        self.confirmation_count = len(self.confirmers)
        return True

    def calculate_timeout(self) -> float:
        """Calculate adaptive timeout based on confirmations.

        Uses logarithmic decay formula:
        fraction = log(n + 1) / log(k + 1)
        timeout = max - fraction * (max - min)

        Where:
        - n = number of confirmations
        - k = confirmation threshold
        """
        if self.confirmation_count >= self.confirmation_threshold:
            return self.min_timeout

        n = self.confirmation_count
        k = self.confirmation_threshold

        fraction = log(n + 1) / log(k + 1)
        timeout = self.max_timeout - fraction * (self.max_timeout - self.min_timeout)

        return max(timeout, self.min_timeout)

    def remaining_timeout(self) -> float:
        """Calculate remaining timeout from now."""
        elapsed = time.time() - self.started_at
        timeout = self.calculate_timeout()
        return max(0.0, timeout - elapsed)

    def is_expired(self) -> bool:
        """Check if suspicion timeout has expired."""
        return self.remaining_timeout() <= 0


@dataclass
class ProbeList:
    """Round-robin probe list with shuffle.

    Maintains deterministic probing order for fair failure detection.
    """

    members: list[str] = field(default_factory=list)
    index: int = 0
    generation: int = 0

    def add_member(self, node_id: str) -> None:
        """Add a member to the probe list."""
        if node_id not in self.members:
            # Insert at random position for fairness
            import random

            pos = random.randint(0, len(self.members))
            self.members.insert(pos, node_id)

    def remove_member(self, node_id: str) -> None:
        """Remove a member from the probe list."""
        if node_id in self.members:
            idx = self.members.index(node_id)
            self.members.remove(node_id)
            # Adjust index if necessary
            if idx < self.index:
                self.index -= 1
            if self.index >= len(self.members):
                self.index = 0

    def next_target(self) -> str | None:
        """Get next probe target (round-robin).

        Returns None if list is empty.
        Shuffles and resets when list is exhausted.
        """
        if not self.members:
            return None

        # Get current target
        target = self.members[self.index]

        # Advance index
        self.index += 1

        # Check if we've completed a round
        if self.index >= len(self.members):
            self._shuffle_and_reset()

        return target

    def _shuffle_and_reset(self) -> None:
        """Shuffle the list and reset index."""
        import random

        random.shuffle(self.members)
        self.index = 0
        self.generation += 1

    def __len__(self) -> int:
        return len(self.members)


def calculate_min_suspicion_timeout(
    cluster_size: int,
    protocol_period: float,
    alpha: float = 5.0,
) -> float:
    """Calculate minimum suspicion timeout.

    Formula: α × log₁₀(cluster_size) × protocol_period
    """
    if cluster_size <= 1:
        return alpha * protocol_period

    return alpha * log10(cluster_size) * protocol_period


def calculate_max_suspicion_timeout(
    min_timeout: float,
    beta: float = 6.0,
) -> float:
    """Calculate maximum suspicion timeout.

    Formula: β × min_timeout
    """
    return beta * min_timeout
