"""Mergeable protocol for three-way merge support.

Defines the interface that actors must implement to support
conflict-free merging across distributed nodes.
"""

from __future__ import annotations

from typing import Any, Protocol, Self, runtime_checkable


@runtime_checkable
class Mergeable(Protocol):
    """Protocol for actors that support three-way merge.

    Any actor that implements __casty_merge__ is considered mergeable.
    The framework will detect this and enable conflict resolution
    during network partitions.

    The merge method receives:
    - self: Your current state
    - base: The common ancestor state (before divergence)
    - other: The state from the other node

    After the method returns, `self` should contain the merged state.

    Example:
        class Account(Actor):
            def __init__(self):
                self.balance = 0

            async def receive(self, msg, ctx):
                match msg:
                    case Deposit(amount):
                        self.balance += amount

            def __casty_merge__(self, base: 'Account', other: 'Account'):
                # Sum deltas for concurrent deposits
                my_delta = self.balance - base.balance
                their_delta = other.balance - base.balance
                self.balance = base.balance + my_delta + their_delta
    """

    def get_state(self) -> dict[str, Any]:
        """Get actor state for serialization."""
        ...

    def set_state(self, state: dict[str, Any]) -> None:
        """Restore actor state from serialization."""
        ...

    def __casty_merge__(self, base: Self, other: Self) -> None:
        """Merge concurrent modifications using three-way merge.

        Args:
            base: State at the point of divergence (common ancestor)
            other: State from the conflicting node

        After this method, `self` should contain the merged state.
        """
        ...
