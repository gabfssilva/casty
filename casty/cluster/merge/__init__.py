"""Three-way merge support for distributed actors.

This module provides tools for conflict-free merging of actor state
across distributed nodes. When concurrent modifications occur during
network partitions, the framework can intelligently merge states
using a three-way merge algorithm (similar to git).

Core Components:
    Mergeable: Protocol for actors that support merging
    VectorClock: Vector clock for causality tracking (re-exported from casty.wal)
    is_mergeable: Check if an actor implements Mergeable

Merge Helpers:
    merge_sum: Sum deltas for numeric values (counters, balances)
    merge_max: Take maximum value (high-water marks)
    merge_min: Take minimum value (low-water marks)
    merge_lww: Last-writer-wins based on timestamp
    merge_union: Set union (add-wins)
    merge_intersection: Set intersection (remove-wins)
    merge_set_add_remove: Set merge with add/remove tracking
    merge_list_append: List merge by appending new items
    merge_dict_shallow: Dictionary merge at key level

Example:
    from casty.cluster.merge import Mergeable, merge_sum

    class Account(Actor):
        def __init__(self):
            self.balance = 0

        async def receive(self, msg, ctx):
            match msg:
                case Deposit(amount):
                    self.balance += amount
                case GetBalance():
                    await ctx.reply(self.balance)

        def get_state(self) -> dict:
            return {"balance": self.balance}

        def set_state(self, state: dict) -> None:
            self.balance = state["balance"]

        def __casty_merge__(self, base: 'Account', other: 'Account'):
            # Concurrent deposits add up correctly
            self.balance = merge_sum(base.balance, self.balance, other.balance)
"""

from .helpers import (
    merge_dict_shallow,
    merge_intersection,
    merge_list_append,
    merge_lww,
    merge_max,
    merge_min,
    merge_set_add_remove,
    merge_sum,
    merge_union,
)
from .protocol import Mergeable

from casty.wal import VectorClock


def is_mergeable(actor: object) -> bool:
    """Check if an actor implements the Mergeable protocol."""
    return (
        hasattr(actor, "__casty_merge__")
        and hasattr(actor, "get_state")
        and hasattr(actor, "set_state")
    )


__all__ = [
    # Protocol
    "Mergeable",
    "is_mergeable",
    # Versioning (re-exported from casty.wal)
    "VectorClock",
    # Helpers
    "merge_sum",
    "merge_max",
    "merge_min",
    "merge_lww",
    "merge_union",
    "merge_intersection",
    "merge_set_add_remove",
    "merge_list_append",
    "merge_dict_shallow",
]
