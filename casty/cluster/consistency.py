"""Shard consistency levels for Casty cluster.

This module defines an ADT (Algebraic Data Type) for shard registration
consistency levels. These determine how many nodes must acknowledge
the registration before spawn() returns.

Example:
    # Wait for all nodes to register
    await system.spawn(MyActor, name="my-type", sharded=True, consistency=Strong())

    # Fire-and-forget (default)
    await system.spawn(MyActor, name="my-type", sharded=True, consistency=Eventual())

    # Wait for majority
    await system.spawn(MyActor, name="my-type", sharded=True, consistency=Quorum())

    # Wait for at least N nodes
    await system.spawn(MyActor, name="my-type", sharded=True, consistency=AtLeast(3))
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


class ShardConsistency(ABC):
    """ADT base class for shard registration consistency levels.

    Determines how many nodes must acknowledge a sharded type registration
    before the spawn() call returns.
    """

    @abstractmethod
    def required_acks(self, total_nodes: int) -> int:
        """Calculate required acknowledgments for given cluster size.

        Args:
            total_nodes: Total number of nodes in the cluster (including self)

        Returns:
            Number of nodes that must acknowledge the registration
        """
        ...

    @abstractmethod
    def should_wait(self) -> bool:
        """Whether this consistency level requires waiting for acks.

        Returns:
            True if we should wait for acknowledgments, False for fire-and-forget
        """
        ...


@dataclass(frozen=True, slots=True)
class Strong(ShardConsistency):
    """Wait for all nodes to acknowledge the registration.

    Most consistent option - guarantees all nodes know about the sharded
    type before spawn() returns. Can be slow in large clusters or if
    nodes are unreachable.
    """

    def required_acks(self, total_nodes: int) -> int:
        return total_nodes

    def should_wait(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "Strong()"


@dataclass(frozen=True, slots=True)
class Eventual(ShardConsistency):
    """Fire-and-forget registration (default behavior).

    Registration is published via Gossip and will eventually propagate
    to all nodes. spawn() returns immediately without waiting.
    This is the fastest option but remote nodes may not know about
    the sharded type immediately.
    """

    def required_acks(self, total_nodes: int) -> int:  # noqa: ARG002
        del total_nodes  # Not used for Eventual
        return 0

    def should_wait(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "Eventual()"


@dataclass(frozen=True, slots=True)
class Quorum(ShardConsistency):
    """Wait for majority of nodes to acknowledge.

    Balances consistency and availability. spawn() returns once
    a majority (n/2 + 1) of nodes have acknowledged. This ensures
    most routing decisions will find the sharded type already registered.
    """

    def required_acks(self, total_nodes: int) -> int:
        return (total_nodes // 2) + 1

    def should_wait(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "Quorum()"


@dataclass(frozen=True, slots=True)
class AtLeast(ShardConsistency):
    """Wait for at least N nodes to acknowledge.

    Useful when you want a specific number of replicas to be aware
    of the sharded type before proceeding.

    Args:
        count: Minimum number of nodes that must acknowledge

    Raises:
        ValueError: If count is less than 1
    """

    count: int

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError("AtLeast count must be at least 1")

    def required_acks(self, total_nodes: int) -> int:
        # Don't require more acks than available nodes
        return min(self.count, total_nodes)

    def should_wait(self) -> bool:
        return True

    def __repr__(self) -> str:
        return f"AtLeast({self.count})"


# Type alias for consistency union
ConsistencyLevel = Strong | Eventual | Quorum | AtLeast

# Default consistency level
DEFAULT_CONSISTENCY: ShardConsistency = Eventual()
