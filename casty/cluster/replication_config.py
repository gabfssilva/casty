"""Replication configuration for distributed actors.

This module provides the Replication dataclass for configuring replication
factor and consistency level when spawning replicated actors.

Example:
    # Replicate with 3 copies, wait for quorum
    repl = Replication(factor=3, consistency=Quorum())
    await system.spawn(Account, name="accounts", sharded=True, replication=repl)

    # Replicate with 2 copies, fire-and-forget
    repl = Replication(factor=2, consistency=Eventual())
    await system.spawn(Leader, singleton=True, replication=repl)
"""

from __future__ import annotations

from dataclasses import dataclass

from .consistency import ShardConsistency, Eventual, Quorum


@dataclass(frozen=True, slots=True)
class Replication:
    """Configuration for actor replication across cluster.

    Attributes:
        factor: Number of replicas (1 = no replication, 3 = primary + 2 replicas)
        consistency: Consistency level for replication (default: Eventual if factor==1, Quorum if factor>1)
    """

    factor: int = 1
    consistency: ShardConsistency | None = None

    def __post_init__(self) -> None:
        """Validate replication configuration."""
        if self.factor < 1:
            raise ValueError("Replication factor must be >= 1")

    def resolve_consistency(self) -> ShardConsistency:
        """Resolve consistency level with sensible defaults.

        Returns:
            Eventual() if factor==1 (no replication), else Quorum() or configured consistency
        """
        if self.consistency is not None:
            return self.consistency
        # Default: Eventual for single node, Quorum for multiple replicas
        return Eventual() if self.factor == 1 else Quorum()
