from __future__ import annotations

from typing import Literal

type Replication = Literal['all', 'quorum'] | int
type Consistency = Literal['all', 'quorum', 'one', 'async'] | int


def resolve_replication(replication: Replication, total_nodes: int) -> int:
    match replication:
        case 'all':
            return total_nodes
        case 'quorum':
            return total_nodes // 2 + 1
        case int(n):
            return min(n, total_nodes)


def resolve_consistency(consistency: Consistency, replicas: int) -> int:
    match consistency:
        case 'async':
            return 0
        case 'one':
            return 1
        case 'all':
            return replicas
        case 'quorum':
            return replicas // 2 + 1
        case int(n):
            return min(n, replicas)
