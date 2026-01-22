from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from casty.serializable import serializable


@serializable
@dataclass
class VectorClock:
    versions: dict[str, int] = field(default_factory=dict)

    def increment(self, node_id: str) -> None:
        self.versions[node_id] = self.versions.get(node_id, 0) + 1

    def merge(self, other: VectorClock) -> None:
        for node_id, version in other.versions.items():
            self.versions[node_id] = max(self.versions.get(node_id, 0), version)

    def copy(self) -> VectorClock:
        return VectorClock(dict(self.versions))

    def compare(self, other: VectorClock) -> Literal["before", "after", "concurrent"]:
        all_nodes = set(self.versions) | set(other.versions)

        self_le_other = all(
            self.versions.get(n, 0) <= other.versions.get(n, 0)
            for n in all_nodes
        )
        other_le_self = all(
            other.versions.get(n, 0) <= self.versions.get(n, 0)
            for n in all_nodes
        )

        if self_le_other and not other_le_self:
            return "before"
        if other_le_self and not self_le_other:
            return "after"
        return "concurrent"

    def is_before_or_equal(self, other: VectorClock) -> bool:
        all_nodes = set(self.versions) | set(other.versions)
        return all(
            self.versions.get(n, 0) <= other.versions.get(n, 0)
            for n in all_nodes
        )

    def meet(self, other: VectorClock) -> VectorClock:
        all_nodes = set(self.versions) | set(other.versions)
        return VectorClock({
            node: min(self.versions.get(node, 0), other.versions.get(node, 0))
            for node in all_nodes
        })
