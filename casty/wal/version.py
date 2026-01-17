from __future__ import annotations

from dataclasses import dataclass, field

from casty.cluster.serializable import serializable


@serializable
@dataclass(slots=True)
class VectorClock:
    clock: dict[str, int] = field(default_factory=dict)

    def increment(self, node_id: str) -> VectorClock:
        new_clock = dict(self.clock)
        new_clock[node_id] = new_clock.get(node_id, 0) + 1
        return VectorClock(new_clock)

    def merge(self, other: VectorClock) -> VectorClock:
        all_nodes = set(self.clock) | set(other.clock)
        return VectorClock({
            node: max(self.clock.get(node, 0), other.clock.get(node, 0))
            for node in all_nodes
        })

    def dominates(self, other: VectorClock) -> bool:
        all_nodes = set(self.clock) | set(other.clock)
        all_gte = all(
            self.clock.get(node, 0) >= other.clock.get(node, 0)
            for node in all_nodes
        )
        any_gt = any(
            self.clock.get(node, 0) > other.clock.get(node, 0)
            for node in all_nodes
        )
        return all_gte and any_gt

    def concurrent(self, other: VectorClock) -> bool:
        return not self.dominates(other) and not other.dominates(self) and self != other

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return False
        all_nodes = set(self.clock) | set(other.clock)
        return all(
            self.clock.get(node, 0) == other.clock.get(node, 0)
            for node in all_nodes
        )

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.clock.items())))

    def __repr__(self) -> str:
        items = ", ".join(f"{k}:{v}" for k, v in sorted(self.clock.items()))
        return f"VectorClock({{{items}}})"

    def to_dict(self) -> dict[str, int]:
        return dict(self.clock)

    @classmethod
    def from_dict(cls, data: dict[str, int]) -> VectorClock:
        return cls(dict(data))
