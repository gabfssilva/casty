from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .hash_ring import HashRing


@dataclass
class ReplicaInfo:
    actor_id: str
    nodes: list[str]
    leader: str


@dataclass
class ReplicaManager:
    _replicas: dict[str, ReplicaInfo] = field(default_factory=dict)

    def register(self, actor_id: str, nodes: list[str], leader: str) -> None:
        self._replicas[actor_id] = ReplicaInfo(
            actor_id=actor_id,
            nodes=list(nodes),
            leader=leader
        )

    def get(self, actor_id: str) -> ReplicaInfo | None:
        return self._replicas.get(actor_id)

    def get_leader(self, actor_id: str) -> str | None:
        info = self._replicas.get(actor_id)
        return info.leader if info else None

    def get_replicas(self, actor_id: str) -> list[str]:
        info = self._replicas.get(actor_id)
        return list(info.nodes) if info else []

    def is_leader(self, actor_id: str, node_id: str) -> bool:
        return self.get_leader(actor_id) == node_id

    def set_leader(self, actor_id: str, node_id: str) -> None:
        info = self._replicas.get(actor_id)
        if info and node_id in info.nodes:
            info.leader = node_id

    def remove_node(self, actor_id: str, node_id: str) -> None:
        info = self._replicas.get(actor_id)
        if not info:
            return

        if node_id in info.nodes:
            info.nodes.remove(node_id)

        if info.leader == node_id and info.nodes:
            info.leader = info.nodes[0]

    def add_node(self, actor_id: str, node_id: str) -> None:
        info = self._replicas.get(actor_id)
        if info and node_id not in info.nodes:
            info.nodes.append(node_id)

    def get_actors_for_node(self, node_id: str) -> list[str]:
        return [
            actor_id
            for actor_id, info in self._replicas.items()
            if node_id in info.nodes
        ]

    def get_actors_where_leader(self, node_id: str) -> list[str]:
        return [
            actor_id
            for actor_id, info in self._replicas.items()
            if info.leader == node_id
        ]

    def get_actors_needing_rebalance(self, target_replicas: int) -> list[str]:
        return [
            actor_id
            for actor_id, info in self._replicas.items()
            if len(info.nodes) < target_replicas
        ]

    def suggest_new_replica(self, actor_id: str, ring: HashRing) -> str | None:
        info = self._replicas.get(actor_id)
        if not info:
            return None

        candidates = ring.get_n_nodes(actor_id, n=len(ring.nodes))

        for node in candidates:
            if node not in info.nodes:
                return node

        return None

    def handle_node_down(self, node_id: str) -> list[tuple[str, str]]:
        affected: list[tuple[str, str]] = []

        for actor_id, info in self._replicas.items():
            if node_id not in info.nodes:
                continue

            info.nodes.remove(node_id)

            if info.leader == node_id:
                if info.nodes:
                    info.leader = info.nodes[0]
                    affected.append((actor_id, info.leader))
                else:
                    info.leader = ""

        return affected
