from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from .hash_ring import HashRing


class MembershipProtocol(Protocol):
    def is_alive(self, node_id: str) -> bool: ...


@dataclass
class ShardCoordinator:
    node_id: str
    hash_ring: HashRing
    membership: MembershipProtocol

    def get_responsible_nodes(self, actor_id: str, replicas: int = 3) -> list[str]:
        return self.hash_ring.get_n_nodes(actor_id, replicas)

    def _get_alive_nodes(self, actor_id: str, replicas: int = 3) -> list[str]:
        responsible = self.get_responsible_nodes(actor_id, replicas)
        return [n for n in responsible if self.membership.is_alive(n)]

    def get_leader_id(self, actor_id: str, replicas: int = 3) -> str:
        alive = self._get_alive_nodes(actor_id, replicas)
        if not alive:
            raise RuntimeError(f"No alive nodes for actor {actor_id}")
        return alive[0]

    def is_leader(self, actor_id: str, replicas: int = 3) -> bool:
        return self.get_leader_id(actor_id, replicas) == self.node_id

    def get_replica_ids(self, actor_id: str, replicas: int = 3) -> list[str]:
        alive = self._get_alive_nodes(actor_id, replicas)
        leader = alive[0] if alive else None
        return [n for n in alive if n != leader]
