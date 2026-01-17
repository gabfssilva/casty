from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class VirtualNode:
    node_id: str
    index: int
    position: int


class HashRing:
    RING_SIZE = 2**32

    def __init__(self, virtual_nodes: int = 150) -> None:
        self._virtual_nodes = virtual_nodes
        self._ring: list[VirtualNode] = []
        self._positions: list[int] = []
        self._nodes: set[str] = set()

    def _hash(self, key: str) -> int:
        digest = hashlib.md5(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:4], "big")

    def add_node(self, node_id: str) -> None:
        if node_id in self._nodes:
            return

        self._nodes.add(node_id)

        for i in range(self._virtual_nodes):
            vnode_key = f"{node_id}#vnode{i}"
            position = self._hash(vnode_key)
            vnode = VirtualNode(node_id, i, position)

            idx = bisect.bisect_left(self._positions, position)
            self._ring.insert(idx, vnode)
            self._positions.insert(idx, position)

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            return

        self._nodes.discard(node_id)

        new_ring: list[VirtualNode] = []
        new_positions: list[int] = []

        for vnode, pos in zip(self._ring, self._positions):
            if vnode.node_id != node_id:
                new_ring.append(vnode)
                new_positions.append(pos)

        self._ring = new_ring
        self._positions = new_positions

    def get_node(self, key: str) -> str | None:
        if not self._ring:
            return None

        hash_val = self._hash(key)
        idx = bisect.bisect_left(self._positions, hash_val)

        if idx >= len(self._ring):
            idx = 0

        return self._ring[idx].node_id

    def get_preference_list(self, key: str, n: int = 3) -> list[str]:
        if not self._ring:
            return []

        hash_val = self._hash(key)
        idx = bisect.bisect_left(self._positions, hash_val)

        if idx >= len(self._ring):
            idx = 0

        result: list[str] = []
        seen: set[str] = set()

        for i in range(len(self._ring)):
            vnode = self._ring[(idx + i) % len(self._ring)]
            if vnode.node_id not in seen:
                seen.add(vnode.node_id)
                result.append(vnode.node_id)
                if len(result) >= n:
                    break

        return result

    def is_responsible(self, node_id: str, key: str) -> bool:
        return self.get_node(key) == node_id

    @property
    def nodes(self) -> frozenset[str]:
        return frozenset(self._nodes)

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @property
    def vnode_count(self) -> int:
        return len(self._ring)

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    def __repr__(self) -> str:
        return f"HashRing(nodes={len(self._nodes)}, vnodes={len(self._ring)})"
