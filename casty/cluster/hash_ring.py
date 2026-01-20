from __future__ import annotations

import bisect
import hashlib


class HashRing:
    def __init__(self, virtual_nodes: int = 150) -> None:
        self._ring: dict[int, str] = {}
        self._sorted_keys: list[int] = []
        self._virtual_nodes = virtual_nodes

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        for i in range(self._virtual_nodes):
            key = self._hash(f"{node}:{i}")
            self._ring[key] = node
        self._sorted_keys = sorted(self._ring.keys())

    def remove_node(self, node: str) -> None:
        for i in range(self._virtual_nodes):
            key = self._hash(f"{node}:{i}")
            self._ring.pop(key, None)
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, actor_id: str) -> str:
        if not self._ring:
            raise RuntimeError("HashRing is empty")

        key = self._hash(actor_id)
        idx = bisect.bisect_left(self._sorted_keys, key)
        if idx == len(self._sorted_keys):
            idx = 0

        return self._ring[self._sorted_keys[idx]]

    def get_nodes(self, actor_id: str, n: int) -> list[str]:
        if not self._ring:
            raise RuntimeError("HashRing is empty")

        key = self._hash(actor_id)
        idx = bisect.bisect_left(self._sorted_keys, key)

        nodes = []
        seen = set()

        for i in range(len(self._sorted_keys)):
            pos = (idx + i) % len(self._sorted_keys)
            node = self._ring[self._sorted_keys[pos]]

            if node not in seen:
                nodes.append(node)
                seen.add(node)

            if len(nodes) == n:
                break

        return nodes
