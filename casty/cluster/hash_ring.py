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
            if key not in self._ring:
                self._ring[key] = node
                bisect.insort(self._sorted_keys, key)

    def remove_node(self, node: str) -> None:
        for i in range(self._virtual_nodes):
            key = self._hash(f"{node}:{i}")
            if key in self._ring:
                del self._ring[key]
                idx = bisect.bisect_left(self._sorted_keys, key)
                if idx < len(self._sorted_keys) and self._sorted_keys[idx] == key:
                    self._sorted_keys.pop(idx)

    def get_node(self, actor_id: str) -> str:
        if not self._ring:
            raise RuntimeError("HashRing is empty")

        key = self._hash(actor_id)
        idx = bisect.bisect_left(self._sorted_keys, key)
        if idx == len(self._sorted_keys):
            idx = 0

        return self._ring[self._sorted_keys[idx]]

    @property
    def nodes(self) -> set[str]:
        return set(self._ring.values())

    def get_n_nodes(self, key: str, n: int) -> list[str]:
        """Get N nodes for a key, starting from the key's position and going clockwise."""
        if not self._ring or n <= 0:
            return []

        key_hash = self._hash(key)
        idx = bisect.bisect_left(self._sorted_keys, key_hash)
        if idx >= len(self._sorted_keys):
            idx = 0

        result: list[str] = []
        seen: set[str] = set()

        for i in range(len(self._sorted_keys)):
            pos = (idx + i) % len(self._sorted_keys)
            node = self._ring[self._sorted_keys[pos]]

            if node not in seen:
                result.append(node)
                seen.add(node)

            if len(result) == n:
                break

        return result

    def get_nodes(self, actor_id: str, n: int) -> list[str]:
        """Alias for get_n_nodes for backward compatibility."""
        return self.get_n_nodes(actor_id, n)
