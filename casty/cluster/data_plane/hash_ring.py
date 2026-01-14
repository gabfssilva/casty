"""Consistent hashing for entity distribution.

Provides a hash ring with virtual nodes for even distribution
and minimal rebalancing when nodes join/leave the cluster.
"""

from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class VirtualNode:
    """A virtual node on the hash ring.

    Multiple virtual nodes per physical node ensures even distribution.
    """

    node_id: str  # Physical node identifier
    virtual_id: int  # Virtual node number (0 to num_vnodes-1)
    position: int  # Position on the ring (0 to 2^32-1)

    def __lt__(self, other: "VirtualNode") -> bool:
        return self.position < other.position


@dataclass
class HashRingConfig:
    """Configuration for the hash ring.

    Attributes:
        virtual_nodes: Number of virtual nodes per physical node
        hash_function: Hash function to use (md5 is deterministic)
    """

    virtual_nodes: int = 150
    hash_function: str = "md5"


class HashRing:
    """Consistent hash ring with virtual nodes.

    Features:
    - O(log n) key lookup via binary search
    - Virtual nodes for even distribution
    - Minimal rebalancing on node join/leave
    - Preference list for replication

    Usage:
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        # Get primary node for a key
        primary = ring.get_node("user:123")

        # Get preference list for replication
        replicas = ring.get_preference_list("user:123", n=3)
    """

    RING_SIZE = 2**32  # 32-bit hash space

    def __init__(self, config: HashRingConfig | None = None):
        self._config = config or HashRingConfig()
        self._ring: list[VirtualNode] = []  # Sorted by position
        self._positions: list[int] = []  # Just positions for bisect
        self._nodes: set[str] = set()  # Physical node IDs
        self._vnode_map: dict[str, list[VirtualNode]] = {}  # node_id -> vnodes

    def _hash(self, key: str) -> int:
        """Compute consistent hash for a key.

        Uses MD5 for deterministic hashing (not for security).
        """
        h = hashlib.md5(key.encode("utf-8")).digest()
        return int.from_bytes(h[:4], "big")

    def _vnode_key(self, node_id: str, virtual_id: int) -> str:
        """Generate key for virtual node hashing."""
        return f"{node_id}#vnode{virtual_id}"

    def add_node(self, node_id: str) -> list[tuple[int, int, str]]:
        """Add a physical node with virtual nodes.

        Args:
            node_id: Unique identifier for the node

        Returns:
            List of (start_pos, end_pos, from_node) representing
            key ranges that moved to this new node.
        """
        if node_id in self._nodes:
            return []

        self._nodes.add(node_id)
        vnodes: list[VirtualNode] = []
        moves: list[tuple[int, int, str]] = []

        for i in range(self._config.virtual_nodes):
            key = self._vnode_key(node_id, i)
            position = self._hash(key)
            vnode = VirtualNode(node_id, i, position)
            vnodes.append(vnode)

            # Calculate what range this vnode takes over
            if self._ring:
                idx = bisect.bisect_left(self._positions, position)
                # The range [prev_position+1, position] moves to new node
                prev_idx = (idx - 1) % len(self._ring)
                prev_vnode = self._ring[prev_idx]
                if prev_vnode.node_id != node_id:
                    # Find who was responsible for this range
                    next_idx = idx % len(self._ring)
                    if next_idx < len(self._ring):
                        old_owner = self._ring[next_idx].node_id
                        if old_owner != node_id:
                            moves.append(
                                (prev_vnode.position + 1, position, old_owner)
                            )

            # Insert into ring maintaining sort order
            idx = bisect.bisect_left(self._positions, position)
            self._ring.insert(idx, vnode)
            self._positions.insert(idx, position)

        self._vnode_map[node_id] = vnodes
        return moves

    def remove_node(self, node_id: str) -> list[tuple[int, int, str]]:
        """Remove a physical node and its virtual nodes.

        Args:
            node_id: The node to remove

        Returns:
            List of (start_pos, end_pos, to_node) representing
            key ranges that need to move to other nodes.
        """
        if node_id not in self._nodes:
            return []

        self._nodes.remove(node_id)
        vnodes = self._vnode_map.pop(node_id, [])
        moves: list[tuple[int, int, str]] = []

        for vnode in vnodes:
            try:
                idx = self._positions.index(vnode.position)
            except ValueError:
                continue

            if self._ring[idx] == vnode:
                # Find new owner for this range
                if len(self._ring) > 1:
                    prev_idx = (idx - 1) % len(self._ring)
                    next_idx = (idx + 1) % len(self._ring)

                    new_owner = self._ring[next_idx].node_id
                    start_pos = self._ring[prev_idx].position + 1
                    moves.append((start_pos, vnode.position, new_owner))

                self._ring.pop(idx)
                self._positions.pop(idx)

        return moves

    def get_node(self, key: str) -> str | None:
        """Get primary node for a key.

        Args:
            key: The key to look up

        Returns:
            Node ID responsible for the key, or None if ring is empty
        """
        if not self._ring:
            return None

        hash_val = self._hash(key)
        idx = bisect.bisect_left(self._positions, hash_val)
        if idx >= len(self._ring):
            idx = 0  # Wrap around

        return self._ring[idx].node_id

    def get_preference_list(self, key: str, n: int = 3) -> list[str]:
        """Get ordered list of nodes for a key (primary + replicas).

        Returns up to n unique physical nodes. The first node is
        the primary, rest are replicas for replication.

        Args:
            key: The key to look up
            n: Maximum number of nodes to return

        Returns:
            List of node IDs in preference order
        """
        if not self._ring:
            return []

        hash_val = self._hash(key)
        idx = bisect.bisect_left(self._positions, hash_val)
        if idx >= len(self._ring):
            idx = 0

        result: list[str] = []
        seen: set[str] = set()

        # Walk around the ring to find unique physical nodes
        for i in range(len(self._ring)):
            vnode = self._ring[(idx + i) % len(self._ring)]
            if vnode.node_id not in seen:
                seen.add(vnode.node_id)
                result.append(vnode.node_id)
                if len(result) >= n:
                    break

        return result

    def get_ranges_for_node(self, node_id: str) -> list[tuple[int, int]]:
        """Get all hash ranges owned by a node.

        Args:
            node_id: The node to query

        Returns:
            List of (start, end) tuples representing contiguous ranges
        """
        if node_id not in self._nodes:
            return []

        ranges: list[tuple[int, int]] = []
        vnodes = self._vnode_map.get(node_id, [])

        for vnode in sorted(vnodes, key=lambda v: v.position):
            # Find the previous vnode position
            try:
                idx = self._positions.index(vnode.position)
            except ValueError:
                continue

            prev_idx = (idx - 1) % len(self._ring)
            start = (self._ring[prev_idx].position + 1) % self.RING_SIZE
            end = vnode.position
            ranges.append((start, end))

        return ranges

    def get_nodes_for_range(
        self, start: int, end: int, n: int = 3
    ) -> list[str]:
        """Get nodes responsible for a hash range.

        Args:
            start: Start of the range
            end: End of the range
            n: Maximum nodes to return

        Returns:
            List of node IDs
        """
        if not self._ring:
            return []

        # Find the node at the start position
        idx = bisect.bisect_left(self._positions, start)
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

    @property
    def nodes(self) -> frozenset[str]:
        """Get all physical nodes in the ring."""
        return frozenset(self._nodes)

    @property
    def node_count(self) -> int:
        """Get number of physical nodes."""
        return len(self._nodes)

    @property
    def vnode_count(self) -> int:
        """Get total number of virtual nodes."""
        return len(self._ring)

    def __len__(self) -> int:
        """Number of physical nodes."""
        return len(self._nodes)

    def __contains__(self, node_id: str) -> bool:
        """Check if a node is in the ring."""
        return node_id in self._nodes

    def __repr__(self) -> str:
        return f"HashRing(nodes={len(self._nodes)}, vnodes={len(self._ring)})"
