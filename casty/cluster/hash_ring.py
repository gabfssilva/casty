"""Consistent hashing for entity distribution.

Provides a hash ring with virtual nodes for even distribution
and minimal rebalancing when nodes join/leave the cluster.

This is a pure data structure (not an actor) used by the Cluster
actor for deterministic entity placement.

Example:
    ring = HashRing()
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")

    # Get primary node for an entity
    primary = ring.get_node("users:user-123")

    # Get preference list for replication
    replicas = ring.get_preference_list("users:user-123", n=3)
"""

from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class VirtualNode:
    """A virtual node on the hash ring.

    Multiple virtual nodes per physical node ensures even distribution
    across the ring, preventing hotspots.
    """

    node_id: str  # Physical node identifier
    index: int  # Virtual node index (0 to num_vnodes-1)
    position: int  # Position on the ring (0 to 2^32-1)


class HashRing:
    """Consistent hash ring with virtual nodes.

    Features:
    - O(log n) key lookup via binary search
    - Virtual nodes for even distribution
    - Minimal rebalancing on node join/leave (~1/n keys move)
    - Preference list for replication

    The hash ring uses MD5 for deterministic hashing. This is NOT
    for security - it's chosen for consistent distribution and
    cross-platform reproducibility.
    """

    RING_SIZE = 2**32  # 32-bit hash space

    def __init__(self, virtual_nodes: int = 150) -> None:
        """Initialize an empty hash ring.

        Args:
            virtual_nodes: Number of virtual nodes per physical node.
                          Higher values = better distribution but more memory.
                          150 is a good default for most clusters.
        """
        self._virtual_nodes = virtual_nodes
        self._ring: list[VirtualNode] = []  # Sorted by position
        self._positions: list[int] = []  # Positions for binary search
        self._nodes: set[str] = set()  # Physical node IDs

    def _hash(self, key: str) -> int:
        """Compute consistent hash for a key.

        Uses first 4 bytes of MD5 for a 32-bit hash value.
        MD5 is chosen for determinism, not security.
        """
        digest = hashlib.md5(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:4], "big")

    def add_node(self, node_id: str) -> None:
        """Add a physical node with its virtual nodes.

        Args:
            node_id: Unique identifier for the node.
                    Typically "host:port" or a UUID.
        """
        if node_id in self._nodes:
            return

        self._nodes.add(node_id)

        for i in range(self._virtual_nodes):
            # Generate deterministic position for each virtual node
            vnode_key = f"{node_id}#vnode{i}"
            position = self._hash(vnode_key)
            vnode = VirtualNode(node_id, i, position)

            # Insert maintaining sorted order
            idx = bisect.bisect_left(self._positions, position)
            self._ring.insert(idx, vnode)
            self._positions.insert(idx, position)

    def remove_node(self, node_id: str) -> None:
        """Remove a physical node and all its virtual nodes.

        Args:
            node_id: The node to remove.
        """
        if node_id not in self._nodes:
            return

        self._nodes.discard(node_id)

        # Remove all virtual nodes for this physical node
        # Build new lists excluding the removed node's vnodes
        new_ring: list[VirtualNode] = []
        new_positions: list[int] = []

        for vnode, pos in zip(self._ring, self._positions):
            if vnode.node_id != node_id:
                new_ring.append(vnode)
                new_positions.append(pos)

        self._ring = new_ring
        self._positions = new_positions

    def get_node(self, key: str) -> str | None:
        """Get the primary node responsible for a key.

        Uses binary search for O(log n) lookup.

        Args:
            key: The key to look up (e.g., "users:user-123")

        Returns:
            Node ID responsible for the key, or None if ring is empty.
        """
        if not self._ring:
            return None

        hash_val = self._hash(key)
        idx = bisect.bisect_left(self._positions, hash_val)

        # Wrap around to first node if past the end
        if idx >= len(self._ring):
            idx = 0

        return self._ring[idx].node_id

    def get_preference_list(self, key: str, n: int = 3) -> list[str]:
        """Get ordered list of nodes for a key (primary + replicas).

        Walks the ring clockwise from the key's position, collecting
        unique physical nodes. First node is primary, rest are replicas.

        Args:
            key: The key to look up.
            n: Maximum number of nodes to return.

        Returns:
            List of node IDs in preference order. May return fewer
            than n nodes if the ring has fewer physical nodes.
        """
        if not self._ring:
            return []

        hash_val = self._hash(key)
        idx = bisect.bisect_left(self._positions, hash_val)

        if idx >= len(self._ring):
            idx = 0

        result: list[str] = []
        seen: set[str] = set()

        # Walk clockwise around the ring
        for i in range(len(self._ring)):
            vnode = self._ring[(idx + i) % len(self._ring)]
            if vnode.node_id not in seen:
                seen.add(vnode.node_id)
                result.append(vnode.node_id)
                if len(result) >= n:
                    break

        return result

    def is_responsible(self, node_id: str, key: str) -> bool:
        """Check if a node is the primary for a key.

        Args:
            node_id: The node to check.
            key: The key to check.

        Returns:
            True if node_id is the primary node for key.
        """
        return self.get_node(key) == node_id

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
