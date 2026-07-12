"""Token ring with vnodes. Pure: no I/O, no clock, immutable ring.

The hash is part of the wire protocol: stable across processes, platforms and
Python versions (blake2b/8, big-endian). Changing it is a cluster-wide breaking
change.
"""

from __future__ import annotations

import bisect
import hashlib
import struct
import uuid
from collections.abc import Iterable
from dataclasses import dataclass

_U32 = struct.Struct("!I")

TOKEN_SPACE = 1 << 64


def node_token(node_id: uuid.UUID, index: int) -> int:
    digest = hashlib.blake2b(node_id.bytes + _U32.pack(index), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def key_token(key: str) -> int:
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


@dataclass(frozen=True, slots=True)
class TokenRange:
    """Half-open circular interval (start, end] — the keys a vnode owns."""

    start: int
    end: int

    def __contains__(self, token: int) -> bool:
        if self.start == self.end:  # single-vnode ring: whole space
            return True
        if self.start < self.end:
            return self.start < token <= self.end
        return token > self.start or token <= self.end

    def size(self) -> int:
        if self.start == self.end:
            return TOKEN_SPACE
        return (self.end - self.start) % TOKEN_SPACE


class Ring:
    def __init__(self, entries: list[tuple[int, uuid.UUID]], nodes: frozenset[uuid.UUID]) -> None:
        self._entries = entries  # sorted by (token, node_id.bytes)
        self._tokens = [token for token, _ in entries]
        self._nodes = nodes

    @classmethod
    def build(cls, nodes: Iterable[uuid.UUID], *, vnodes: int = 128) -> Ring:
        node_set = frozenset(nodes)
        entries = [
            (node_token(node_id, i), node_id) for node_id in node_set for i in range(vnodes)
        ]
        entries.sort(key=lambda e: (e[0], e[1].bytes))
        return cls(entries, node_set)

    @property
    def nodes(self) -> frozenset[uuid.UUID]:
        return self._nodes

    def owner(self, key: str) -> uuid.UUID:
        return self.owner_of_token(key_token(key))

    def owner_of_token(self, token: int) -> uuid.UUID:
        if not self._entries:
            raise ValueError("empty ring")
        return self._entries[self._successor_index(token)][1]

    def replicas(self, key: str, n: int) -> tuple[uuid.UUID, ...]:
        return self.replicas_for_token(key_token(key), n)

    def replicas_for_token(self, token: int, n: int) -> tuple[uuid.UUID, ...]:
        """Owner first, then the next n-1 *distinct physical* nodes walking the
        ring (vnodes of already-collected nodes are skipped)."""
        if not self._entries:
            raise ValueError("empty ring")
        result: list[uuid.UUID] = []
        seen: set[uuid.UUID] = set()
        start = self._successor_index(token)
        for offset in range(len(self._entries)):
            node_id = self._entries[(start + offset) % len(self._entries)][1]
            if node_id in seen:
                continue
            seen.add(node_id)
            result.append(node_id)
            if len(result) >= n:
                break
        return tuple(result)

    def ranges_of(self, node_id: uuid.UUID) -> list[TokenRange]:
        if not self._entries:
            return []
        ranges: list[TokenRange] = []
        for i, (token, owner_id) in enumerate(self._entries):
            if owner_id != node_id:
                continue
            prev = self._entries[i - 1][0]  # i == 0 wraps to the last entry
            ranges.append(TokenRange(start=prev, end=token))
        return ranges

    def _successor_index(self, token: int) -> int:
        index = bisect.bisect_left(self._tokens, token)
        return index % len(self._entries)
