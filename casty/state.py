from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .serializable import serialize, deserialize

if TYPE_CHECKING:
    from .cluster.vector_clock import VectorClock
    from .cluster.snapshot import Snapshot


def _make_clock() -> "VectorClock":
    from .cluster.vector_clock import VectorClock
    return VectorClock()


@dataclass
class State[T]:
    value: T
    node_id: str = ""
    clock: "VectorClock" = field(default_factory=_make_clock, repr=False)
    _hash: bytes = field(default=b"", repr=False)
    _version: int = field(default=0, repr=False)

    @property
    def version(self) -> int:
        return self._version

    def set(self, new_value: T) -> None:
        self.value = new_value
        self._version += 1
        if self.node_id:
            self.clock.increment(self.node_id)

    def snapshot(self) -> "Snapshot":
        from .cluster.snapshot import Snapshot

        return Snapshot(
            data=serialize(self.value),
            clock=self.clock.copy()
        )

    def snapshot_bytes(self) -> bytes:
        return serialize(self.snapshot())

    def restore(self, snapshot: "Snapshot") -> None:
        self.value = deserialize(snapshot.data)
        self.clock = snapshot.clock.copy()

    def changed(self) -> bool:
        new_hash = hashlib.md5(serialize(self.value)).digest()
        changed = new_hash != self._hash
        self._hash = new_hash
        return changed
