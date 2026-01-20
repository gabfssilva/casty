from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .serializable import serialize, deserialize


@dataclass
class State[T]:
    value: T
    version: int = 0
    _hash: bytes = b""

    def __init__(self, value: T) -> None:
        self.value = value
        self.version = 0
        self._hash = b""

    def set(self, new_value: T) -> None:
        self.value = new_value
        self.version += 1

    def snapshot(self) -> bytes:
        return serialize(self.value)

    def restore(self, data: bytes) -> None:
        self.value = deserialize(data)

    def changed(self) -> bool:
        new_hash = hashlib.md5(self.snapshot()).digest()
        changed = new_hash != self._hash
        self._hash = new_hash
        return changed
