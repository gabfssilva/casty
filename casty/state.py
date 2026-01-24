from __future__ import annotations

import hashlib
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .serializable import serialize, deserialize

if TYPE_CHECKING:
    from .cluster.snapshot import Snapshot


_current_stateful: ContextVar["Stateful | None"] = ContextVar("stateful", default=None)


class Stateful:
    def __init__(self):
        self._states: dict[str, "State"] = {}
        self._token: Any = None

    def __enter__(self):
        self._token = _current_stateful.set(self)
        return self

    def __exit__(self, *_):
        if self._token:
            _current_stateful.reset(self._token)

    def register(self, name: str, s: "State") -> None:
        self._states[name] = s

    def snapshot(self) -> dict[str, Any]:
        return {name: s.value for name, s in self._states.items()}

    def restore(self, data: dict[str, Any]) -> None:
        for name, value in data.items():
            if name in self._states:
                self._states[name].set(value)


def state[T](name: str, initial: T) -> "State[T]":
    s = State(value=initial)
    ctx = _current_stateful.get()
    if ctx:
        ctx.register(name, s)
    return s


@dataclass
class State[T]:
    value: T
    node_id: str = ""
    _hash: bytes = field(default=b"", repr=False)
    _version: int = field(default=0, repr=False)

    @property
    def version(self) -> int:
        return self._version

    def set(self, new_value: T) -> None:
        self.value = new_value
        self._version += 1

    def snapshot(self) -> "Snapshot":
        from .cluster.snapshot import Snapshot

        return Snapshot(
            data=serialize(self.value),
            version=self._version,
        )

    def snapshot_bytes(self) -> bytes:
        return serialize(self.snapshot())

    def restore(self, snapshot: "Snapshot") -> None:
        self.value = deserialize(snapshot.data)
        self._version = snapshot.version

    def changed(self) -> bool:
        new_hash = hashlib.md5(serialize(self.value)).digest()
        changed = new_hash != self._hash
        self._hash = new_hash
        return changed
