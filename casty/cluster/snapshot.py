from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from casty.serializable import serializable, serialize, deserialize
from .vector_clock import VectorClock


@serializable
@dataclass
class Snapshot:
    data: bytes
    clock: VectorClock


class SnapshotBackend(Protocol):
    async def save(self, snapshot: Snapshot) -> None: ...
    async def load_latest(self) -> Snapshot | None: ...
    async def find_base(self, clock_a: VectorClock, clock_b: VectorClock) -> Snapshot | None: ...
    async def prune(self, keep: int) -> None: ...


@dataclass
class InMemory:
    _history: list[Snapshot] = field(default_factory=list)

    async def save(self, snapshot: Snapshot) -> None:
        self._history.append(snapshot)

    async def load_latest(self) -> Snapshot | None:
        if not self._history:
            return None
        return self._history[-1]

    async def find_base(self, clock_a: VectorClock, clock_b: VectorClock) -> Snapshot | None:
        meet = clock_a.meet(clock_b)

        for snapshot in reversed(self._history):
            if snapshot.clock.is_before_or_equal(meet):
                return snapshot

        return None

    async def prune(self, keep: int) -> None:
        if len(self._history) > keep:
            self._history = self._history[-keep:]


@dataclass
class FileBackend:
    path: str
    _index: list[tuple[str, VectorClock]] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        Path(self.path).mkdir(parents=True, exist_ok=True)
        self._load_index()

    def _load_index(self) -> None:
        self._index = []
        path = Path(self.path)

        for file in sorted(path.glob("snapshot-*.msgpack")):
            try:
                data = file.read_bytes()
                snapshot = deserialize(data)
                self._index.append((file.name, snapshot.clock))
            except (OSError, TypeError, KeyError, ValueError):
                pass

    def _snapshot_path(self, filename: str) -> Path:
        return Path(self.path) / filename

    async def save(self, snapshot: Snapshot) -> None:
        filename = f"snapshot-{int(time.time() * 1000000)}.msgpack"
        filepath = self._snapshot_path(filename)

        data = serialize(snapshot)
        filepath.write_bytes(data)

        self._index.append((filename, snapshot.clock.copy()))

    async def load_latest(self) -> Snapshot | None:
        if not self._index:
            return None

        filename, _ = self._index[-1]
        filepath = self._snapshot_path(filename)

        if not filepath.exists():
            return None

        data = filepath.read_bytes()
        return deserialize(data)

    async def find_base(self, clock_a: VectorClock, clock_b: VectorClock) -> Snapshot | None:
        meet = clock_a.meet(clock_b)

        for filename, clock in reversed(self._index):
            if clock.is_before_or_equal(meet):
                filepath = self._snapshot_path(filename)
                if filepath.exists():
                    data = filepath.read_bytes()
                    return deserialize(data)

        return None

    async def prune(self, keep: int) -> None:
        if len(self._index) <= keep:
            return

        to_delete = self._index[:-keep]
        self._index = self._index[-keep:]

        for filename, _ in to_delete:
            filepath = self._snapshot_path(filename)
            if filepath.exists():
                filepath.unlink()
