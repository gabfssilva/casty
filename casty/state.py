from __future__ import annotations

import hashlib
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .serializable import serialize, deserialize, serializable
from .core import actor, Mailbox


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


@serializable
@dataclass
class Snapshot:
    data: bytes
    version: int = 0


@serializable
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

    def snapshot(self) -> Snapshot:
        return Snapshot(
            data=serialize(self.value),
            version=self._version,
        )

    def snapshot_bytes(self) -> bytes:
        return serialize(self.snapshot())

    def restore(self, snapshot: Snapshot) -> None:
        self.value = deserialize(snapshot.data)
        self._version = snapshot.version

    def changed(self) -> bool:
        new_hash = hashlib.md5(serialize(self.value)).digest()
        changed = new_hash != self._hash
        self._hash = new_hash
        return changed


class SnapshotBackend(Protocol):
    async def save(self, snapshot: Snapshot) -> None: ...
    async def load_latest(self) -> Snapshot | None: ...
    async def prune(self, keep: int) -> None: ...


@dataclass
class InMemoryBackend:
    _history: list[Snapshot] = field(default_factory=list)

    async def save(self, snapshot: Snapshot) -> None:
        self._history.append(snapshot)

    async def load_latest(self) -> Snapshot | None:
        if not self._history:
            return None
        return self._history[-1]

    async def prune(self, keep: int) -> None:
        if len(self._history) > keep:
            self._history = self._history[-keep:]


@dataclass
class FileBackend:
    path: str
    _index: list[tuple[str, int]] = field(default_factory=list, repr=False)

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
                self._index.append((file.name, snapshot.version))
            except (OSError, TypeError, KeyError, ValueError):
                pass

    def _snapshot_path(self, filename: str) -> Path:
        return Path(self.path) / filename

    async def save(self, snapshot: Snapshot) -> None:
        filename = f"snapshot-{int(time.time() * 1000000)}.msgpack"
        filepath = self._snapshot_path(filename)

        data = serialize(snapshot)
        filepath.write_bytes(data)

        self._index.append((filename, snapshot.version))

    async def load_latest(self) -> Snapshot | None:
        if not self._index:
            return None

        filename, _ = self._index[-1]
        filepath = self._snapshot_path(filename)

        if not filepath.exists():
            return None

        data = filepath.read_bytes()
        return deserialize(data)

    async def prune(self, keep: int) -> None:
        if len(self._index) <= keep:
            return

        to_delete = self._index[:-keep]
        self._index = self._index[-keep:]

        for filename, _ in to_delete:
            filepath = self._snapshot_path(filename)
            if filepath.exists():
                filepath.unlink()


class StateBackend(Protocol):
    async def store(self, actor_id: str, state: dict[str, Any]) -> None: ...
    async def get(self, actor_id: str) -> dict[str, Any] | None: ...
    async def delete(self, actor_id: str) -> None: ...


class MemoryStateBackend:
    def __init__(self):
        self._data: dict[str, dict[str, Any]] = {}

    async def store(self, actor_id: str, state: dict[str, Any]) -> None:
        self._data[actor_id] = state

    async def get(self, actor_id: str) -> dict[str, Any] | None:
        return self._data.get(actor_id)

    async def delete(self, actor_id: str) -> None:
        self._data.pop(actor_id, None)


from .messages import StoreState, StoreAck, GetState, DeleteState, StatesMessage


@actor
async def states(backend: StateBackend | None = None, *, mailbox: Mailbox[StatesMessage]):
    if backend is None:
        backend = MemoryStateBackend()

    async for msg, ctx in mailbox:
        match msg:
            case StoreState(actor_id, state):
                await backend.store(actor_id, state)
                await ctx.reply(StoreAck(actor_id))
            case GetState(actor_id):
                result = await backend.get(actor_id)
                await ctx.reply(result)
            case DeleteState(actor_id):
                await backend.delete(actor_id)


class ReplicationQuorumError(Exception):
    pass
