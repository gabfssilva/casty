from __future__ import annotations

from pathlib import Path
from typing import Protocol

from .entry import WALEntry


class WALBackend(Protocol):
    async def append(self, entry: WALEntry) -> None: ...
    async def read(self, actor_id: str) -> list[WALEntry]: ...
    async def snapshot(self, actor_id: str, data: bytes) -> None: ...
    async def get_snapshot(self, actor_id: str) -> bytes | None: ...


class InMemoryBackend:
    def __init__(self) -> None:
        self._entries: dict[str, list[WALEntry]] = {}
        self._snapshots: dict[str, bytes] = {}

    async def append(self, entry: WALEntry) -> None:
        if entry.actor_id not in self._entries:
            self._entries[entry.actor_id] = []
        self._entries[entry.actor_id].append(entry)

    async def read(self, actor_id: str) -> list[WALEntry]:
        return self._entries.get(actor_id, [])

    async def snapshot(self, actor_id: str, data: bytes) -> None:
        self._snapshots[actor_id] = data
        self._entries[actor_id] = []

    async def get_snapshot(self, actor_id: str) -> bytes | None:
        return self._snapshots.get(actor_id)


class FileBackend:
    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)

    def _actor_path(self, actor_id: str) -> Path:
        safe_id = actor_id.replace("/", "_")
        return self._path / f"{safe_id}.wal"

    def _snapshot_path(self, actor_id: str) -> Path:
        safe_id = actor_id.replace("/", "_")
        return self._path / f"{safe_id}.snapshot"

    async def append(self, entry: WALEntry) -> None:
        path = self._actor_path(entry.actor_id)
        with open(path, "ab") as f:
            data = entry.to_bytes()
            f.write(len(data).to_bytes(4, "big"))
            f.write(data)

    async def read(self, actor_id: str) -> list[WALEntry]:
        path = self._actor_path(actor_id)
        if not path.exists():
            return []

        entries = []
        with open(path, "rb") as f:
            while True:
                length_bytes = f.read(4)
                if not length_bytes:
                    break
                length = int.from_bytes(length_bytes, "big")
                data = f.read(length)
                entries.append(WALEntry.from_bytes(data))

        return entries

    async def snapshot(self, actor_id: str, data: bytes) -> None:
        path = self._snapshot_path(actor_id)
        with open(path, "wb") as f:
            f.write(data)
        wal_path = self._actor_path(actor_id)
        if wal_path.exists():
            wal_path.unlink()

    async def get_snapshot(self, actor_id: str) -> bytes | None:
        path = self._snapshot_path(actor_id)
        if not path.exists():
            return None
        with open(path, "rb") as f:
            return f.read()
