"""Raft state persistence mixin for durability."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..raft_types import LogEntry, RaftLog, Snapshot

if TYPE_CHECKING:
    from ..distributed import ClusterState

log = logging.getLogger(__name__)


class PersistenceMixin:
    """Persistence for Raft durable state: term, voted_for, log."""

    _cluster: ClusterState
    _persistence_dir: Path | None
    _persistence_lock: asyncio.Lock

    def _init_persistence(self, persistence_dir: str | None = None) -> None:
        """Initialize persistence directory."""
        if persistence_dir:
            self._persistence_dir = Path(persistence_dir)
            self._persistence_dir.mkdir(parents=True, exist_ok=True)
        else:
            self._persistence_dir = None
        self._persistence_lock = asyncio.Lock()

    def _get_persistence_path(self, kind: str) -> Path | None:
        """Get path to a persistence file of the given kind."""
        if not self._persistence_dir:
            return None
        return self._persistence_dir / f"raft_{kind}_{self._cluster.node_id[:8]}.json"

    def _get_state_file_path(self) -> Path | None:
        """Get path to state file."""
        return self._get_persistence_path("state")

    def _get_log_file_path(self) -> Path | None:
        """Get path to log file."""
        return self._get_persistence_path("log")

    def _get_snapshot_file_path(self) -> Path | None:
        """Get path to snapshot file."""
        return self._get_persistence_path("snapshot")

    async def _persist_state(self) -> None:
        """Persist current term and voted_for to disk with fsync."""
        state_path = self._get_state_file_path()
        if not state_path:
            return

        state_data = {
            "term": self._cluster.term,
            "voted_for": self._cluster.voted_for,
        }

        async with self._persistence_lock:
            await asyncio.to_thread(self._write_json_sync, state_path, state_data)

    async def _persist_log(self) -> None:
        """Persist log entries to disk with fsync."""
        log_path = self._get_log_file_path()
        if not log_path:
            return

        entries_data = [
            {"term": e.term, "index": e.index, "command": e.command}
            for e in self._cluster.raft_log.entries
        ]
        log_data = {
            "entries": entries_data,
            "commit_index": self._cluster.raft_log.commit_index,
            "last_applied": self._cluster.raft_log.last_applied,
        }

        async with self._persistence_lock:
            await asyncio.to_thread(self._write_json_sync, log_path, log_data)

    async def _persist_snapshot(self, snapshot: Snapshot) -> None:
        """Persist snapshot to disk with fsync."""
        snapshot_path = self._get_snapshot_file_path()
        if not snapshot_path:
            return

        snapshot_data = {
            "last_included_index": snapshot.last_included_index,
            "last_included_term": snapshot.last_included_term,
            "data": snapshot.data.hex(),  # Store bytes as hex string
        }

        async with self._persistence_lock:
            await asyncio.to_thread(self._write_json_sync, snapshot_path, snapshot_data)

    def _write_json_sync(self, path: Path, data: dict[str, Any]) -> None:
        """Synchronously write JSON with fsync for durability."""
        temp_path = path.with_suffix(".tmp")
        with open(temp_path, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        temp_path.rename(path)

    async def _load_persisted_state(self) -> bool:
        """Load persisted state from disk. Returns True if state was loaded."""
        state_path = self._get_state_file_path()
        if not state_path or not state_path.exists():
            return False

        try:
            state_data = await asyncio.to_thread(self._read_json_sync, state_path)
            self._cluster.term = state_data.get("term", 0)
            self._cluster.voted_for = state_data.get("voted_for")
            log.info(f"Loaded persisted state: term={self._cluster.term}")
            return True
        except Exception as e:
            log.warning(f"Failed to load persisted state: {e}")
            return False

    async def _load_persisted_log(self) -> bool:
        """Load persisted log from disk. Returns True if log was loaded."""
        log_path = self._get_log_file_path()
        if not log_path or not log_path.exists():
            return False

        try:
            log_data = await asyncio.to_thread(self._read_json_sync, log_path)
            entries = [
                LogEntry(term=e["term"], index=e["index"], command=e["command"])
                for e in log_data.get("entries", [])
            ]
            self._cluster.raft_log = RaftLog(
                entries=entries,
                commit_index=log_data.get("commit_index", 0),
                last_applied=log_data.get("last_applied", 0),
            )
            log.info(f"Loaded persisted log: {len(entries)} entries")
            return True
        except Exception as e:
            log.warning(f"Failed to load persisted log: {e}")
            return False

    async def _load_persisted_snapshot(self) -> Snapshot | None:
        """Load persisted snapshot from disk."""
        snapshot_path = self._get_snapshot_file_path()
        if not snapshot_path or not snapshot_path.exists():
            return None

        try:
            snapshot_data = await asyncio.to_thread(self._read_json_sync, snapshot_path)
            return Snapshot(
                last_included_index=snapshot_data["last_included_index"],
                last_included_term=snapshot_data["last_included_term"],
                data=bytes.fromhex(snapshot_data["data"]),
            )
        except Exception as e:
            log.warning(f"Failed to load persisted snapshot: {e}")
            return None

    def _read_json_sync(self, path: Path) -> dict[str, Any]:
        """Synchronously read JSON file."""
        with open(path) as f:
            return json.load(f)

    async def _clear_persistence(self) -> None:
        """Clear all persisted state (for testing)."""
        paths = [
            self._get_state_file_path(),
            self._get_log_file_path(),
            self._get_snapshot_file_path(),
        ]
        for path in paths:
            if path and path.exists():
                path.unlink()
