"""Event sourcing and persistence for Casty actors.

This module provides:
- Write-Ahead Log (WAL) for durable event storage
- PersistentActor base class for stateful actors
- Replication configuration for distributed persistence
"""

from __future__ import annotations

import asyncio
import struct
import time
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator, Literal

from .actor import Actor, Context

if TYPE_CHECKING:
    from .actor import ActorId


class EventType(Enum):
    """Types of events in the event log."""

    MESSAGE_PROCESSED = auto()
    STATE_SNAPSHOT = auto()
    ACTOR_STARTED = auto()
    ACTOR_STOPPED = auto()


@dataclass(frozen=True, slots=True)
class Event:
    """An event in the event log."""

    event_type: EventType
    actor_id: "ActorId"
    sequence: int
    timestamp: float
    payload: bytes
    checksum: int = 0


@dataclass
class ReplicationConfig:
    """Configuration for state replication.

    Attributes:
        factor: Number of replicas (including primary). Default 3.
        sync_count: Number of replicas that must ACK before success.
                   0 = async (fire-and-forget, default)
                   1 = wait for 1 replica
                   N = wait for N replicas
                   -1 = wait for ALL replicas
        timeout: Timeout for synchronous replication (seconds)

    Examples:
        # Async replication (default)
        ReplicationConfig(factor=3)

        # Wait for 1 replica to ACK
        ReplicationConfig(factor=3, sync_count=1)

        # Wait for quorum (2 of 3)
        ReplicationConfig(factor=3, sync_count=2)

        # Wait for all replicas
        ReplicationConfig(factor=3, sync_count=-1)
    """

    factor: int = 3
    sync_count: int = 0  # 0=async, N=sync N, -1=sync all
    timeout: float = 5.0

    @property
    def is_sync(self) -> bool:
        """Check if this config requires synchronous replication."""
        return self.sync_count != 0

    def required_acks(self, num_replicas: int) -> int:
        """Calculate required ACKs based on sync_count.

        Args:
            num_replicas: Actual number of replicas available

        Returns:
            Number of ACKs required for success
        """
        if self.sync_count == 0:
            return 0  # Async
        elif self.sync_count == -1:
            return num_replicas  # All
        else:
            return min(self.sync_count, num_replicas)


@dataclass
class WALConfig:
    """Configuration for Write-Ahead Log.

    Attributes:
        base_dir: Base directory for WAL files
        max_segment_size: Maximum size of each log segment
        sync_mode: How to sync writes to disk
        snapshot_interval: Events between automatic snapshots
    """

    base_dir: Path = field(default_factory=lambda: Path("./data/wal"))
    max_segment_size: int = 64 * 1024 * 1024  # 64MB
    sync_mode: Literal["sync", "async", "batch"] = "async"
    snapshot_interval: int = 1000
    batch_size: int = 100
    batch_timeout: float = 0.01


# =============================================================================
# StoreBackend Abstraction (for actor-based WAL)
# =============================================================================


class StoreBackend(ABC):
    """Abstract backend for WAL storage."""

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the backend."""
        ...

    @abstractmethod
    async def append(self, data: bytes) -> None:
        """Append data to storage."""
        ...

    @abstractmethod
    def read_all(self) -> AsyncIterator[bytes]:
        """Read all entries from storage."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Close the backend."""
        ...


class FileStoreBackend(StoreBackend):
    """File-based storage backend (default).

    Uses segmented log files with automatic rotation.
    """

    def __init__(
        self,
        log_dir: Path,
        max_segment_size: int = 64 * 1024 * 1024,
    ):
        self._log_dir = log_dir
        self._max_segment_size = max_segment_size
        self._current_segment: Path | None = None
        self._file: Any | None = None
        self._segment_size = 0
        self._segment_index = 0

    async def initialize(self) -> None:
        """Create log directory and open first segment."""
        self._log_dir.mkdir(parents=True, exist_ok=True)

        # Find last segment index
        segments = sorted(self._log_dir.glob("*.wal"))
        if segments:
            last = segments[-1]
            self._segment_index = int(last.stem)

        await self._open_segment()

    async def append(self, data: bytes) -> None:
        """Append data to current segment."""
        # Rotate if segment too large
        if self._segment_size >= self._max_segment_size:
            await self._rotate_segment()

        # Write + flush
        await asyncio.to_thread(self._file.write, data)
        await asyncio.to_thread(self._file.flush)
        self._segment_size += len(data)

    async def read_all(self) -> AsyncIterator[bytes]:
        """Read all entries from all segments."""
        segments = sorted(self._log_dir.glob("*.wal"))

        for segment in segments:

            def read_sync() -> list[bytes]:
                entries = []
                with open(segment, "rb") as f:
                    while True:
                        # Read length prefix (4 bytes)
                        length_bytes = f.read(4)
                        if len(length_bytes) < 4:
                            break

                        entry_len = struct.unpack(">I", length_bytes)[0]
                        entry = f.read(entry_len)
                        if len(entry) < entry_len:
                            break

                        entries.append(entry)
                return entries

            entries = await asyncio.to_thread(read_sync)
            for entry in entries:
                yield entry

    async def _open_segment(self) -> None:
        """Open current segment for writing."""
        segment_name = f"{self._segment_index:016d}.wal"
        self._current_segment = self._log_dir / segment_name
        self._file = await asyncio.to_thread(open, self._current_segment, "ab")
        self._segment_size = (
            self._current_segment.stat().st_size
            if self._current_segment.exists()
            else 0
        )

    async def _rotate_segment(self) -> None:
        """Close current segment and open new one."""
        if self._file:
            await asyncio.to_thread(self._file.close)
        self._segment_index += 1
        await self._open_segment()

    async def close(self) -> None:
        """Close current segment."""
        if self._file:
            await asyncio.to_thread(self._file.flush)
            await asyncio.to_thread(self._file.close)
            self._file = None


# =============================================================================
# WriteAheadLog Actor Messages
# =============================================================================


@dataclass(frozen=True)
class Append:
    """Append event to WAL."""

    payload: bytes


@dataclass(frozen=True)
class Snapshot:
    """Take state snapshot."""

    state: bytes


@dataclass(frozen=True)
class Recover:
    """Recover from WAL. Returns (last_snapshot, events_after)."""

    pass


@dataclass(frozen=True)
class Close:
    """Close WAL and flush."""

    pass


# =============================================================================
# WriteAheadLog Actor
# =============================================================================


class WriteAheadLog(Actor[Append | Snapshot | Recover | Close]):
    """Actor that manages Write-Ahead Log for another actor.

    Responsibilities:
    - Append events via StoreBackend
    - Take snapshots periodically
    - Recover state on startup
    - Backend agnostic (file, memory, S3, etc.)
    """

    HEADER_FORMAT = ">BIQII"  # type, sequence, timestamp_ns, payload_len, checksum
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(
        self,
        actor_id: "ActorId",
        backend: StoreBackend | None = None,
        snapshot_interval: int = 1000,
    ):
        self._actor_id = actor_id
        self._snapshot_interval = snapshot_interval
        self._sequence = 0
        self._events_since_snapshot = 0
        self._last_snapshot_seq = 0

        # Default to FileStoreBackend if not provided
        if backend is None:
            actor_name = actor_id.name or actor_id.uid.hex[:8]
            log_dir = Path("./data/wal") / actor_name
            backend = FileStoreBackend(log_dir)

        self._backend = backend

    async def on_start(self) -> None:
        """Initialize WAL on actor start."""
        await self._backend.initialize()

    async def receive(
        self,
        msg: Append | Snapshot | Recover | Close,
        ctx: Context,
    ) -> None:
        match msg:
            case Append(payload):
                await self._append(payload)
                self._events_since_snapshot += 1

            case Snapshot(state):
                await self._snapshot(state)

            case Recover():
                result = await self._recover()
                ctx.reply(result)

            case Close():
                await self._close()

    async def _append(self, payload: bytes) -> None:
        """Append event to WAL."""
        self._sequence += 1

        # Pack event with header (length-prefix + header + payload)
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        header = struct.pack(
            self.HEADER_FORMAT,
            1,  # MESSAGE_PROCESSED
            self._sequence,
            int(time.time() * 1e9),
            len(payload),
            checksum,
        )

        entry = header + payload
        # Length-prefix for easy parsing
        data = struct.pack(">I", len(entry)) + entry

        await self._backend.append(data)

    async def _snapshot(self, state: bytes) -> None:
        """Take state snapshot."""
        self._sequence += 1

        checksum = zlib.crc32(state) & 0xFFFFFFFF
        header = struct.pack(
            self.HEADER_FORMAT,
            2,  # STATE_SNAPSHOT
            self._sequence,
            int(time.time() * 1e9),
            len(state),
            checksum,
        )

        entry = header + state
        data = struct.pack(">I", len(entry)) + entry

        await self._backend.append(data)

        self._last_snapshot_seq = self._sequence
        self._events_since_snapshot = 0

    async def _recover(self) -> tuple[bytes | None, list[bytes]]:
        """Recover from WAL. Returns (last_snapshot, events_after)."""
        last_snapshot: bytes | None = None
        events_after: list[bytes] = []

        async for entry in self._backend.read_all():
            # Parse header
            header_bytes = entry[: self.HEADER_SIZE]
            event_type, sequence, timestamp_ns, payload_len, checksum = struct.unpack(
                self.HEADER_FORMAT, header_bytes
            )

            payload = entry[self.HEADER_SIZE :]

            # Verify checksum
            if checksum != zlib.crc32(payload) & 0xFFFFFFFF:
                raise ValueError(f"Checksum mismatch at sequence {sequence}")

            # Update sequence
            self._sequence = max(self._sequence, sequence)

            # Handle event type
            if event_type == 2:  # STATE_SNAPSHOT
                last_snapshot = payload
                events_after.clear()
                self._last_snapshot_seq = sequence
            elif event_type == 1:  # MESSAGE_PROCESSED
                events_after.append(payload)

        return (last_snapshot, events_after)

    async def _close(self) -> None:
        """Close WAL."""
        await self._backend.close()

    async def on_stop(self) -> None:
        """Cleanup on actor stop."""
        await self._close()


class PersistentActor[M](Actor[M]):
    """Base class for actors requiring explicit state persistence control.

    Use this when you need to FORCE explicit get_state/set_state implementation.
    For most cases, the base Actor class's default implementation is sufficient:

        # Simple case - default serializes all public attributes automatically
        class Counter(Actor[Msg]):
            def __init__(self):
                self.count = 0  # Automatically serialized

        # Exclude specific fields
        class CachedActor(Actor[Msg]):
            __casty_exclude_fields__ = {"cache", "temp_data"}

    Use PersistentActor when you need explicit control over serialization:

        class Account(PersistentActor[AccountMsg]):
            def __init__(self):
                self.balance = 0
                self._cache = {}  # Not persisted (starts with _)

            def get_state(self) -> dict:
                return {"balance": self.balance}

            def set_state(self, state: dict) -> None:
                self.balance = state.get("balance", 0)
    """

    @abstractmethod
    def get_state(self) -> dict[str, Any]:
        """Get the current state to persist.

        Returns:
            Dictionary of state to serialize and persist
        """
        ...

    @abstractmethod
    def set_state(self, state: dict[str, Any]) -> None:
        """Restore state from persistence.

        Args:
            state: Previously persisted state dictionary
        """
        ...
