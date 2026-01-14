"""Event sourcing and persistence for Casty actors.

This module provides:
- Write-Ahead Log (WAL) for durable event storage
- PersistentActor base class for stateful actors
- Replication configuration for distributed persistence
"""

from __future__ import annotations

import asyncio
import os
import struct
import time
import zlib
from abc import abstractmethod
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


class WriteAheadLog:
    """Write-Ahead Log for durable event storage.

    Provides append-only storage with:
    - Segmented log files for efficient management
    - Configurable sync modes
    - CRC32 checksums for data integrity
    - Snapshot support for log compaction
    """

    HEADER_FORMAT = ">BIQII"  # type, sequence, timestamp_ns, payload_len, checksum
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(
        self,
        actor_id: "ActorId",
        config: WALConfig | None = None,
    ):
        self._actor_id = actor_id
        self._config = config or WALConfig()
        self._sequence = 0
        self._current_segment: Path | None = None
        self._segment_size = 0
        self._file: Any | None = None
        self._batch: list[Event] = []
        self._lock = asyncio.Lock()
        self._last_snapshot_seq = 0
        self._events_since_snapshot = 0

    @property
    def log_dir(self) -> Path:
        """Get log directory for this actor."""
        actor_name = self._actor_id.name or self._actor_id.uid.hex[:8]
        return self._config.base_dir / actor_name

    async def initialize(self) -> None:
        """Initialize the WAL, creating directories and loading state."""
        self.log_dir.mkdir(parents=True, exist_ok=True)
        await self._recover_sequence()
        await self._open_segment()

    async def _recover_sequence(self) -> None:
        """Recover sequence number from existing logs."""
        segments = sorted(self.log_dir.glob("*.wal"))
        if segments:
            async for event in self._read_segment(segments[-1]):
                self._sequence = max(self._sequence, event.sequence)

    async def _open_segment(self) -> None:
        """Open current segment for writing."""
        segment_name = f"{self._sequence:016d}.wal"
        self._current_segment = self.log_dir / segment_name
        self._file = await asyncio.to_thread(open, self._current_segment, "ab")
        self._segment_size = (
            self._current_segment.stat().st_size
            if self._current_segment.exists()
            else 0
        )

    async def append(self, event_type: EventType, payload: bytes) -> Event:
        """Append an event to the log."""
        async with self._lock:
            self._sequence += 1

            event = Event(
                event_type=event_type,
                actor_id=self._actor_id,
                sequence=self._sequence,
                timestamp=time.time(),
                payload=payload,
                checksum=self._compute_checksum(payload),
            )

            if self._segment_size >= self._config.max_segment_size:
                await self._rotate_segment()

            match self._config.sync_mode:
                case "sync":
                    await self._write_event(event, sync=True)
                case "async":
                    await self._write_event(event, sync=False)
                case "batch":
                    self._batch.append(event)
                    if len(self._batch) >= self._config.batch_size:
                        await self._flush_batch()

            self._events_since_snapshot += 1
            return event

    async def _write_event(self, event: Event, sync: bool = False) -> None:
        """Write a single event to disk."""
        header = struct.pack(
            self.HEADER_FORMAT,
            event.event_type.value,
            event.sequence,
            int(event.timestamp * 1e9),
            len(event.payload),
            event.checksum,
        )

        data = header + event.payload

        await asyncio.to_thread(self._file.write, data)
        if sync:
            await asyncio.to_thread(self._file.flush)
            await asyncio.to_thread(os.fsync, self._file.fileno())

        self._segment_size += len(data)

    async def _flush_batch(self) -> None:
        """Flush pending batch to disk."""
        if not self._batch:
            return

        for event in self._batch:
            await self._write_event(event, sync=False)

        await asyncio.to_thread(self._file.flush)
        await asyncio.to_thread(os.fsync, self._file.fileno())
        self._batch.clear()

    async def _rotate_segment(self) -> None:
        """Close current segment and open new one."""
        if self._file:
            await self._flush_batch()
            await asyncio.to_thread(self._file.close)
        await self._open_segment()

    async def read_from(self, from_sequence: int) -> AsyncIterator[Event]:
        """Read events starting from a sequence number."""
        segments = sorted(self.log_dir.glob("*.wal"))

        for segment in segments:
            async for event in self._read_segment(segment):
                if event.sequence >= from_sequence:
                    yield event

    async def _read_segment(self, path: Path) -> AsyncIterator[Event]:
        """Read all events from a segment file."""

        def read_sync() -> list[Event]:
            events = []
            with open(path, "rb") as f:
                while True:
                    header_bytes = f.read(self.HEADER_SIZE)
                    if len(header_bytes) < self.HEADER_SIZE:
                        break

                    event_type_val, sequence, timestamp_ns, payload_len, checksum = (
                        struct.unpack(self.HEADER_FORMAT, header_bytes)
                    )

                    payload = f.read(payload_len)
                    if len(payload) < payload_len:
                        break

                    if checksum != zlib.crc32(payload) & 0xFFFFFFFF:
                        raise ValueError(f"Checksum mismatch at sequence {sequence}")

                    events.append(
                        Event(
                            event_type=EventType(event_type_val),
                            actor_id=self._actor_id,
                            sequence=sequence,
                            timestamp=timestamp_ns / 1e9,
                            payload=payload,
                            checksum=checksum,
                        )
                    )
            return events

        events = await asyncio.to_thread(read_sync)
        for event in events:
            yield event

    def _compute_checksum(self, data: bytes) -> int:
        """Compute CRC32 checksum."""
        return zlib.crc32(data) & 0xFFFFFFFF

    async def take_snapshot(self, state: bytes) -> Event:
        """Take a state snapshot, enabling log compaction."""
        event = await self.append(EventType.STATE_SNAPSHOT, state)
        self._last_snapshot_seq = event.sequence
        self._events_since_snapshot = 0
        return event

    def should_snapshot(self) -> bool:
        """Check if a snapshot should be taken."""
        return self._events_since_snapshot >= self._config.snapshot_interval

    async def compact(self) -> None:
        """Remove segments before the last snapshot."""
        if self._last_snapshot_seq == 0:
            return

        segments = sorted(self.log_dir.glob("*.wal"))
        for segment in segments[:-1]:
            segment_start = int(segment.stem)
            if segment_start < self._last_snapshot_seq:
                await asyncio.to_thread(segment.unlink)

    async def recover(self) -> tuple[bytes | None, list[Event]]:
        """Recover state from the WAL.

        Returns:
            Tuple of (last_snapshot_bytes, events_after_snapshot)
        """
        last_snapshot: bytes | None = None
        events_after_snapshot: list[Event] = []

        async for event in self.read_from(0):
            if event.event_type == EventType.STATE_SNAPSHOT:
                last_snapshot = event.payload
                events_after_snapshot.clear()
            else:
                events_after_snapshot.append(event)

        return last_snapshot, events_after_snapshot

    async def close(self) -> None:
        """Close the WAL."""
        if self._file:
            await self._flush_batch()
            await asyncio.to_thread(self._file.close)
            self._file = None


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
