"""Tests for persistence and event sourcing."""

import asyncio
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from casty import Actor, Context
from casty.actor import ActorId
from casty.persistence import (
    Event,
    EventType,
    PersistentActor,
    ReplicationConfig,
    WALConfig,
    WriteAheadLog,
)


@dataclass
class GetState:
    """Message to get state."""
    pass


@dataclass
class SetValue:
    """Message to set value."""
    value: int


@dataclass
class Increment:
    """Message to increment value."""
    amount: int = 1


class TestEventType:
    """Tests for EventType enum."""

    def test_event_types_exist(self):
        """Test that all event types are defined."""
        assert EventType.MESSAGE_PROCESSED is not None
        assert EventType.STATE_SNAPSHOT is not None
        assert EventType.ACTOR_STARTED is not None
        assert EventType.ACTOR_STOPPED is not None

    def test_event_types_unique(self):
        """Test that event types have unique values."""
        values = [e.value for e in EventType]
        assert len(values) == len(set(values))


class TestEvent:
    """Tests for Event dataclass."""

    def test_create_event(self):
        """Test creating an event."""
        from uuid import uuid4

        actor_id = ActorId(uid=uuid4(), name="test-actor")
        event = Event(
            event_type=EventType.MESSAGE_PROCESSED,
            actor_id=actor_id,
            sequence=1,
            timestamp=1234567890.123,
            payload=b"test payload",
            checksum=12345,
        )

        assert event.event_type == EventType.MESSAGE_PROCESSED
        assert event.actor_id == actor_id
        assert event.sequence == 1
        assert event.payload == b"test payload"

    def test_event_is_frozen(self):
        """Test that Event is immutable."""
        from uuid import uuid4

        actor_id = ActorId(uid=uuid4())
        event = Event(
            event_type=EventType.MESSAGE_PROCESSED,
            actor_id=actor_id,
            sequence=1,
            timestamp=1234567890.123,
            payload=b"test",
        )

        with pytest.raises((AttributeError, TypeError)):
            event.sequence = 2


class TestReplicationConfig:
    """Tests for ReplicationConfig."""

    def test_default_config(self):
        """Test default replication config."""
        config = ReplicationConfig()
        assert config.sync_count == 0  # Async by default
        assert config.factor == 3
        assert config.timeout == 5.0
        assert config.is_sync is False

    def test_custom_config(self):
        """Test custom replication config."""
        config = ReplicationConfig(
            sync_count=2,  # Wait for 2 replicas
            factor=5,
            timeout=10.0,
        )
        assert config.sync_count == 2
        assert config.factor == 5
        assert config.timeout == 10.0
        assert config.is_sync is True

    def test_required_acks_async(self):
        """Test required_acks for async mode."""
        config = ReplicationConfig(sync_count=0, factor=3)
        assert config.required_acks(2) == 0  # No ACKs needed for async

    def test_required_acks_sync_n(self):
        """Test required_acks for sync N mode."""
        config = ReplicationConfig(sync_count=2, factor=3)
        assert config.required_acks(3) == 2  # Wait for 2 of 3
        assert config.required_acks(1) == 1  # Min of sync_count and available

    def test_required_acks_sync_all(self):
        """Test required_acks for sync all mode."""
        config = ReplicationConfig(sync_count=-1, factor=5)
        assert config.required_acks(4) == 4  # Wait for all 4 replicas


class TestWALConfig:
    """Tests for WALConfig."""

    def test_default_config(self):
        """Test default WAL config."""
        config = WALConfig()
        assert config.base_dir == Path("./data/wal")
        assert config.max_segment_size == 64 * 1024 * 1024
        assert config.sync_mode == "async"
        assert config.snapshot_interval == 1000

    def test_custom_config(self):
        """Test custom WAL config."""
        config = WALConfig(
            base_dir=Path("/tmp/custom/wal"),
            max_segment_size=1024 * 1024,
            sync_mode="sync",
            snapshot_interval=100,
        )
        assert config.base_dir == Path("/tmp/custom/wal")
        assert config.max_segment_size == 1024 * 1024
        assert config.sync_mode == "sync"
        assert config.snapshot_interval == 100


class TestWriteAheadLog:
    """Tests for WriteAheadLog."""

    @pytest.fixture
    def wal_dir(self, tmp_path: Path):
        """Create a temporary WAL directory."""
        return tmp_path / "wal"

    @pytest.fixture
    def actor_id(self):
        """Create a test actor ID."""
        from uuid import uuid4
        return ActorId(uid=uuid4(), name="test-actor")

    @pytest.mark.asyncio
    async def test_wal_initialization(self, wal_dir: Path, actor_id: ActorId):
        """Test WAL initialization creates directories."""
        config = WALConfig(base_dir=wal_dir)
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()

        assert wal.log_dir.exists()
        await wal.close()

    @pytest.mark.asyncio
    async def test_wal_append_event(self, wal_dir: Path, actor_id: ActorId):
        """Test appending an event to WAL."""
        config = WALConfig(base_dir=wal_dir, sync_mode="sync")
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()

        event = await wal.append(EventType.MESSAGE_PROCESSED, b"test data")

        assert event.sequence == 1
        assert event.event_type == EventType.MESSAGE_PROCESSED
        assert event.payload == b"test data"
        assert event.checksum != 0

        await wal.close()

    @pytest.mark.asyncio
    async def test_wal_append_multiple_events(self, wal_dir: Path, actor_id: ActorId):
        """Test appending multiple events."""
        config = WALConfig(base_dir=wal_dir, sync_mode="sync")
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()

        events = []
        for i in range(5):
            event = await wal.append(
                EventType.MESSAGE_PROCESSED,
                f"event-{i}".encode(),
            )
            events.append(event)

        assert len(events) == 5
        assert events[0].sequence == 1
        assert events[4].sequence == 5

        await wal.close()

    @pytest.mark.asyncio
    async def test_wal_read_events(self, wal_dir: Path, actor_id: ActorId):
        """Test reading events from WAL."""
        config = WALConfig(base_dir=wal_dir, sync_mode="sync")
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()

        # Write events
        for i in range(3):
            await wal.append(EventType.MESSAGE_PROCESSED, f"event-{i}".encode())

        await wal.close()

        # Read events back
        wal2 = WriteAheadLog(actor_id, config)
        await wal2.initialize()

        events = []
        async for event in wal2.read_from(1):
            events.append(event)

        assert len(events) == 3
        assert events[0].payload == b"event-0"
        assert events[2].payload == b"event-2"

        await wal2.close()

    @pytest.mark.asyncio
    async def test_wal_snapshot(self, wal_dir: Path, actor_id: ActorId):
        """Test taking a snapshot."""
        config = WALConfig(base_dir=wal_dir, sync_mode="sync")
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()

        # Write some events
        await wal.append(EventType.MESSAGE_PROCESSED, b"event-1")
        await wal.append(EventType.MESSAGE_PROCESSED, b"event-2")

        # Take snapshot
        snapshot = await wal.take_snapshot(b'{"state": "snapshot"}')

        assert snapshot.event_type == EventType.STATE_SNAPSHOT
        assert snapshot.payload == b'{"state": "snapshot"}'

        await wal.close()

    @pytest.mark.asyncio
    async def test_wal_recover(self, wal_dir: Path, actor_id: ActorId):
        """Test recovering from WAL."""
        config = WALConfig(base_dir=wal_dir, sync_mode="sync")
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()

        # Write events
        await wal.append(EventType.MESSAGE_PROCESSED, b"event-1")
        await wal.append(EventType.MESSAGE_PROCESSED, b"event-2")
        await wal.take_snapshot(b'{"value": 42}')
        await wal.append(EventType.MESSAGE_PROCESSED, b"event-3")

        await wal.close()

        # Recover
        wal2 = WriteAheadLog(actor_id, config)
        await wal2.initialize()

        snapshot, events = await wal2.recover()

        assert snapshot == b'{"value": 42}'
        assert len(events) == 1  # Only event after snapshot
        assert events[0].payload == b"event-3"

        await wal2.close()

    @pytest.mark.asyncio
    async def test_wal_should_snapshot(self, wal_dir: Path, actor_id: ActorId):
        """Test snapshot interval tracking."""
        config = WALConfig(base_dir=wal_dir, sync_mode="sync", snapshot_interval=3)
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()

        assert wal.should_snapshot() is False

        await wal.append(EventType.MESSAGE_PROCESSED, b"1")
        assert wal.should_snapshot() is False

        await wal.append(EventType.MESSAGE_PROCESSED, b"2")
        assert wal.should_snapshot() is False

        await wal.append(EventType.MESSAGE_PROCESSED, b"3")
        assert wal.should_snapshot() is True

        await wal.take_snapshot(b"snapshot")
        assert wal.should_snapshot() is False

        await wal.close()

    @pytest.mark.asyncio
    async def test_wal_checksum_integrity(self, wal_dir: Path, actor_id: ActorId):
        """Test that checksum validates data integrity."""
        import zlib

        config = WALConfig(base_dir=wal_dir, sync_mode="sync")
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()

        data = b"important data"
        event = await wal.append(EventType.MESSAGE_PROCESSED, data)

        expected_checksum = zlib.crc32(data) & 0xFFFFFFFF
        assert event.checksum == expected_checksum

        await wal.close()


class TestPersistentActor:
    """Tests for PersistentActor base class."""

    def test_persistent_actor_is_abstract(self):
        """Test that PersistentActor requires implementation."""
        with pytest.raises(TypeError):
            # Can't instantiate abstract class
            PersistentActor()

    def test_persistent_actor_subclass(self):
        """Test implementing a PersistentActor subclass."""

        class Counter(PersistentActor[Increment | GetState]):
            def __init__(self):
                self.value = 0

            def get_state(self) -> dict[str, Any]:
                return {"value": self.value}

            def set_state(self, state: dict[str, Any]) -> None:
                self.value = state.get("value", 0)

            async def receive(
                self, msg: Increment | GetState, ctx: Context
            ) -> None:
                match msg:
                    case GetState():
                        ctx.reply(self.value)
                    case Increment(amount):
                        self.value += amount

        actor = Counter()
        assert actor.get_state() == {"value": 0}

        actor.value = 42
        assert actor.get_state() == {"value": 42}

        actor.set_state({"value": 100})
        assert actor.value == 100


class TestSyncCountSemantics:
    """Tests for understanding sync_count semantics."""

    def test_sync_count_zero_means_async(self):
        """Test sync_count=0 means async replication (fire-and-forget)."""
        config = ReplicationConfig(sync_count=0, factor=3)
        assert config.is_sync is False
        assert config.required_acks(2) == 0

    def test_sync_count_positive_means_wait_for_n(self):
        """Test sync_count=N waits for N replicas."""
        config = ReplicationConfig(sync_count=2, factor=5)
        assert config.is_sync is True
        assert config.required_acks(4) == 2  # Wait for 2 of 4

    def test_sync_count_exceeds_available(self):
        """Test sync_count larger than available replicas."""
        config = ReplicationConfig(sync_count=5, factor=3)
        # Only 2 replicas available, so required_acks caps at 2
        assert config.required_acks(2) == 2

    def test_sync_count_negative_one_means_all(self):
        """Test sync_count=-1 waits for all replicas."""
        config = ReplicationConfig(sync_count=-1, factor=5)
        assert config.is_sync is True
        assert config.required_acks(4) == 4  # Wait for all 4

    def test_is_sync_property(self):
        """Test is_sync property correctly identifies sync/async."""
        async_config = ReplicationConfig(sync_count=0)
        sync_one_config = ReplicationConfig(sync_count=1)
        sync_all_config = ReplicationConfig(sync_count=-1)

        assert async_config.is_sync is False
        assert sync_one_config.is_sync is True
        assert sync_all_config.is_sync is True


class TestWALSyncModes:
    """Tests for WAL sync modes."""

    @pytest.fixture
    def wal_dir(self, tmp_path: Path):
        """Create a temporary WAL directory."""
        return tmp_path / "wal"

    @pytest.fixture
    def actor_id(self):
        """Create a test actor ID."""
        from uuid import uuid4
        return ActorId(uid=uuid4(), name="sync-test")

    @pytest.mark.asyncio
    async def test_sync_mode_sync(self, wal_dir: Path, actor_id: ActorId):
        """Test sync mode writes immediately."""
        config = WALConfig(base_dir=wal_dir, sync_mode="sync")
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()
        await wal.append(EventType.MESSAGE_PROCESSED, b"sync-data")

        # File should exist immediately
        assert list(wal.log_dir.glob("*.wal"))

        await wal.close()

    @pytest.mark.asyncio
    async def test_sync_mode_async(self, wal_dir: Path, actor_id: ActorId):
        """Test async mode writes without fsync."""
        config = WALConfig(base_dir=wal_dir, sync_mode="async")
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()
        await wal.append(EventType.MESSAGE_PROCESSED, b"async-data")

        await wal.close()

    @pytest.mark.asyncio
    async def test_sync_mode_batch(self, wal_dir: Path, actor_id: ActorId):
        """Test batch mode batches writes."""
        config = WALConfig(
            base_dir=wal_dir,
            sync_mode="batch",
            batch_size=3,
        )
        wal = WriteAheadLog(actor_id, config)

        await wal.initialize()

        # Write events (should batch)
        await wal.append(EventType.MESSAGE_PROCESSED, b"1")
        await wal.append(EventType.MESSAGE_PROCESSED, b"2")
        await wal.append(EventType.MESSAGE_PROCESSED, b"3")  # Should flush

        await wal.close()

        # Read back to verify all were written
        wal2 = WriteAheadLog(actor_id, config)
        await wal2.initialize()

        events = []
        async for event in wal2.read_from(1):
            events.append(event)

        assert len(events) == 3
        await wal2.close()
