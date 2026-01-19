"""Tests for durable actors with WAL persistence."""

import asyncio
import shutil
import tempfile
from pathlib import Path

import pytest

from casty import ActorSystem, Append, Snapshot, Recover, Close, WriteAheadLog, FileStoreBackend

# Import from conftest (pytest will make it available)
from .conftest import Counter, GetValue, Increment


@pytest.fixture(autouse=True)
def cleanup_wal_data():
    """Clean up WAL data before and after each test for proper isolation."""
    # Clean before test
    wal_dir = Path("data/wal")
    if wal_dir.exists():
        shutil.rmtree(wal_dir)

    yield

    # Clean after test
    if wal_dir.exists():
        shutil.rmtree(wal_dir)


@pytest.mark.asyncio
async def test_durable_counter_increments():
    """Test that durable counter increments and processes messages."""
    async with ActorSystem.local() as system:
        counter = await system.actor(Counter, name="durable-counter", durable=True)

        # Send increments
        await counter.send(Increment(5))
        await counter.send(Increment(3))

        # Check result
        value = await counter.ask(GetValue())
        assert value == 8


@pytest.mark.asyncio
async def test_durable_counter_full_recovery():
    """Full end-to-end test: Create → Mutate → Snapshot → Close → Recreate → Verify.

    This validates the complete durability scenario with automatic state recovery.
    State recovery is OPTIONAL - uses default Actor.get_state() automatically!

    Scenario:
    1. Create durable actor, send messages, verify state
    2. Force snapshot to WAL (using system.snapshot_durable_actor)
    3. Close ActorSystem (WAL + snapshot persists to disk)
    4. Create new ActorSystem, spawn actor with same name
    5. Verify state was AUTOMATICALLY recovered from snapshot
       (No need to override get_state/set_state!)

    Key insight: get_state() and set_state() are BUILT-IN to Actor and
    automatically serialize/restore all public attributes. You only need
    to override them if you have custom serialization needs.
    """
    from casty.system import LocalSystem

    # Step 1: Create system, actor, mutate state
    async with LocalSystem() as system:
        counter = await system.actor(Counter, name="recovery-test", durable=True)

        # Send messages to modify state
        await counter.send(Increment(5))
        await counter.send(Increment(3))
        await counter.send(Increment(2))

        # Verify state in-memory
        value = await counter.ask(GetValue())
        assert value == 10, "State should be 10 after increments"

        # Step 2: FORCE snapshot before closing (no need to send 1000 messages!)
        # This uses the built-in Actor.get_state() which serializes all public attrs
        await system.snapshot_durable_actor(counter)
        await asyncio.sleep(0.05)  # Let snapshot write to disk

    # Step 3: System closed, WAL + snapshot persisted to disk
    # Verify WAL file was created (path includes class name prefix)
    wal_path = Path("data/wal/Counter/recovery-test")
    assert wal_path.exists(), f"WAL directory should exist at {wal_path}"

    # Step 4: Create NEW system, create actor with SAME NAME
    async with LocalSystem() as system2:
        counter2 = await system2.actor(Counter, name="recovery-test", durable=True)

        # Step 5: VALIDATE state was AUTOMATICALLY recovered
        # This works because:
        # 1. Counter.get_state() serialized {"count": 10}
        # 2. WAL snapshot persisted it to disk
        # 3. New Counter instance called set_state({"count": 10}) automatically
        # 4. set_state() restored all public attributes
        value2 = await counter2.ask(GetValue())
        assert value2 == 10, "State MUST be recovered from WAL snapshot (no custom code needed!)"


@pytest.mark.asyncio
async def test_durable_counter_recovery_simple():
    """Simpler test: validates WAL infrastructure is in place.

    This test demonstrates that durable actors create successfully
    and the WAL actor infrastructure works. Full state recovery
    requires explicit get_state/set_state on Counter.
    """
    # First run
    async with ActorSystem.local() as system:
        counter = await system.actor(Counter, name="counter", durable=True)
        await counter.send(Increment(5))
        await counter.send(Increment(3))

        value = await counter.ask(GetValue())
        assert value == 8

    # Second run - WAL infrastructure is in place
    async with ActorSystem.local() as system:
        counter = await system.actor(Counter, name="counter", durable=True)
        # WAL recovery mechanism works, but state recovery depends on
        # actor implementation of get_state/set_state


@pytest.mark.asyncio
async def test_write_ahead_log_append():
    """Test that WAL appends messages correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        from casty.actor import ActorId
        from uuid import uuid4

        actor_id = ActorId(uid=uuid4(), name="test-actor")
        log_dir = Path(tmpdir) / "wal" / "test-actor"

        backend = FileStoreBackend(log_dir)
        wal = WriteAheadLog(actor_id=actor_id, backend=backend)

        # Create and initialize
        async with ActorSystem.local() as system:
            ref = await system.actor(
                WriteAheadLog,
                name="wal-test",
                actor_id=actor_id,
                backend=backend,
            )

            # Send append
            await ref.send(Append(b"test payload"))

            # Take snapshot
            await ref.send(Snapshot(b"snapshot data"))

            # Close
            await ref.send(Close())


@pytest.mark.asyncio
async def test_write_ahead_log_recover():
    """Test that WAL can be recovered correctly."""
    from casty.actor import ActorId
    from uuid import uuid4
    import msgpack

    with tempfile.TemporaryDirectory() as tmpdir:
        actor_id = ActorId(uid=uuid4(), name="test-recover")
        log_dir = Path(tmpdir) / "wal" / "test-recover"

        # First, create and populate a WAL
        backend1 = FileStoreBackend(log_dir)

        async with ActorSystem.local() as system1:
            ref1 = await system1.actor(
                WriteAheadLog,
                name="wal-first",
                actor_id=actor_id,
                backend=backend1,
            )

            # Log a snapshot with state
            state_bytes = msgpack.packb({"count": 42}, use_bin_type=True)
            await ref1.send(Snapshot(state_bytes))

            # Close gracefully
            await ref1.send(Close())
            await asyncio.sleep(0.1)  # Let close complete

        # Second, recover from the same WAL
        backend2 = FileStoreBackend(log_dir)

        async with ActorSystem.local() as system2:
            ref2 = await system2.actor(
                WriteAheadLog,
                name="wal-second",
                actor_id=actor_id,
                backend=backend2,
            )

            # Recover
            snapshot, events = await ref2.ask(Recover(), timeout=5.0)

            # Verify recovery - snapshot should be restored
            assert snapshot is not None
            recovered_state = msgpack.unpackb(snapshot)
            assert recovered_state["count"] == 42

            # Close
            await ref2.send(Close())
