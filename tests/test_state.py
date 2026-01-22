# tests/test_state.py
import pytest


def test_state_initial_value():
    from casty.state import State

    state: State[int] = State(42)
    assert state.value == 42


def test_state_set_updates_value():
    from casty.state import State

    state: State[int] = State(0)
    state.set(10)
    assert state.value == 10


def test_state_set_increments_clock():
    from casty.state import State

    state: State[int] = State(0, node_id="test-node")
    assert state.clock.versions == {}

    state.set(1)
    assert state.clock.versions == {"test-node": 1}

    state.set(2)
    assert state.clock.versions == {"test-node": 2}


def test_state_changed_detects_mutation():
    from casty.state import State

    state: State[int] = State(0)

    # First call initializes hash, returns True
    assert state.changed() is True

    # No change, returns False
    assert state.changed() is False

    # After mutation, returns True
    state.set(1)
    assert state.changed() is True

    # No change again
    assert state.changed() is False


def test_state_snapshot_serializes_value():
    from casty.state import State
    from casty.cluster.snapshot import Snapshot

    state: State[int] = State(42)
    snapshot = state.snapshot()

    assert isinstance(snapshot, Snapshot)
    assert isinstance(snapshot.data, bytes)
    assert len(snapshot.data) > 0


def test_state_with_dataclass():
    from casty.state import State
    from casty.serializable import serializable
    from dataclasses import dataclass

    @serializable
    @dataclass
    class Counter:
        count: int
        name: str

    state: State[Counter] = State(Counter(count=0, name="test"))
    assert state.value.count == 0
    assert state.value.name == "test"

    state.set(Counter(count=5, name="updated"))
    assert state.value.count == 5
    assert state.changed() is True


class TestStateWithVectorClock:
    def test_state_has_clock(self):
        from casty.state import State
        from casty.cluster.vector_clock import VectorClock

        state = State(value={"count": 0}, node_id="node-1")
        assert isinstance(state.clock, VectorClock)
        assert state.clock.versions == {}

    def test_set_increments_clock(self):
        from casty.state import State

        state = State(value={"count": 0}, node_id="node-1")
        state.set({"count": 1})
        assert state.clock.versions == {"node-1": 1}

    def test_set_increments_clock_multiple_times(self):
        from casty.state import State

        state = State(value=0, node_id="node-1")
        state.set(1)
        state.set(2)
        state.set(3)
        assert state.clock.versions == {"node-1": 3}

    def test_snapshot_returns_snapshot_with_clock(self):
        from casty.state import State
        from casty.cluster.snapshot import Snapshot

        state = State(value={"count": 5}, node_id="node-1")
        state.set({"count": 5})

        snapshot = state.snapshot()

        assert isinstance(snapshot, Snapshot)
        assert snapshot.clock.versions == {"node-1": 1}

    def test_restore_from_snapshot(self):
        from casty.state import State
        from casty.cluster.vector_clock import VectorClock
        from casty.cluster.snapshot import Snapshot

        state = State(value={"count": 0}, node_id="node-1")

        snapshot = Snapshot(
            data=b'\x81\xa5count\x0a',
            clock=VectorClock({"node-2": 5})
        )

        state.restore(snapshot)

        assert state.value == {"count": 10}
        assert state.clock.versions == {"node-2": 5}

    def test_state_without_node_id_does_not_increment_clock(self):
        from casty.state import State

        state = State(value=0)
        state.set(1)
        assert state.clock.versions == {}
