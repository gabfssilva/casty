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


def test_state_set_increments_version():
    from casty.state import State

    state: State[int] = State(0)
    assert state.version == 0

    state.set(1)
    assert state.version == 1

    state.set(2)
    assert state.version == 2


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

    state: State[int] = State(42)
    snapshot = state.snapshot()

    assert isinstance(snapshot, bytes)
    assert len(snapshot) > 0


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
