from __future__ import annotations

import pytest
from unittest.mock import Mock, AsyncMock
from casty.state import Stateful, state
from casty.context import Context
from casty.cluster.replication.filter import replication_filter
from casty.cluster.states import StoreState, StoreAck, ReplicationQuorumError


async def make_stream(messages, stateful=None):
    for msg in messages:
        ctx = Mock()
        ctx.self_id = "test-actor"
        ctx._mailbox = Mock()
        ctx._mailbox._stateful = stateful
        yield msg, ctx


@pytest.mark.asyncio
async def test_replication_filter_sends_to_states():
    stored_states = []
    mock_ref = AsyncMock()
    mock_ref.ask = AsyncMock(return_value=StoreAck("test-actor"))

    async def capture_ask(msg):
        stored_states.append(msg)
        return StoreAck("test-actor")

    mock_ref.ask = capture_ask

    with Stateful() as stateful:
        counter = state("count", 0)

        filter_fn = replication_filter([mock_ref], write_quorum=1)
        stream = filter_fn(stateful, make_stream(["a", "b"], stateful))

        async for msg, ctx in stream:
            counter.set(counter.value + 1)

    assert len(stored_states) == 2
    assert all(isinstance(s, StoreState) for s in stored_states)
    assert stored_states[0].actor_id == "test-actor"


@pytest.mark.asyncio
async def test_replication_filter_no_states_refs():
    with Stateful() as stateful:
        counter = state("count", 0)

        filter_fn = replication_filter([], write_quorum=0)
        stream = filter_fn(stateful, make_stream(["a", "b"], stateful))

        messages = []
        async for msg, ctx in stream:
            messages.append(msg)
            counter.set(counter.value + 1)

        assert messages == ["a", "b"]


@pytest.mark.asyncio
async def test_replication_filter_quorum_not_met():
    async def fail_ask(msg):
        raise TimeoutError("Connection failed")

    mock_ref = Mock()
    mock_ref.ask = fail_ask

    with Stateful() as stateful:
        counter = state("count", 0)

        filter_fn = replication_filter([mock_ref], write_quorum=1)
        stream = filter_fn(stateful, make_stream(["a"], stateful))

        with pytest.raises(ReplicationQuorumError):
            async for msg, ctx in stream:
                counter.set(counter.value + 1)


@pytest.mark.asyncio
async def test_replication_filter_zero_quorum_doesnt_fail():
    async def fail_ask(msg):
        raise TimeoutError("Connection failed")

    mock_ref = Mock()
    mock_ref.ask = fail_ask

    with Stateful() as stateful:
        counter = state("count", 0)

        filter_fn = replication_filter([mock_ref], write_quorum=0)
        stream = filter_fn(stateful, make_stream(["a"], stateful))

        messages = []
        async for msg, ctx in stream:
            messages.append(msg)
            counter.set(counter.value + 1)

        assert messages == ["a"]


@pytest.mark.asyncio
async def test_replication_filter_quorum_partial_success():
    stored = []

    async def succeed_ask(msg):
        stored.append(msg)
        return StoreAck("test-actor")

    async def fail_ask(msg):
        raise TimeoutError("Connection failed")

    mock_ref_ok = Mock()
    mock_ref_ok.ask = succeed_ask

    mock_ref_fail = Mock()
    mock_ref_fail.ask = fail_ask

    with Stateful() as stateful:
        counter = state("count", 0)

        filter_fn = replication_filter([mock_ref_ok, mock_ref_fail], write_quorum=1)
        stream = filter_fn(stateful, make_stream(["a"], stateful))

        messages = []
        async for msg, ctx in stream:
            messages.append(msg)
            counter.set(counter.value + 1)

        assert messages == ["a"]
        assert len(stored) == 1


@pytest.mark.asyncio
async def test_replication_filter_no_stateful():
    filter_fn = replication_filter([], write_quorum=0)
    stream = filter_fn(None, make_stream(["a", "b"], None))

    messages = []
    async for msg, ctx in stream:
        messages.append(msg)

    assert messages == ["a", "b"]
