import pytest
from casty.state import State
from casty.context import Context
from casty.cluster.replication.filter import replication_filter
from casty.cluster.replication.replicator import Replicator
from casty.cluster.replication import ReplicationConfig


async def make_stream(messages):
    """Helper to create message stream"""
    for msg in messages:
        ctx = Context(self_id="test", sender=None, node_id="local")
        yield msg, ctx


@pytest.mark.asyncio
async def test_replication_filter_detects_change():
    state: State[int] = State(0)
    replicated_snapshots = []

    async def mock_send(node_id, msg):
        replicated_snapshots.append(msg.snapshot)

    replicator = Replicator(
        actor_id="test/actor",
        config=ReplicationConfig(factor=2, write_quorum=1),
        replica_nodes=["node-2"],
        send_fn=mock_send,
        node_id="node-1",
    )

    filter_fn = replication_filter(replicator)
    stream = filter_fn(state, make_stream(["a", "b", "c"]))

    messages = []
    async for msg, ctx in stream:
        messages.append(msg)
        if msg == "b":
            state.set(42)

    assert messages == ["a", "b", "c"]
    assert len(replicated_snapshots) == 1


@pytest.mark.asyncio
async def test_replication_filter_no_change_no_replicate():
    state: State[int] = State(0)
    replicated_snapshots = []

    async def mock_send(node_id, msg):
        replicated_snapshots.append(msg.snapshot)

    replicator = Replicator(
        actor_id="test/actor",
        config=ReplicationConfig(factor=2, write_quorum=1),
        replica_nodes=["node-2"],
        send_fn=mock_send,
        node_id="node-1",
    )

    filter_fn = replication_filter(replicator)
    stream = filter_fn(state, make_stream(["a", "b"]))

    async for msg, ctx in stream:
        pass

    assert replicated_snapshots == []


@pytest.mark.asyncio
async def test_replication_filter_no_state():
    """Filter should work even without state"""
    async def mock_send(node_id, msg):
        pass

    replicator = Replicator(
        actor_id="test/actor",
        config=ReplicationConfig(factor=2, write_quorum=1),
        replica_nodes=["node-2"],
        send_fn=mock_send,
        node_id="node-1",
    )

    filter_fn = replication_filter(replicator)
    stream = filter_fn(None, make_stream(["a", "b"]))

    messages = []
    async for msg, ctx in stream:
        messages.append(msg)

    assert messages == ["a", "b"]


@pytest.mark.asyncio
async def test_replication_filter_multiple_changes():
    state: State[int] = State(0)
    replicated_versions = []

    async def mock_send(node_id, msg):
        replicated_versions.append(msg.version)

    replicator = Replicator(
        actor_id="test/actor",
        config=ReplicationConfig(factor=2, write_quorum=1),
        replica_nodes=["node-2"],
        send_fn=mock_send,
        node_id="node-1",
    )

    filter_fn = replication_filter(replicator)
    stream = filter_fn(state, make_stream(["a", "b", "c"]))

    async for msg, ctx in stream:
        state.set(state.value + 1)

    assert len(replicated_versions) == 3
