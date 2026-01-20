import pytest
import asyncio
from casty.cluster.replication.replicator import Replicator
from casty.cluster.replication import ReplicationConfig


@pytest.mark.asyncio
async def test_replicator_no_change_no_replicate():
    sent_messages = []

    async def mock_send(node_id, msg):
        sent_messages.append((node_id, msg))

    replicator = Replicator(
        actor_id="counter/main",
        config=ReplicationConfig(factor=3, write_quorum=2),
        replica_nodes=["node-2", "node-3"],
        send_fn=mock_send,
        node_id="node-1",
    )

    snapshot = b"same"
    await replicator.replicate(before=snapshot, after=snapshot, version=1)

    assert sent_messages == []


@pytest.mark.asyncio
async def test_replicator_sends_to_all_replicas():
    sent_messages = []

    async def mock_send(node_id, msg):
        sent_messages.append((node_id, msg))

    replicator = Replicator(
        actor_id="counter/main",
        config=ReplicationConfig(factor=3, write_quorum=1),
        replica_nodes=["node-2", "node-3"],
        send_fn=mock_send,
        node_id="node-1",
    )

    await replicator.replicate(before=b"old", after=b"new", version=1)

    assert len(sent_messages) == 2
    assert sent_messages[0][0] == "node-2"
    assert sent_messages[1][0] == "node-3"


@pytest.mark.asyncio
async def test_replicator_waits_for_quorum():
    async def mock_send(node_id, msg):
        pass

    replicator = Replicator(
        actor_id="counter/main",
        config=ReplicationConfig(factor=3, write_quorum=2),
        replica_nodes=["node-2", "node-3"],
        send_fn=mock_send,
        node_id="node-1",
    )

    async def send_ack():
        await asyncio.sleep(0.01)
        replicator.on_ack_received("counter/main", version=1, from_node="node-2")

    asyncio.create_task(send_ack())

    await replicator.replicate(before=b"old", after=b"new", version=1)


@pytest.mark.asyncio
async def test_replicator_ignores_wrong_actor_id():
    async def mock_send(node_id, msg):
        pass

    replicator = Replicator(
        actor_id="counter/main",
        config=ReplicationConfig(factor=3, write_quorum=2),
        replica_nodes=["node-2"],
        send_fn=mock_send,
        node_id="node-1",
        timeout=0.1,
    )

    async def send_wrong_ack():
        await asyncio.sleep(0.01)
        replicator.on_ack_received("other/actor", version=1, from_node="node-2")

    asyncio.create_task(send_wrong_ack())

    await replicator.replicate(before=b"old", after=b"new", version=1)


@pytest.mark.asyncio
async def test_replicator_no_replicas():
    sent_messages = []

    async def mock_send(node_id, msg):
        sent_messages.append((node_id, msg))

    replicator = Replicator(
        actor_id="counter/main",
        config=ReplicationConfig(factor=1, write_quorum=1),
        replica_nodes=[],
        send_fn=mock_send,
        node_id="node-1",
    )

    await replicator.replicate(before=b"old", after=b"new", version=1)

    assert sent_messages == []
