import pytest
from casty.serializable import serialize, deserialize


def test_replicate_message():
    from casty.cluster.replication.messages import Replicate

    msg = Replicate(actor_id="counter/main", version=1, snapshot=b"data")

    assert msg.actor_id == "counter/main"
    assert msg.version == 1
    assert msg.snapshot == b"data"

    data = serialize(msg)
    restored = deserialize(data)
    assert restored.actor_id == msg.actor_id
    assert restored.version == msg.version
    assert restored.snapshot == msg.snapshot


def test_replicate_ack_message():
    from casty.cluster.replication.messages import ReplicateAck

    msg = ReplicateAck(actor_id="counter/main", version=1, node_id="node-1")

    data = serialize(msg)
    restored = deserialize(data)
    assert restored.actor_id == msg.actor_id
    assert restored.version == msg.version
    assert restored.node_id == msg.node_id


def test_sync_request_message():
    from casty.cluster.replication.messages import SyncRequest

    msg = SyncRequest(actor_id="counter/main")

    data = serialize(msg)
    restored = deserialize(data)
    assert restored.actor_id == msg.actor_id


def test_sync_response_message():
    from casty.cluster.replication.messages import SyncResponse

    msg = SyncResponse(actor_id="counter/main", version=5, snapshot=b"state")

    data = serialize(msg)
    restored = deserialize(data)
    assert restored.actor_id == msg.actor_id
    assert restored.version == msg.version
    assert restored.snapshot == msg.snapshot
