import pytest
from casty.cluster.replication.messages import Replicate
from casty.serializable import serialize, deserialize


def test_replicate_simple_fields():
    msg = Replicate(
        actor_id="counter/main",
        version=1,
        snapshot=b"state",
    )
    assert msg.actor_id == "counter/main"
    assert msg.version == 1
    assert msg.snapshot == b"state"


def test_replicate_serialization():
    msg = Replicate(
        actor_id="test",
        version=1,
        snapshot=b"data",
    )
    data = serialize(msg)
    restored = deserialize(data)
    assert restored.actor_id == "test"
    assert restored.version == 1
    assert restored.snapshot == b"data"
