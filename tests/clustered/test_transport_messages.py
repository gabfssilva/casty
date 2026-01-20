import pytest

from casty.cluster.transport_messages import (
    Transmit, Deliver, Register, Connect, GetConnection, Disconnect, Received, NewConnection
)
from casty.serializable import serialize, deserialize


def test_transmit_serializable():
    msg = Transmit(data=b"hello")
    assert deserialize(serialize(msg)) == msg


def test_deliver_is_internal_message():
    msg = Deliver(data=b"world", reply_connection=None)
    assert msg.data == b"world"
    assert msg.reply_connection is None


def test_connect_serializable():
    msg = Connect(node_id="node-2", address="localhost:8002")
    assert deserialize(serialize(msg)) == msg


def test_get_connection_serializable():
    msg = GetConnection(node_id="node-2")
    assert deserialize(serialize(msg)) == msg


def test_disconnect_serializable():
    msg = Disconnect()
    assert deserialize(serialize(msg)) == msg


def test_spawn_replica_serializable():
    from casty.cluster.transport_messages import SpawnReplica
    from casty.cluster.replication import ReplicationConfig, Routing

    msg = SpawnReplica(
        actor_id="counter/main",
        behavior_name="casty.actors.counter",
        initial_args=(),
        initial_kwargs={"initial": 0},
        config=ReplicationConfig(factor=3, write_quorum=2, routing=Routing.LEADER),
        is_leader=False,
    )

    data = serialize(msg)
    restored = deserialize(data)

    assert restored.actor_id == "counter/main"
    assert restored.behavior_name == "casty.actors.counter"
    assert restored.initial_args == []  # tuple becomes list after serialization
    assert restored.initial_kwargs == {"initial": 0}
    assert restored.config.factor == 3
    assert restored.is_leader is False


def test_promote_to_leader_serializable():
    from casty.cluster.transport_messages import PromoteToLeader

    msg = PromoteToLeader(actor_id="counter/main")

    data = serialize(msg)
    restored = deserialize(data)

    assert restored.actor_id == "counter/main"
