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
