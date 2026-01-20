# tests/test_cluster_messages.py
import pytest


def test_membership_update():
    from casty.cluster.messages import MembershipUpdate
    from casty.serializable import serialize, deserialize

    update = MembershipUpdate(node_id="node-1", status="alive", incarnation=1)

    data = serialize(update)
    restored = deserialize(data)

    assert restored == update


def test_ping_with_updates():
    from casty.cluster.messages import Ping, MembershipUpdate
    from casty.serializable import serialize, deserialize

    ping = Ping(updates=[
        MembershipUpdate("node-1", "alive", 1),
        MembershipUpdate("node-2", "down", 2),
    ])

    data = serialize(ping)
    restored = deserialize(data)

    assert len(restored.updates) == 2
    assert restored.updates[0].node_id == "node-1"
