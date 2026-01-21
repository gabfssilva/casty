# tests/test_cluster_messages.py
import pytest


def test_member_snapshot():
    from casty.cluster.messages import MemberSnapshot
    from casty.serializable import serialize, deserialize

    snapshot = MemberSnapshot(node_id="node-1", address="127.0.0.1:8001", state="alive", incarnation=1)

    data = serialize(snapshot)
    restored = deserialize(data)

    assert restored == snapshot


def test_ping_with_members():
    from casty.cluster.messages import Ping, MemberSnapshot
    from casty.serializable import serialize, deserialize

    ping = Ping(sender="node-0", members=[
        MemberSnapshot("node-1", "127.0.0.1:8001", "alive", 1),
        MemberSnapshot("node-2", "127.0.0.1:8002", "down", 2),
    ])

    data = serialize(ping)
    restored = deserialize(data)

    assert len(restored.members) == 2
    assert restored.members[0].node_id == "node-1"
