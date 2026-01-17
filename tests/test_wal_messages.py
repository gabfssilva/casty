from casty.wal.messages import Append, Recover, GetCurrentVersion, FindBase, Merge
from casty.wal.version import VectorClock


def test_append_message():
    msg = Append(delta={"count": 10})
    assert msg.delta == {"count": 10}


def test_find_base_message():
    version = VectorClock({"node-a": 1})
    msg = FindBase(their_version=version)
    assert msg.their_version == version


def test_merge_message():
    version = VectorClock({"node-b": 2})
    state = {"count": 5}
    msg = Merge(
        their_version=version,
        their_state=state,
        my_state={"count": 3},
        actor=None,
    )
    assert msg.their_version == version
    assert msg.their_state == state
