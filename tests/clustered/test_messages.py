import pytest

from casty.cluster import (
    serializable, deserialize,
    Node, Shard, All, Send,
    NodeJoined, NodeLeft, NodeFailed,
    GetMembers, GetNodeForKey,
)
from casty.cluster.messages import Ping, Ack, Handshake, HandshakeAck


class TestRouteMessages:
    def test_node_roundtrip(self):
        msg = Node(id="node-1")
        data = Node.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg

    def test_shard_roundtrip(self):
        msg = Shard(key="user:123")
        data = Shard.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg

    def test_all_roundtrip(self):
        msg = All()
        data = All.Codec.serialize(msg)
        result = deserialize(data)
        assert isinstance(result, All)


class TestClusterEvents:
    def test_node_joined(self):
        msg = NodeJoined(node_id="node-1", address=("127.0.0.1", 7946))
        data = NodeJoined.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg
        assert result.address == ("127.0.0.1", 7946)

    def test_node_left(self):
        msg = NodeLeft(node_id="node-1")
        data = NodeLeft.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg

    def test_node_failed(self):
        msg = NodeFailed(node_id="node-1")
        data = NodeFailed.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg


class TestSwimMessages:
    def test_ping(self):
        msg = Ping(sequence=42)
        data = Ping.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg

    def test_ack(self):
        msg = Ack(sequence=42)
        data = Ack.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg


class TestHandshake:
    def test_handshake(self):
        msg = Handshake(node_id="node-1", address=("192.168.1.1", 7946))
        data = Handshake.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg

    def test_handshake_ack(self):
        msg = HandshakeAck(node_id="node-2", address=("192.168.1.2", 7946))
        data = HandshakeAck.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg


class TestReplicationMessages:
    def test_replicate_state(self):
        from casty.cluster.messages import ReplicateState
        from casty.cluster.merge.version import VectorClock
        from casty.cluster.serializable import deserialize

        version = VectorClock({"node-1": 5})
        msg = ReplicateState(actor_id="user-123", version=version, state={"key": "value"})
        data = msg.Codec.serialize(msg)
        restored = deserialize(data)

        assert restored.actor_id == "user-123"
        assert restored.state == {"key": "value"}
        assert restored.version == version

    def test_replicate_ack(self):
        from casty.cluster.messages import ReplicateAck
        from casty.cluster.merge.version import VectorClock
        from casty.cluster.serializable import deserialize

        version = VectorClock({"node-1": 5})
        msg = ReplicateAck(actor_id="user-123", version=version, success=True)
        data = msg.Codec.serialize(msg)
        restored = deserialize(data)

        assert restored.actor_id == "user-123"
        assert restored.version == version
        assert restored.success is True

    def test_request_full_sync(self):
        from casty.cluster.messages import RequestFullSync
        from casty.cluster.serializable import deserialize

        msg = RequestFullSync(actor_id="user-123")
        data = msg.Codec.serialize(msg)
        restored = deserialize(data)

        assert restored.actor_id == "user-123"

    def test_full_sync_response(self):
        from casty.cluster.messages import FullSyncResponse
        from casty.cluster.merge.version import VectorClock
        from casty.cluster.serializable import deserialize

        version = VectorClock({"node-1": 3, "node-2": 5})
        msg = FullSyncResponse(actor_id="user-123", version=version, state={"count": 42})
        data = msg.Codec.serialize(msg)
        restored = deserialize(data)

        assert restored.actor_id == "user-123"
        assert restored.version == version
        assert restored.state == {"count": 42}

    def test_clustered_spawn(self):
        from casty.cluster.messages import ClusteredSpawn
        from casty.cluster.serializable import deserialize

        msg = ClusteredSpawn(
            actor_id="user-123",
            actor_cls_fqn="myapp.actors.UserActor",
            replication=3,
            singleton=False,
        )
        data = msg.Codec.serialize(msg)
        restored = deserialize(data)

        assert restored.actor_id == "user-123"
        assert restored.actor_cls_fqn == "myapp.actors.UserActor"
        assert restored.replication == 3
        assert restored.singleton is False

    def test_clustered_send(self):
        from casty.cluster.messages import ClusteredSend
        from casty.cluster.serializable import deserialize

        msg = ClusteredSend(
            actor_id="user-123",
            request_id="req-1",
            payload_type="myapp.messages.Increment",
            payload=b"msgpack",
            consistency=2,
        )
        data = msg.Codec.serialize(msg)
        restored = deserialize(data)

        assert restored.actor_id == "user-123"
        assert restored.consistency == 2
