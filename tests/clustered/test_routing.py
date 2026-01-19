import pytest
from unittest.mock import AsyncMock, MagicMock
from dataclasses import dataclass


@dataclass
class SampleMsg:
    value: int


class TestRouting:
    def test_routing_type_exists(self):
        from casty.cluster import Routing

        assert Routing is not None

    def test_no_local_replica_error_exists(self):
        from casty.cluster import NoLocalReplicaError

        with pytest.raises(NoLocalReplicaError):
            raise NoLocalReplicaError("test")


class TestClusteredMessages:
    def test_clustered_send_has_routing_field(self):
        from casty.cluster.messages import ClusteredSend

        msg = ClusteredSend(
            actor_id="test",
            request_id="req-1",
            payload_type="test.Msg",
            payload=b"data",
            routing="leader",
        )
        assert msg.routing == "leader"

    def test_clustered_ask_has_routing_field(self):
        from casty.cluster.messages import ClusteredAsk

        msg = ClusteredAsk(
            actor_id="test",
            request_id="req-1",
            payload_type="test.Msg",
            payload=b"data",
            routing="local",
        )
        assert msg.routing == "local"


class TestClusteredActorRefRouting:
    def test_clustered_ref_no_write_consistency_field(self):
        from casty.cluster.clustered_ref import ClusteredActorRef

        cluster = MagicMock()
        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )
        assert not hasattr(ref, 'write_consistency')

    @pytest.mark.asyncio
    async def test_send_default_routing_is_leader(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from casty.cluster.messages import ClusteredSend

        cluster = MagicMock()
        cluster.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        await ref.send(SampleMsg(42))

        call_args = cluster.send.call_args
        sent_msg = call_args[0][0]
        assert isinstance(sent_msg, ClusteredSend)
        assert sent_msg.routing == "leader"

    @pytest.mark.asyncio
    async def test_send_routing_local_uses_local_ref(self):
        from casty.cluster.clustered_ref import ClusteredActorRef

        cluster = MagicMock()
        cluster.send = AsyncMock()

        local_ref = MagicMock()
        local_ref.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=local_ref,
        )

        await ref.send(SampleMsg(42), routing='local')

        local_ref.send.assert_called_once()
        cluster.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_routing_local_raises_without_local_ref(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from casty.cluster import NoLocalReplicaError

        cluster = MagicMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        with pytest.raises(NoLocalReplicaError):
            await ref.send(SampleMsg(42), routing='local')

    @pytest.mark.asyncio
    async def test_send_routing_fastest_uses_local_if_available(self):
        from casty.cluster.clustered_ref import ClusteredActorRef

        cluster = MagicMock()
        cluster.send = AsyncMock()

        local_ref = MagicMock()
        local_ref.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=local_ref,
        )

        await ref.send(SampleMsg(42), routing='fastest')

        local_ref.send.assert_called_once()
        cluster.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_routing_fastest_falls_back_to_leader(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from casty.cluster.messages import ClusteredSend

        cluster = MagicMock()
        cluster.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        await ref.send(SampleMsg(42), routing='fastest')

        call_args = cluster.send.call_args
        sent_msg = call_args[0][0]
        assert isinstance(sent_msg, ClusteredSend)
        assert sent_msg.routing == "leader"

    @pytest.mark.asyncio
    async def test_send_routing_specific_node(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from casty.cluster.messages import ClusteredSend

        cluster = MagicMock()
        cluster.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        await ref.send(SampleMsg(42), routing='node-2')

        call_args = cluster.send.call_args
        sent_msg = call_args[0][0]
        assert isinstance(sent_msg, ClusteredSend)
        assert sent_msg.routing == "node-2"
