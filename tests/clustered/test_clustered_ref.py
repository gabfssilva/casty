import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock


class TestClusteredRef:
    def test_creation(self):
        from casty.cluster.clustered_ref import ClusteredActorRef

        cluster = MagicMock()
        ref = ClusteredActorRef(
            actor_id="user-123",
            cluster=cluster,
            local_ref=None,
        )

        assert ref.actor_id == "user-123"

    @pytest.mark.asyncio
    async def test_send_delegates_to_cluster(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from dataclasses import dataclass

        @dataclass
        class Increment:
            amount: int

        cluster = MagicMock()
        cluster.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="counter-1",
            cluster=cluster,
            local_ref=None,
        )

        await ref.send(Increment(5))

        cluster.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_ask_delegates_to_cluster(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from dataclasses import dataclass

        @dataclass
        class GetValue:
            pass

        cluster = MagicMock()
        cluster.ask = AsyncMock(return_value=42)

        ref = ClusteredActorRef(
            actor_id="counter-1",
            cluster=cluster,
            local_ref=None,
        )

        result = await ref.ask(GetValue())

        assert result == 42
        cluster.ask.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_with_custom_routing(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from casty.cluster.messages import ClusteredSend
        from dataclasses import dataclass

        @dataclass
        class Msg:
            pass

        cluster = MagicMock()
        cluster.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        await ref.send(Msg(), routing='node-1')

        call_args = cluster.send.call_args
        sent_msg = call_args[0][0]
        assert isinstance(sent_msg, ClusteredSend)
        assert sent_msg.routing == 'node-1'

    @pytest.mark.asyncio
    async def test_rshift_operator_calls_send(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from dataclasses import dataclass

        @dataclass
        class Msg:
            value: int

        cluster = MagicMock()
        cluster.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        await (ref >> Msg(42))

        cluster.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_lshift_operator_calls_ask(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from dataclasses import dataclass

        @dataclass
        class Query:
            pass

        cluster = MagicMock()
        cluster.ask = AsyncMock(return_value=100)

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        result = await (ref << Query())

        assert result == 100
        cluster.ask.assert_called_once()

    def test_repr(self):
        from casty.cluster.clustered_ref import ClusteredActorRef

        cluster = MagicMock()
        ref = ClusteredActorRef(
            actor_id="user-456",
            cluster=cluster,
            local_ref=None,
        )

        assert repr(ref) == "ClusteredRef(user-456)"

    @pytest.mark.asyncio
    async def test_send_default_routing_is_leader(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from casty.cluster.messages import ClusteredSend
        from dataclasses import dataclass

        @dataclass
        class Msg:
            pass

        cluster = MagicMock()
        cluster.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        await ref.send(Msg())

        call_args = cluster.send.call_args
        sent_message = call_args[0][0]
        assert isinstance(sent_message, ClusteredSend)
        assert sent_message.routing == "leader"


class TestClusteredRefLocal:
    @pytest.mark.asyncio
    async def test_send_uses_local_ref_when_routing_local(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from dataclasses import dataclass

        @dataclass
        class Msg:
            value: int

        cluster = MagicMock()
        cluster.send = AsyncMock()

        local_ref = MagicMock()
        local_ref.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=local_ref,
        )

        await ref.send(Msg(42), routing='local')

        local_ref.send.assert_called_once()
        cluster.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_ask_uses_local_ref_when_routing_local(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from dataclasses import dataclass

        @dataclass
        class Query:
            pass

        cluster = MagicMock()
        cluster.ask = AsyncMock()

        local_ref = MagicMock()
        local_ref.ask = AsyncMock(return_value=42)

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=local_ref,
        )

        result = await ref.ask(Query(), routing='local')

        assert result == 42
        local_ref.ask.assert_called_once()
        cluster.ask.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_uses_cluster_when_no_local_ref(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from dataclasses import dataclass

        @dataclass
        class Msg:
            value: int

        cluster = MagicMock()
        cluster.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        await ref.send(Msg(42))

        cluster.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_ask_uses_cluster_when_no_local_ref(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from dataclasses import dataclass

        @dataclass
        class Query:
            pass

        cluster = MagicMock()
        cluster.ask = AsyncMock(return_value=99)

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=None,
        )

        result = await ref.ask(Query())

        assert result == 99
        cluster.ask.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_routing_fastest_uses_local_if_available(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from dataclasses import dataclass

        @dataclass
        class Msg:
            value: int

        cluster = MagicMock()
        cluster.send = AsyncMock()

        local_ref = MagicMock()
        local_ref.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=local_ref,
        )

        await ref.send(Msg(42), routing='fastest')

        local_ref.send.assert_called_once()
        cluster.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_routing_leader_uses_cluster(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
        from casty.cluster.messages import ClusteredSend
        from dataclasses import dataclass

        @dataclass
        class Msg:
            value: int

        cluster = MagicMock()
        cluster.send = AsyncMock()

        local_ref = MagicMock()
        local_ref.send = AsyncMock()

        ref = ClusteredActorRef(
            actor_id="test",
            cluster=cluster,
            local_ref=local_ref,
        )

        await ref.send(Msg(42), routing='leader')

        cluster.send.assert_called_once()
        local_ref.send.assert_not_called()

        call_args = cluster.send.call_args
        sent_msg = call_args[0][0]
        assert isinstance(sent_msg, ClusteredSend)
        assert sent_msg.routing == "leader"
