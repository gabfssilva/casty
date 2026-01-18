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
            write_consistency='one',
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
            write_consistency='one',
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
            write_consistency='one',
        )

        result = await ref.ask(GetValue())

        assert result == 42
        cluster.ask.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_with_custom_consistency(self):
        from casty.cluster.clustered_ref import ClusteredActorRef
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
            write_consistency='one',
        )

        await ref.send(Msg(), consistency='all')

        call_args = cluster.send.call_args
        assert call_args is not None

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
            write_consistency='one',
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
            write_consistency='one',
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
            write_consistency='one',
        )

        assert repr(ref) == "ClusteredRef(user-456)"

    @pytest.mark.asyncio
    async def test_send_uses_write_consistency_by_default(self):
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
            write_consistency='quorum',
        )

        await ref.send(Msg())

        call_args = cluster.send.call_args
        sent_message = call_args[0][0]
        assert isinstance(sent_message, ClusteredSend)
        assert sent_message.actor_id == "test"


class TestClusteredRefLocal:
    @pytest.mark.asyncio
    async def test_send_uses_local_ref_when_available(self):
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
            write_consistency='one',
        )

        await ref.send(Msg(42))

        local_ref.send.assert_called_once()
        cluster.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_ask_uses_local_ref_when_available(self):
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
            write_consistency='one',
        )

        result = await ref.ask(Query())

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
            write_consistency='one',
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
            write_consistency='one',
        )

        result = await ref.ask(Query())

        assert result == 99
        cluster.ask.assert_called_once()
