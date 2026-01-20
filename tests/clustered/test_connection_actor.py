import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from casty import actor, Mailbox, ActorSystem
from casty.envelope import Envelope
from casty.serializable import serialize


@pytest.mark.asyncio
async def test_connection_transmit():
    from casty.cluster.connection import connection_actor
    from casty.cluster.transport_messages import Transmit

    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()

    reader = AsyncMock()
    reader.readexactly = AsyncMock(side_effect=asyncio.CancelledError)

    router_ref = AsyncMock()

    async with ActorSystem() as system:
        conn_ref = await system.actor(
            connection_actor(reader, writer, router_ref),
            name="conn-test"
        )

        await conn_ref.send(Transmit(data=b"hello"))
        await asyncio.sleep(0.05)

        writer.write.assert_called()
        call_args = writer.write.call_args[0][0]
        assert call_args == len(b"hello").to_bytes(4, "big") + b"hello"


@pytest.mark.asyncio
async def test_connection_received_forwards_to_router():
    from casty.cluster.connection import connection_actor
    from casty.cluster.transport_messages import Deliver

    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()

    test_data = b"test-payload"
    length_bytes = len(test_data).to_bytes(4, "big")
    read_calls = [length_bytes, test_data]
    call_count = [0]

    async def mock_readexactly(n):
        if call_count[0] < len(read_calls):
            result = read_calls[call_count[0]]
            call_count[0] += 1
            return result
        await asyncio.sleep(10)
        raise asyncio.CancelledError

    reader = AsyncMock()
    reader.readexactly = mock_readexactly

    delivered = []

    async def mock_send(msg):
        delivered.append(msg)

    router_ref = AsyncMock()
    router_ref.send = mock_send

    async with ActorSystem() as system:
        conn_ref = await system.actor(
            connection_actor(reader, writer, router_ref),
            name="conn-test"
        )

        await asyncio.sleep(0.1)

        assert len(delivered) == 1
        assert isinstance(delivered[0], Deliver)
        assert delivered[0].data == test_data


@pytest.mark.asyncio
async def test_connection_disconnect_closes_writer():
    from casty.cluster.connection import connection_actor
    from casty.cluster.transport_messages import Disconnect

    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()

    reader = AsyncMock()
    reader.readexactly = AsyncMock(side_effect=asyncio.CancelledError)

    router_ref = AsyncMock()

    async with ActorSystem() as system:
        conn_ref = await system.actor(
            connection_actor(reader, writer, router_ref),
            name="conn-test"
        )

        await conn_ref.send(Disconnect())
        await asyncio.sleep(0.05)

        writer.close.assert_called_once()
        writer.wait_closed.assert_called_once()


@pytest.mark.asyncio
async def test_connection_incomplete_read_triggers_disconnect():
    from casty.cluster.connection import connection_actor
    from casty.cluster.transport_messages import Transmit

    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()

    reader = AsyncMock()
    reader.readexactly = AsyncMock(side_effect=asyncio.IncompleteReadError(b"", 4))

    router_ref = AsyncMock()

    async with ActorSystem() as system:
        conn_ref = await system.actor(
            connection_actor(reader, writer, router_ref),
            name="conn-test"
        )

        await asyncio.sleep(0.1)

        writer.close.assert_called_once()
        writer.wait_closed.assert_called_once()


@pytest.mark.asyncio
async def test_connection_multiple_transmits():
    from casty.cluster.connection import connection_actor
    from casty.cluster.transport_messages import Transmit

    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()

    reader = AsyncMock()
    reader.readexactly = AsyncMock(side_effect=asyncio.CancelledError)

    router_ref = AsyncMock()

    async with ActorSystem() as system:
        conn_ref = await system.actor(
            connection_actor(reader, writer, router_ref),
            name="conn-test"
        )

        await conn_ref.send(Transmit(data=b"first"))
        await conn_ref.send(Transmit(data=b"second"))
        await asyncio.sleep(0.05)

        assert writer.write.call_count == 2
        calls = [call[0][0] for call in writer.write.call_args_list]
        assert calls[0] == len(b"first").to_bytes(4, "big") + b"first"
        assert calls[1] == len(b"second").to_bytes(4, "big") + b"second"
