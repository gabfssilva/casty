import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from casty import ActorSystem, actor, Mailbox
from casty.io.framing import RawFramer, LengthPrefixedFramer
from casty.io.messages import (
    Register, Write, Close,
    Received, PeerClosed, InboundEvent,
)
from casty.io.connection import tcp_connection


@pytest.mark.asyncio
async def test_connection_receives_data():
    received_messages: list[InboundEvent] = []

    @actor
    async def collector(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            received_messages.append(msg)

    async with ActorSystem() as system:
        handler = await system.actor(collector(), name="collector")

        reader = AsyncMock(spec=asyncio.StreamReader)
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 8080))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        reader.read = AsyncMock(side_effect=[b"hello", b""])

        conn = await system.actor(
            tcp_connection(reader, writer, handler, RawFramer()),
            name="conn"
        )

        await asyncio.sleep(0.05)

        assert len(received_messages) >= 1
        assert isinstance(received_messages[0], Received)
        assert received_messages[0].data == b"hello"


@pytest.mark.asyncio
async def test_connection_sends_data():
    @actor
    async def dummy(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            pass

    async with ActorSystem() as system:
        handler = await system.actor(dummy(), name="dummy")

        reader = AsyncMock(spec=asyncio.StreamReader)
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        reader.read = AsyncMock(side_effect=[asyncio.CancelledError()])

        conn = await system.actor(
            tcp_connection(reader, writer, handler, RawFramer()),
            name="conn"
        )

        await conn.send(Write(b"hello"))
        await asyncio.sleep(0.01)

        writer.write.assert_called_with(b"hello")
        writer.drain.assert_called()


@pytest.mark.asyncio
async def test_connection_close():
    @actor
    async def dummy(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            pass

    async with ActorSystem() as system:
        handler = await system.actor(dummy(), name="dummy")

        reader = AsyncMock(spec=asyncio.StreamReader)
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        reader.read = AsyncMock(side_effect=[asyncio.CancelledError()])

        conn = await system.actor(
            tcp_connection(reader, writer, handler, RawFramer()),
            name="conn"
        )

        await conn.send(Close())
        await asyncio.sleep(0.01)

        writer.close.assert_called()


@pytest.mark.asyncio
async def test_connection_with_length_prefixed_framing():
    received_messages: list[InboundEvent] = []

    @actor
    async def collector(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            received_messages.append(msg)

    async with ActorSystem() as system:
        handler = await system.actor(collector(), name="collector")

        reader = AsyncMock(spec=asyncio.StreamReader)
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        reader.read = AsyncMock(side_effect=[
            b"\x00\x00\x00\x05hello",
            b""
        ])

        conn = await system.actor(
            tcp_connection(reader, writer, handler, LengthPrefixedFramer()),
            name="conn"
        )

        await asyncio.sleep(0.05)

        received = [m for m in received_messages if isinstance(m, Received)]
        assert len(received) == 1
        assert received[0].data == b"hello"
