import pytest
import asyncio

from casty import ActorSystem, actor, Mailbox
from casty.io.tcp import tcp
from casty.io.messages import (
    Bind, Connect, Bound, BindFailed, Connected, ConnectFailed,
    Received, Register, Write, Close, PeerClosed,
    InboundEvent,
)
from casty.io.framing import LengthPrefixedFramer


@pytest.mark.asyncio
async def test_tcp_bind():
    received: list[InboundEvent] = []

    @actor
    async def handler(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            received.append(msg)

    async with ActorSystem() as system:
        tcp_mgr = await system.actor(tcp(), name="tcp")
        h = await system.actor(handler(), name="handler")

        await tcp_mgr.send(Bind(handler=h, port=0))
        await asyncio.sleep(0.05)

        assert len(received) == 1
        assert isinstance(received[0], Bound)
        assert received[0].local_address[1] > 0


@pytest.mark.asyncio
async def test_tcp_bind_failed():
    received1: list[InboundEvent] = []
    received2: list[InboundEvent] = []

    @actor
    async def handler1(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            received1.append(msg)

    @actor
    async def handler2(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            received2.append(msg)

    async with ActorSystem() as system:
        tcp_mgr = await system.actor(tcp(), name="tcp")
        h1 = await system.actor(handler1(), name="handler1")
        h2 = await system.actor(handler2(), name="handler2")

        await tcp_mgr.send(Bind(handler=h1, port=0))
        await asyncio.sleep(0.05)

        assert len(received1) == 1
        assert isinstance(received1[0], Bound)
        port = received1[0].local_address[1]

        await tcp_mgr.send(Bind(handler=h2, port=port))
        await asyncio.sleep(0.05)

        assert len(received2) == 1
        assert isinstance(received2[0], BindFailed)


@pytest.mark.asyncio
async def test_tcp_connect():
    server_received: list[InboundEvent] = []
    client_received: list[InboundEvent] = []

    @actor
    async def server_handler(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            server_received.append(msg)
            if isinstance(msg, Connected):
                await msg.connection.send(Register(ctx._self_ref))

    @actor
    async def client_handler(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            client_received.append(msg)

    async with ActorSystem() as system:
        tcp_mgr = await system.actor(tcp(), name="tcp")
        server = await system.actor(server_handler(), name="server")
        client = await system.actor(client_handler(), name="client")

        await tcp_mgr.send(Bind(handler=server, port=0))
        await asyncio.sleep(0.05)

        bound = server_received[0]
        assert isinstance(bound, Bound)
        port = bound.local_address[1]

        await tcp_mgr.send(Connect(handler=client, host="127.0.0.1", port=port))
        await asyncio.sleep(0.05)

        client_connected = [m for m in client_received if isinstance(m, Connected)]
        assert len(client_connected) == 1

        server_connected = [m for m in server_received if isinstance(m, Connected)]
        assert len(server_connected) == 1


@pytest.mark.asyncio
async def test_tcp_echo():
    server_received: list[InboundEvent] = []
    client_received: list[InboundEvent] = []

    @actor
    async def server_handler(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            server_received.append(msg)
            match msg:
                case Connected(connection, _, _):
                    await connection.send(Register(ctx._self_ref))
                case Received(data):
                    await ctx.sender.send(Write(data))

    @actor
    async def client_handler(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            client_received.append(msg)
            match msg:
                case Connected(connection, _, _):
                    await connection.send(Register(ctx._self_ref))
                    await connection.send(Write(b"hello"))

    async with ActorSystem() as system:
        tcp_mgr = await system.actor(tcp(), name="tcp")
        server = await system.actor(server_handler(), name="server")
        client = await system.actor(client_handler(), name="client")

        await tcp_mgr.send(Bind(handler=server, port=0, framing=LengthPrefixedFramer()))
        await asyncio.sleep(0.05)

        bound = server_received[0]
        port = bound.local_address[1]

        await tcp_mgr.send(Connect(
            handler=client,
            host="127.0.0.1",
            port=port,
            framing=LengthPrefixedFramer()
        ))
        await asyncio.sleep(0.1)

        client_received_msgs = [m for m in client_received if isinstance(m, Received)]
        assert len(client_received_msgs) == 1
        assert client_received_msgs[0].data == b"hello"


@pytest.mark.asyncio
async def test_tcp_connect_failed():
    received: list[InboundEvent] = []

    @actor
    async def handler(*, mailbox: Mailbox[InboundEvent]):
        async for msg, ctx in mailbox:
            received.append(msg)

    async with ActorSystem() as system:
        tcp_mgr = await system.actor(tcp(), name="tcp")
        h = await system.actor(handler(), name="handler")

        await tcp_mgr.send(Connect(handler=h, host="127.0.0.1", port=59999))
        await asyncio.sleep(0.1)

        assert len(received) == 1
        assert isinstance(received[0], ConnectFailed)
