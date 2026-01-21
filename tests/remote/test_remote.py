import pytest
import asyncio
from dataclasses import dataclass

from casty import ActorSystem, actor, Mailbox
from casty.serializable import serializable
from casty.remote import (
    remote, Listen, Connect, Listening, Connected,
    ListenFailed, ConnectFailed, Expose, Lookup, LookupResult,
)


@pytest.mark.asyncio
async def test_remote_listen():
    async with ActorSystem() as system:
        remote_mgr = await system.actor(remote(), name="remote")

        result = await remote_mgr.ask(Listen(port=0))

        assert isinstance(result, Listening)
        assert result.address[1] > 0


@pytest.mark.asyncio
async def test_remote_listen_failed():
    async with ActorSystem() as system:
        remote_mgr = await system.actor(remote(), name="remote")

        result = await remote_mgr.ask(Listen(port=-1))

        assert isinstance(result, ListenFailed)


@pytest.mark.asyncio
async def test_remote_connect():
    async with ActorSystem() as system:
        remote_mgr = await system.actor(remote(), name="remote")

        server_result = await remote_mgr.ask(Listen(port=0))
        assert isinstance(server_result, Listening)
        port = server_result.address[1]

        client_result = await remote_mgr.ask(Connect(host="127.0.0.1", port=port))

        assert isinstance(client_result, Connected)
        assert client_result.peer_id is not None


@pytest.mark.asyncio
async def test_remote_connect_failed():
    async with ActorSystem() as system:
        remote_mgr = await system.actor(remote(), name="remote")

        result = await remote_mgr.ask(Connect(host="127.0.0.1", port=59999))

        assert isinstance(result, ConnectFailed)


@pytest.mark.asyncio
async def test_remote_expose_and_lookup():
    @serializable
    @dataclass
    class Ping:
        pass

    @serializable
    @dataclass
    class Pong:
        pass

    @actor
    async def ping_actor(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply(Pong())

    async with ActorSystem() as system_server, ActorSystem() as system_client:
        # Server
        server_remote = await system_server.actor(remote(), name="remote")
        server_result = await server_remote.ask(Listen(port=0))
        port = server_result.address[1]

        # Expose on server's remote manager directly
        ping = await system_server.actor(ping_actor(), name="ping")
        await server_remote.ask(Expose(ref=ping, name="ping"))

        # Client
        client_remote = await system_client.actor(remote(), name="remote")
        await client_remote.ask(Connect(host="127.0.0.1", port=port))

        await asyncio.sleep(0.05)

        # Lookup from client
        lookup_result = await client_remote.ask(Lookup("ping"))

        assert isinstance(lookup_result, LookupResult)
        assert lookup_result.ref is not None

        result = await lookup_result.ref.ask(Ping())
        assert isinstance(result, Pong)


@pytest.mark.asyncio
async def test_remote_bidirectional():
    """Test that both server and client can expose and lookup actors."""
    @serializable
    @dataclass
    class Ping:
        message: str

    @serializable
    @dataclass
    class Pong:
        message: str

    @actor
    async def echo_actor(name: str, *, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply(Pong(message=f"{name}: {msg.message}"))

    async with ActorSystem() as system_a, ActorSystem() as system_b:
        remote_a = await system_a.actor(remote(), name="remote")
        remote_b = await system_b.actor(remote(), name="remote")

        # A listens, B connects
        listen_result = await remote_a.ask(Listen(port=0))
        port = listen_result.address[1]

        await remote_b.ask(Connect(host="127.0.0.1", port=port))

        # A exposes actor
        echo_a = await system_a.actor(echo_actor("A"), name="echo-a")
        await remote_a.ask(Expose(ref=echo_a, name="echo-a"))

        # B exposes actor
        echo_b = await system_b.actor(echo_actor("B"), name="echo-b")
        await remote_b.ask(Expose(ref=echo_b, name="echo-b"))

        await asyncio.sleep(0.1)

        # B looks up A's actor (client -> server)
        result_a = await remote_b.ask(Lookup("echo-a"))
        assert result_a.ref is not None
        response_a = await result_a.ref.ask(Ping(message="from B"))
        assert response_a.message == "A: from B"

        # A looks up B's actor (server -> client) - TRUE BIDIRECTIONAL TEST
        result_b = await remote_a.ask(Lookup("echo-b"))
        assert result_b.ref is not None, "Server could not lookup client's actor - bidirectional failed!"
        response_b = await result_b.ref.ask(Ping(message="from A"))
        assert response_b.message == "B: from A"
