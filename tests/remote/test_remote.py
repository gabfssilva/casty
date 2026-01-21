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
        assert result.registry is not None
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
        assert client_result.registry is not None


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

    async with ActorSystem() as system:
        remote_mgr = await system.actor(remote(), name="remote")

        server_result = await remote_mgr.ask(Listen(port=0))
        server_registry = server_result.registry
        port = server_result.address[1]

        ping = await system.actor(ping_actor(), name="ping")
        await server_registry.send(Expose(ref=ping, name="ping"))

        client_result = await remote_mgr.ask(Connect(host="127.0.0.1", port=port))
        client_registry = client_result.registry

        await asyncio.sleep(0.05)

        lookup_result = await client_registry.ask(Lookup("ping"))

        assert isinstance(lookup_result, LookupResult)
        assert lookup_result.ref is not None

        result = await lookup_result.ref.ask(Ping())
        assert isinstance(result, Pong)
