import asyncio
import pytest
from dataclasses import dataclass

from casty import ActorSystem, actor, Mailbox
from casty.remote import remote, Listen, Listening, Connect, Connected, Expose, Lookup, LookupResult
from casty.serializable import serializable


@serializable
@dataclass
class Ping:
    value: int = 0


@pytest.mark.asyncio
async def test_remote_listen_opens_port():
    """Listen() opens TCP port for incoming connections"""
    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            remote_ref = await system.actor(remote(), name="remote")

            result = await remote_ref.ask(Listen(port=0, host="127.0.0.1"))

            assert isinstance(result, Listening)
            assert result.address[0] == "127.0.0.1"
            assert result.address[1] > 0


@pytest.mark.asyncio
async def test_remote_connect_to_peer():
    """Connect() establishes connection to remote peer"""
    async with asyncio.timeout(5):
        async with ActorSystem() as system1, ActorSystem() as system2:
            remote1 = await system1.actor(remote(), name="remote")
            remote2 = await system2.actor(remote(), name="remote")

            listen_result = await remote1.ask(Listen(port=0, host="127.0.0.1"))
            port = listen_result.address[1]

            connect_result = await remote2.ask(Connect(host="127.0.0.1", port=port))

            assert isinstance(connect_result, Connected)


@pytest.mark.asyncio
async def test_expose_makes_actor_accessible():
    """Expose() makes local actor accessible to remote peers"""

    @actor
    async def service(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply(f"pong:{msg.value}")

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            remote_ref = await system.actor(remote(), name="remote")
            service_ref = await system.actor(service(), name="service")

            await remote_ref.send(Expose(ref=service_ref, name="my-service"))

            await asyncio.sleep(0.05)
            # Actor is now exposed (verifiable via Lookup from another system)


@pytest.mark.asyncio
async def test_lookup_finds_remote_actor():
    """Lookup() finds actor exposed on remote peer"""

    @actor
    async def service(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply(f"pong:{msg.value}")

    async with asyncio.timeout(10):
        async with ActorSystem() as system1, ActorSystem() as system2:
            remote1 = await system1.actor(remote(), name="remote")
            remote2 = await system2.actor(remote(), name="remote")

            service_ref = await system1.actor(service(), name="service")
            await remote1.send(Expose(ref=service_ref, name="my-service"))

            listen_result = await remote1.ask(Listen(port=0, host="127.0.0.1"))
            port = listen_result.address[1]

            await remote2.ask(Connect(host="127.0.0.1", port=port))
            await asyncio.sleep(0.1)

            lookup_result = await remote2.ask(
                Lookup(name="my-service", peer=f"127.0.0.1:{port}", timeout=5.0)
            )

            assert isinstance(lookup_result, LookupResult)
            assert lookup_result.ref is not None
            assert lookup_result.error is None


@pytest.mark.asyncio
async def test_send_message_to_remote_actor():
    """Messages can be sent to actor on remote system"""

    @actor
    async def service(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply(f"pong:{msg.value}")

    async with asyncio.timeout(10):
        async with ActorSystem() as system1, ActorSystem() as system2:
            remote1 = await system1.actor(remote(), name="remote")
            remote2 = await system2.actor(remote(), name="remote")

            service_ref = await system1.actor(service(), name="service")
            await remote1.send(Expose(ref=service_ref, name="my-service"))

            listen_result = await remote1.ask(Listen(port=0, host="127.0.0.1"))
            port = listen_result.address[1]

            await remote2.ask(Connect(host="127.0.0.1", port=port))
            await asyncio.sleep(0.1)

            lookup_result = await remote2.ask(
                Lookup(name="my-service", peer=f"127.0.0.1:{port}", timeout=5.0)
            )

            result = await lookup_result.ref.ask(Ping(42))

            assert result == "pong:42"
