import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox, ActorSystem
from casty.serializable import serializable, serialize
from casty.envelope import Envelope


@serializable
@dataclass
class InboundTestMsg:
    value: str


@pytest.mark.asyncio
async def test_inbound_accepts_and_receives():
    from casty.cluster.inbound import inbound_actor, GetPort
    from casty.cluster.router import router_actor
    from casty.cluster.transport_messages import Register

    received = []

    @actor
    async def receiver(*, mailbox: Mailbox[InboundTestMsg]):
        async for msg, ctx in mailbox:
            received.append(msg.value)

    async with ActorSystem() as system:
        router_ref = await system.actor(router_actor(), name="router")
        receiver_ref = await system.actor(receiver(), name="receiver")

        await router_ref.send(Register(ref=receiver_ref))

        inbound_ref = await system.actor(
            inbound_actor("127.0.0.1", 0, router_ref),
            name="inbound"
        )

        port = await inbound_ref.ask(GetPort())
        assert port > 0

        _, writer = await asyncio.open_connection("127.0.0.1", port)

        envelope = Envelope(payload=InboundTestMsg("hello"), target=receiver_ref.actor_id)
        data = serialize(envelope)
        writer.write(len(data).to_bytes(4, "big") + data)
        await writer.drain()

        await asyncio.sleep(0.1)
        writer.close()
        await writer.wait_closed()

        assert received == ["hello"]


@pytest.mark.asyncio
async def test_inbound_get_port_returns_actual_port():
    from casty.cluster.inbound import inbound_actor, GetPort
    from casty.cluster.router import router_actor

    async with ActorSystem() as system:
        router_ref = await system.actor(router_actor(), name="router")

        inbound_ref = await system.actor(
            inbound_actor("127.0.0.1", 0, router_ref),
            name="inbound"
        )

        port = await inbound_ref.ask(GetPort())
        assert isinstance(port, int)
        assert port > 0


@pytest.mark.asyncio
async def test_inbound_multiple_connections():
    from casty.cluster.inbound import inbound_actor, GetPort
    from casty.cluster.router import router_actor
    from casty.cluster.transport_messages import Register

    received = []

    @actor
    async def receiver(*, mailbox: Mailbox[InboundTestMsg]):
        async for msg, ctx in mailbox:
            received.append(msg.value)

    async with ActorSystem() as system:
        router_ref = await system.actor(router_actor(), name="router")
        receiver_ref = await system.actor(receiver(), name="receiver")

        await router_ref.send(Register(ref=receiver_ref))

        inbound_ref = await system.actor(
            inbound_actor("127.0.0.1", 0, router_ref),
            name="inbound"
        )

        port = await inbound_ref.ask(GetPort())

        _, writer1 = await asyncio.open_connection("127.0.0.1", port)
        _, writer2 = await asyncio.open_connection("127.0.0.1", port)

        envelope1 = Envelope(payload=InboundTestMsg("from-conn-1"), target=receiver_ref.actor_id)
        envelope2 = Envelope(payload=InboundTestMsg("from-conn-2"), target=receiver_ref.actor_id)

        data1 = serialize(envelope1)
        data2 = serialize(envelope2)

        writer1.write(len(data1).to_bytes(4, "big") + data1)
        await writer1.drain()

        writer2.write(len(data2).to_bytes(4, "big") + data2)
        await writer2.drain()

        await asyncio.sleep(0.1)

        writer1.close()
        await writer1.wait_closed()
        writer2.close()
        await writer2.wait_closed()

        assert sorted(received) == ["from-conn-1", "from-conn-2"]
