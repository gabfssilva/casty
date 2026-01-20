import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox, ActorSystem
from casty.envelope import Envelope
from casty.serializable import serializable, serialize


@serializable
@dataclass
class Ping:
    value: int


@pytest.mark.asyncio
async def test_router_register_and_deliver():
    from casty.cluster.router import router_actor
    from casty.cluster.transport_messages import Register, Deliver

    received = []

    @actor
    async def receiver(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received.append(msg)

    async with ActorSystem() as system:
        router_ref = await system.actor(router_actor(), name="router")

        receiver_ref = await system.actor(receiver(), name="recv")

        await router_ref.send(Register(ref=receiver_ref))

        envelope = Envelope(payload=Ping(42), target=receiver_ref.actor_id)
        envelope_bytes = serialize(envelope)

        await router_ref.send(Deliver(data=envelope_bytes))

        await asyncio.sleep(0.05)

        assert len(received) == 1
        assert received[0] == Ping(42)


@pytest.mark.asyncio
async def test_router_deliver_to_unregistered_actor():
    from casty.cluster.router import router_actor
    from casty.cluster.transport_messages import Deliver

    async with ActorSystem() as system:
        router_ref = await system.actor(router_actor(), name="router")

        envelope = Envelope(payload=Ping(99), target="nonexistent/actor")
        envelope_bytes = serialize(envelope)

        await router_ref.send(Deliver(data=envelope_bytes))

        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_router_multiple_actors():
    from casty.cluster.router import router_actor
    from casty.cluster.transport_messages import Register, Deliver

    received_1 = []
    received_2 = []

    @actor
    async def receiver1(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received_1.append(msg)

    @actor
    async def receiver2(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received_2.append(msg)

    async with ActorSystem() as system:
        router_ref = await system.actor(router_actor(), name="router")

        ref1 = await system.actor(receiver1(), name="recv1")
        ref2 = await system.actor(receiver2(), name="recv2")

        await router_ref.send(Register(ref=ref1))
        await router_ref.send(Register(ref=ref2))

        envelope1 = Envelope(payload=Ping(1), target=ref1.actor_id)
        envelope2 = Envelope(payload=Ping(2), target=ref2.actor_id)

        await router_ref.send(Deliver(data=serialize(envelope1)))
        await router_ref.send(Deliver(data=serialize(envelope2)))

        await asyncio.sleep(0.05)

        assert len(received_1) == 1
        assert received_1[0] == Ping(1)
        assert len(received_2) == 1
        assert received_2[0] == Ping(2)
