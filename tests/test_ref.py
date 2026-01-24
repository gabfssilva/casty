import pytest
import asyncio
from dataclasses import dataclass


@dataclass
class Ping:
    value: int


@pytest.mark.asyncio
async def test_local_ref_send():
    from casty.ref import LocalActorRef
    from casty.mailbox import ActorMailbox
    from casty.envelope import Envelope

    mailbox: ActorMailbox[Ping] = ActorMailbox(self_id="test/t1")

    async def deliver(envelope: Envelope[Ping]) -> None:
        await mailbox.put(envelope)

    ref: LocalActorRef[Ping] = LocalActorRef(actor_id="test/t1", _deliver=deliver)

    await ref.send(Ping(42))

    envelope = await mailbox._queue.get()
    assert envelope.payload == Ping(42)


@pytest.mark.asyncio
async def test_local_ref_ask():
    async with asyncio.timeout(2.5):
        from casty import actor, Mailbox, ActorSystem

        @actor
        async def echo(*, mailbox: Mailbox[Ping]):
            async for msg, ctx in mailbox:
                await ctx.reply(msg.value * 10)

        async with ActorSystem() as system:
            ref = await system.actor(echo(), name="echo")
            result = await ref.ask(Ping(10))
            assert result == 100


@pytest.mark.asyncio
async def test_local_ref_operators():
    from casty.ref import LocalActorRef
    from casty.mailbox import ActorMailbox
    from casty.envelope import Envelope

    mailbox: ActorMailbox[Ping] = ActorMailbox(self_id="test/t1")

    async def deliver(envelope: Envelope[Ping]) -> None:
        await mailbox.put(envelope)

    ref: LocalActorRef[Ping] = LocalActorRef(actor_id="test/t1", _deliver=deliver)

    await (ref >> Ping(1))
    envelope = await mailbox._queue.get()
    assert envelope.payload == Ping(1)
