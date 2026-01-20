import pytest
import asyncio
from dataclasses import dataclass


@dataclass
class Ping:
    value: int


@pytest.mark.asyncio
async def test_local_ref_send():
    from casty.ref import LocalActorRef
    from casty.mailbox import Mailbox
    from casty.envelope import Envelope

    queue: asyncio.Queue[Envelope] = asyncio.Queue()
    mailbox: Mailbox[Ping] = Mailbox(queue=queue, self_id="test/t1")
    ref: LocalActorRef[Ping] = LocalActorRef(actor_id="test/t1", mailbox=mailbox)

    await ref.send(Ping(42))

    envelope = await queue.get()
    assert envelope.payload == Ping(42)


@pytest.mark.asyncio
async def test_local_ref_ask():
    from casty.ref import LocalActorRef
    from casty.mailbox import Mailbox
    from casty.envelope import Envelope

    queue: asyncio.Queue[Envelope] = asyncio.Queue()
    mailbox: Mailbox[Ping] = Mailbox(queue=queue, self_id="test/t1")
    ref: LocalActorRef[Ping] = LocalActorRef(actor_id="test/t1", mailbox=mailbox)

    # Start ask (will wait for reply)
    ask_task = asyncio.create_task(ref.ask(Ping(10), timeout=1.0))

    # Simulate actor receiving and replying
    await asyncio.sleep(0.01)
    envelope = await queue.get()
    assert envelope.payload == Ping(10)
    assert envelope.reply_to is not None
    envelope.reply_to.set_result(100)

    result = await ask_task
    assert result == 100


@pytest.mark.asyncio
async def test_local_ref_ask_timeout():
    from casty.ref import LocalActorRef
    from casty.mailbox import Mailbox
    from casty.envelope import Envelope

    queue: asyncio.Queue[Envelope] = asyncio.Queue()
    mailbox: Mailbox[Ping] = Mailbox(queue=queue, self_id="test/t1")
    ref: LocalActorRef[Ping] = LocalActorRef(actor_id="test/t1", mailbox=mailbox)

    with pytest.raises(asyncio.TimeoutError):
        await ref.ask(Ping(10), timeout=0.01)


@pytest.mark.asyncio
async def test_local_ref_operators():
    from casty.ref import LocalActorRef
    from casty.mailbox import Mailbox
    from casty.envelope import Envelope

    queue: asyncio.Queue[Envelope] = asyncio.Queue()
    mailbox: Mailbox[Ping] = Mailbox(queue=queue, self_id="test/t1")
    ref: LocalActorRef[Ping] = LocalActorRef(actor_id="test/t1", mailbox=mailbox)

    # >> is send
    await (ref >> Ping(1))
    envelope = await queue.get()
    assert envelope.payload == Ping(1)
