import pytest
from dataclasses import dataclass


@dataclass
class Ping:
    value: int


def test_mailbox_is_protocol():
    from casty.mailbox import Mailbox
    from typing import Protocol
    assert issubclass(Mailbox, Protocol) or hasattr(Mailbox, '__class_getitem__')


def test_actor_mailbox_implements_protocol():
    from casty.mailbox import ActorMailbox

    mailbox = ActorMailbox(self_id="test", node_id="local")

    assert hasattr(mailbox, '__aiter__')
    assert hasattr(mailbox, '__anext__')
    assert hasattr(mailbox, 'put')


@pytest.mark.asyncio
async def test_mailbox_iteration():
    from casty.mailbox import ActorMailbox, Stop
    from casty.envelope import Envelope

    mailbox: ActorMailbox[Ping] = ActorMailbox()

    await mailbox.put(Envelope(Ping(1), sender="a"))
    await mailbox.put(Envelope(Ping(2), sender="b"))
    await mailbox.put(Envelope(Stop()))

    results = []
    async for msg, ctx in mailbox:
        results.append((msg, ctx.sender_id))

    assert results == [(Ping(1), "a"), (Ping(2), "b")]


@pytest.mark.asyncio
async def test_mailbox_stop():
    from casty.mailbox import ActorMailbox, Stop
    from casty.envelope import Envelope

    mailbox: ActorMailbox[Ping] = ActorMailbox()

    await mailbox.put(Envelope(Ping(1)))
    await mailbox.put(Envelope(Stop()))

    results = []
    async for msg, ctx in mailbox:
        results.append(msg)

    assert results == [Ping(1)]
