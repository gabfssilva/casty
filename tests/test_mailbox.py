import pytest
import asyncio
from dataclasses import dataclass


@dataclass
class Ping:
    value: int


@pytest.mark.asyncio
async def test_mailbox_iteration():
    from casty.mailbox import Mailbox
    from casty.context import Context
    from casty.envelope import Envelope

    queue: asyncio.Queue[Envelope] = asyncio.Queue()
    mailbox: Mailbox[Ping] = Mailbox(queue)

    # Put messages
    await queue.put(Envelope(Ping(1), sender="a"))
    await queue.put(Envelope(Ping(2), sender="b"))

    # Iterate
    results = []
    count = 0
    async for msg, ctx in mailbox:
        results.append((msg, ctx.sender))
        count += 1
        if count >= 2:
            break

    assert results == [(Ping(1), "a"), (Ping(2), "b")]


@pytest.mark.asyncio
async def test_mailbox_stop():
    from casty.mailbox import Mailbox, Stop
    from casty.envelope import Envelope

    queue: asyncio.Queue[Envelope] = asyncio.Queue()
    mailbox: Mailbox[Ping] = Mailbox(queue)

    await queue.put(Envelope(Ping(1)))
    await queue.put(Envelope(Stop()))

    results = []
    async for msg, ctx in mailbox:
        results.append(msg)

    assert results == [Ping(1)]
