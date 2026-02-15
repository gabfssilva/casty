from __future__ import annotations

import asyncio


from casty.core.mailbox import Mailbox, MailboxOverflowStrategy


async def test_unbounded_mailbox_put_and_get() -> None:
    mb: Mailbox[str] = Mailbox()
    mb.put("hello")
    mb.put("world")
    assert await mb.get() == "hello"
    assert await mb.get() == "world"


async def test_unbounded_mailbox_no_limit() -> None:
    mb: Mailbox[int] = Mailbox()
    for i in range(10_000):
        mb.put(i)
    assert mb.size() == 10_000


async def test_bounded_drop_new_discards_incoming() -> None:
    mb: Mailbox[int] = Mailbox(capacity=2, overflow=MailboxOverflowStrategy.drop_new)
    mb.put(1)
    mb.put(2)
    mb.put(3)  # dropped
    assert mb.size() == 2
    assert await mb.get() == 1
    assert await mb.get() == 2


async def test_bounded_drop_oldest_discards_oldest() -> None:
    mb: Mailbox[int] = Mailbox(capacity=2, overflow=MailboxOverflowStrategy.drop_oldest)
    mb.put(1)
    mb.put(2)
    mb.put(3)  # 1 is dropped
    assert mb.size() == 2
    assert await mb.get() == 2
    assert await mb.get() == 3


async def test_bounded_backpressure_blocks_until_space() -> None:
    mb: Mailbox[int] = Mailbox(capacity=1, overflow=MailboxOverflowStrategy.backpressure)
    mb.put(1)

    put_done = False

    async def delayed_put() -> None:
        nonlocal put_done
        await mb.put_async(2)
        put_done = True

    task = asyncio.create_task(delayed_put())
    await asyncio.sleep(0.05)
    assert not put_done  # blocked

    await mb.get()  # free space
    await asyncio.sleep(0.05)
    assert put_done
    assert await mb.get() == 2
    task.cancel()


async def test_get_blocks_until_message() -> None:
    mb: Mailbox[str] = Mailbox()
    result: list[str] = []

    async def consumer() -> None:
        result.append(await mb.get())

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.05)
    assert result == []

    mb.put("hello")
    await asyncio.sleep(0.05)
    assert result == ["hello"]
    task.cancel()


async def test_empty_returns_true_when_empty() -> None:
    mb: Mailbox[str] = Mailbox()
    assert mb.empty()
    mb.put("x")
    assert not mb.empty()
