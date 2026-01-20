import pytest
import asyncio
from dataclasses import dataclass


@dataclass
class Increment:
    amount: int


@dataclass
class Get:
    pass


def test_actor_decorator_creates_behavior():
    from casty.actor import actor, Behavior
    from casty.mailbox import Mailbox

    @actor
    async def counter(initial: int, *, mailbox: Mailbox[Increment | Get]):
        count = initial
        async for msg, ctx in mailbox:
            match msg:
                case Increment(amount):
                    count += amount
                case Get():
                    await ctx.reply(count)

    # Calling counter(0) returns a Behavior, not a coroutine
    behavior = counter(0)
    assert isinstance(behavior, Behavior)
    assert behavior.initial_args == (0,)
    assert behavior.initial_kwargs == {}


def test_behavior_stores_function():
    from casty.actor import actor, Behavior
    from casty.mailbox import Mailbox

    @actor
    async def simple(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass

    behavior = simple()
    assert behavior.func is not None
    assert callable(behavior.func)


@pytest.mark.asyncio
async def test_behavior_can_be_started():
    from casty.actor import actor, Behavior
    from casty.mailbox import Mailbox, Stop
    from casty.envelope import Envelope

    results = []

    @actor
    async def collector(prefix: str, *, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            results.append(f"{prefix}:{msg}")

    behavior = collector("test")

    # Create queue and mailbox
    queue: asyncio.Queue = asyncio.Queue()
    mailbox = Mailbox(queue, self_id="collector/c1")

    # Start the behavior
    task = asyncio.create_task(behavior.func("test", mailbox=mailbox))

    # Send messages
    await queue.put(Envelope("hello"))
    await queue.put(Envelope("world"))
    await queue.put(Envelope(Stop()))

    await task

    assert results == ["test:hello", "test:world"]
