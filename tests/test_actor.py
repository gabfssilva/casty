import asyncio
import pytest
from dataclasses import dataclass

from casty import ActorSystem, actor, Mailbox


@dataclass
class Ping:
    value: int = 0


@dataclass
class Inc:
    amount: int


@dataclass
class Get:
    pass


@pytest.mark.asyncio
async def test_actor_receives_messages_in_order():
    """Messages sent to an actor are delivered in FIFO order"""
    received: list[int] = []

    @actor
    async def collector(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received.append(msg.value)

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(collector(), name="collector")

            await ref.send(Ping(1))
            await ref.send(Ping(2))
            await ref.send(Ping(3))

            await asyncio.sleep(0.05)

    assert received == [1, 2, 3]


@pytest.mark.asyncio
async def test_send_is_fire_and_forget():
    """send() returns immediately without waiting for processing"""
    processed = False

    @actor
    async def slow(*, mailbox: Mailbox[Ping]):
        nonlocal processed
        async for msg, ctx in mailbox:
            await asyncio.sleep(0.1)
            processed = True

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(slow(), name="slow")

            await ref.send(Ping())

            assert not processed


@pytest.mark.asyncio
async def test_ask_waits_for_response():
    """ask() sends message and waits for reply from actor"""

    @actor
    async def echo(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply(f"pong:{msg.value}")

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(echo(), name="echo")

            result = await ref.ask(Ping(42))

            assert result == "pong:42"


@pytest.mark.asyncio
async def test_ask_times_out():
    """ask() raises TimeoutError if no reply within timeout"""

    @actor
    async def silent(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            pass  # Never replies

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(silent(), name="silent")

            with pytest.raises(TimeoutError):
                await ref.ask(Ping(), timeout=0.1)


@pytest.mark.asyncio
async def test_reply_sends_to_sender():
    """ctx.reply() sends response back to the original sender"""
    from casty import Reply

    responses: list[str] = []

    @actor
    async def responder(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply("response")

    @actor
    async def requester(*, mailbox: Mailbox[Reply[str]]):
        async for msg, ctx in mailbox:
            responses.append(msg.result)

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            responder_ref = await system.actor(responder(), name="responder")
            requester_ref = await system.actor(requester(), name="requester")

            await responder_ref.send(Ping(), sender=requester_ref)

            await asyncio.sleep(0.05)

    assert responses == ["response"]


@pytest.mark.asyncio
async def test_actor_with_initial_state():
    """Actor can be initialized with State[T] that persists across messages"""
    from casty.state import State

    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Inc | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Inc(amount):
                    state.set(state.value + amount)
                case Get():
                    await ctx.reply(state.value)

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(counter(State(10)), name="counter")

            await ref.send(Inc(5))
            await ref.send(Inc(3))

            result = await ref.ask(Get())

            assert result == 18
