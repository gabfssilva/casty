import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox, ActorSystem, ActorRef
from casty.reply import reply, Reply, Cancel


@dataclass
class Question:
    text: str
    reply_to: ActorRef[Reply[str]]


@actor
async def responder(*, mailbox: Mailbox[Question]):
    async for msg, ctx in mailbox:
        match msg:
            case Question(text=text, reply_to=reply_to):
                await reply_to.send(Reply(result=f"answer to: {text}"))


@pytest.mark.asyncio
async def test_reply_actor_receives_reply():
    """Test that reply actor correctly resolves promise when Reply is received."""
    async with ActorSystem() as system:
        @actor
        async def dummy_target(*, mailbox: Mailbox[Question]):
            async for msg, ctx in mailbox:
                pass

        target = await system.actor(dummy_target(), name="target")
        promise: asyncio.Future[str] = asyncio.Future()

        reply_ref = await system.actor(
            reply(
                content=Question(text="hello", reply_to=None),
                to=target,
                promise=promise,
                timeout=5.0,
            ),
            name="reply-1",
        )

        await reply_ref.send(Reply(result="the answer"))

        result = await asyncio.wait_for(promise, timeout=1.0)
        assert result == "the answer"


@pytest.mark.asyncio
async def test_reply_actor_timeout():
    """Test that reply actor rejects promise on timeout."""
    async with ActorSystem() as system:
        @actor
        async def black_hole(*, mailbox: Mailbox[Question]):
            async for msg, ctx in mailbox:
                pass

        target = await system.actor(black_hole(), name="black-hole")
        promise: asyncio.Future[str] = asyncio.Future()

        await system.actor(
            reply(
                content=Question(text="hello", reply_to=None),
                to=target,
                promise=promise,
                timeout=0.1,
            ),
            name="reply-timeout",
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(promise, timeout=1.0)


@pytest.mark.asyncio
async def test_reply_actor_exception_result():
    """Test that reply actor rejects promise when Reply contains an Exception."""
    async with ActorSystem() as system:
        @actor
        async def black_hole(*, mailbox: Mailbox[Question]):
            async for msg, ctx in mailbox:
                pass

        target = await system.actor(black_hole(), name="target")
        promise: asyncio.Future[str] = asyncio.Future()

        reply_ref = await system.actor(
            reply(
                content=Question(text="hello", reply_to=None),
                to=target,
                promise=promise,
                timeout=5.0,
            ),
            name="reply-exc",
        )

        await reply_ref.send(Reply(result=ValueError("something went wrong")))

        with pytest.raises(ValueError, match="something went wrong"):
            await asyncio.wait_for(promise, timeout=1.0)


@pytest.mark.asyncio
async def test_reply_actor_manual_cancel():
    """Test that reply actor rejects promise when Cancel is sent."""
    async with ActorSystem() as system:
        @actor
        async def black_hole(*, mailbox: Mailbox[Question]):
            async for msg, ctx in mailbox:
                pass

        target = await system.actor(black_hole(), name="target")
        promise: asyncio.Future[str] = asyncio.Future()

        reply_ref = await system.actor(
            reply(
                content=Question(text="hello", reply_to=None),
                to=target,
                promise=promise,
                timeout=5.0,
            ),
            name="reply-cancel",
        )

        await reply_ref.send(Cancel(reason=RuntimeError("cancelled by user")))

        with pytest.raises(RuntimeError, match="cancelled by user"):
            await asyncio.wait_for(promise, timeout=1.0)

@pytest.mark.asyncio
async def test_reply_actor_full_flow():
    """Test the complete flow: reply actor sends to target, target responds."""
    async with ActorSystem() as system:
        target = await system.actor(responder(), name="target")
        promise: asyncio.Future[str] = asyncio.Future()

        reply_ref = await system.actor(
            reply(
                content=Question(text="what is 2+2?", reply_to=None),
                to=target,
                promise=promise,
                timeout=5.0,
            ),
            name="reply-full",
        )

        # Manually send the message with the correct reply_to
        await target.send(Question(text="what is 2+2?", reply_to=reply_ref))

        result = await asyncio.wait_for(promise, timeout=1.0)
        assert result == "answer to: what is 2+2?"
