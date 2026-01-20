import pytest
import asyncio


def test_context_properties():
    from casty.context import Context

    ctx = Context(
        self_id="counter/c1",
        sender="caller/x",
        node_id="node-1",
        is_leader=True,
    )

    assert ctx.self_id == "counter/c1"
    assert ctx.sender == "caller/x"
    assert ctx.node_id == "node-1"
    assert ctx.is_leader is True


@pytest.mark.asyncio
async def test_context_reply():
    from casty.context import Context

    future: asyncio.Future[int] = asyncio.Future()
    ctx = Context(
        self_id="counter/c1",
        reply_to=future,
    )

    await ctx.reply(42)
    assert future.result() == 42


@pytest.mark.asyncio
async def test_context_reply_no_future():
    from casty.context import Context

    ctx = Context(self_id="counter/c1")
    # Should not raise, just no-op
    await ctx.reply(42)
