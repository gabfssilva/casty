import pytest
import asyncio
from casty import ActorSystem, Actor, Context


class EchoActor(Actor[str]):
    async def receive(self, msg: str, ctx: Context) -> None:
        if ctx.sender:
            await ctx.sender.send(f"echo: {msg}")


@pytest.mark.asyncio
async def test_ask_with_reply_actor():
    async with ActorSystem.local() as system:
        echo = await system.actor(EchoActor, name="echo-actor")

        result = await echo.ask("hello")

        assert result == "echo: hello"


@pytest.mark.asyncio
async def test_ask_timeout():
    class SlowActor(Actor[str]):
        async def receive(self, msg: str, ctx: Context) -> None:
            await asyncio.sleep(10)
            if ctx.sender:
                await ctx.sender.send("done")

    async with ActorSystem.local() as system:
        slow = await system.actor(SlowActor, name="slow-actor")

        with pytest.raises(asyncio.TimeoutError):
            await slow.ask("hello", timeout=0.1)


@pytest.mark.asyncio
async def test_ask_multiple_sequential():
    class CounterActor(Actor[str]):
        def __init__(self):
            self.count = 0

        async def receive(self, msg: str, ctx: Context) -> None:
            self.count += 1
            if ctx.sender:
                await ctx.sender.send(self.count)

    async with ActorSystem.local() as system:
        counter = await system.actor(CounterActor, name="counter-actor")

        r1 = await counter.ask("ping")
        r2 = await counter.ask("ping")
        r3 = await counter.ask("ping")

        assert r1 == 1
        assert r2 == 2
        assert r3 == 3


@pytest.mark.asyncio
async def test_ctx_reply_uses_sender():
    """Test that ctx.reply() sends to sender when using ask()."""
    from dataclasses import dataclass

    @dataclass
    class GetValue:
        pass

    class ValueActor(Actor[GetValue]):
        async def receive(self, msg: GetValue, ctx: Context) -> None:
            await ctx.reply(42)

    async with ActorSystem.local() as system:
        actor = await system.actor(ValueActor, name="value-actor")
        result = await actor.ask(GetValue())
        assert result == 42


@pytest.mark.asyncio
async def test_reply_actor_cleanup_on_timeout():
    """Verify that _ReplyActor is cleaned up on timeout."""
    from casty.system import LocalSystem

    class NeverReplyActor(Actor[str]):
        async def receive(self, msg: str, ctx: Context) -> None:
            pass  # Never replies

    async with LocalSystem() as system:
        actor = await system.actor(NeverReplyActor, name="never-reply-actor")

        initial_count = len(system._supervision_tree._nodes)

        with pytest.raises(asyncio.TimeoutError):
            await actor.ask("hello", timeout=0.1)

        # Give some time for cleanup
        await asyncio.sleep(0.1)

        # The reply actor should be cleaned up
        final_count = len(system._supervision_tree._nodes)
        assert final_count == initial_count


@pytest.mark.asyncio
async def test_reply_actor_stops_after_response():
    """Verify that _ReplyActor stops itself after receiving a response."""
    from casty.system import LocalSystem

    class QuickReplyActor(Actor[str]):
        async def receive(self, msg: str, ctx: Context) -> None:
            await ctx.reply("done")

    async with LocalSystem() as system:
        actor = await system.actor(QuickReplyActor, name="quick-reply-actor")

        initial_count = len(system._supervision_tree._nodes)

        result = await actor.ask("hello")
        assert result == "done"

        # Give some time for the reply actor to stop
        await asyncio.sleep(0.1)

        # The reply actor should be cleaned up
        final_count = len(system._supervision_tree._nodes)
        assert final_count == initial_count
