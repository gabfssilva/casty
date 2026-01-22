import asyncio
import pytest

from casty import actor, Mailbox, message, State
from casty.actor_config import Routing
from casty.cluster import DevelopmentCluster


@message(readonly=True)
class Get:
    pass


@message
class Increment:
    amount: int


@pytest.mark.asyncio
async def test_replicated_actor_creation():
    @actor(replicated=2, routing={Get: Routing.LEADER, Increment: Routing.LEADER})
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Increment(amount):
                    state.set(state.value + amount)

    async with DevelopmentCluster(nodes=3) as cluster:
        ref = await cluster.actor(counter(0), name="main")

        result = await ref.ask(Get())
        assert result == 0

        await ref.send(Increment(5))
        result = await ref.ask(Get())
        assert result == 5


@pytest.mark.asyncio
async def test_replicated_actor_with_override():
    @actor(replicated=2)
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Increment(amount):
                    state.set(state.value + amount)

    async with DevelopmentCluster(nodes=3) as cluster:
        ref = await cluster.actor(counter(0), name="main")

        result = await ref.ask(Get())
        assert result == 0


@pytest.mark.asyncio
async def test_non_replicated_actor_with_replicas_param():
    """Test that replicated actors work with routing configuration."""
    @actor(replicated=2, routing={Get: Routing.LOCAL_FIRST, Increment: Routing.LOCAL_FIRST})
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Increment(amount):
                    state.set(state.value + amount)
                    await ctx.reply(state.value)

    async with asyncio.timeout(10):
        async with DevelopmentCluster(nodes=1) as cluster:
            node = cluster.nodes[0]
            ref = await node.actor(
                counter(0),
                name="main",
            )

            result = await ref.ask(Get())
            assert result == 0

            await ref.ask(Increment(10))
            result = await ref.ask(Get())
            assert result == 10


@pytest.mark.asyncio
async def test_replicated_actor_config_preserved():
    @actor(replicated=5, routing={Get: Routing.LOCAL_FIRST, Increment: Routing.LOCAL_FIRST})
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Increment(amount):
                    state.set(state.value + amount)

    assert hasattr(counter, "__replication_config__")
    config = counter.__replication_config__
    assert config.replicated == 5
    assert config.routing == {Get: Routing.LOCAL_FIRST, Increment: Routing.LOCAL_FIRST}

    async with DevelopmentCluster(nodes=3) as cluster:
        ref = await cluster.actor(counter(0), name="main")

        result = await ref.ask(Get())
        assert result == 0
