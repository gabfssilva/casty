import pytest
import asyncio
from casty import actor, Mailbox, message
from casty.state import State
from casty.cluster.replication import replicated, Routing
from casty.cluster import DevelopmentCluster


@message(readonly=True)
class GetValue:
    pass


@message
class SetValue:
    value: int


@pytest.mark.asyncio
async def test_replication_end_to_end():
    """Complete test of replicated actor with state"""

    @replicated(factor=2, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[GetValue | SetValue]):
        async for msg, ctx in mailbox:
            match msg:
                case GetValue():
                    await ctx.reply(state.value)
                case SetValue(value):
                    state.set(value)

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(counter(0), name="counter")

        result = await ref.ask(GetValue())
        assert result == 0

        await ref.send(SetValue(100))
        await asyncio.sleep(0.05)

        result = await ref.ask(GetValue())
        assert result == 100


@pytest.mark.asyncio
async def test_multiple_updates():
    """Multiple state updates"""

    @replicated(factor=2, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def accumulator(state: State[int], *, mailbox: Mailbox[GetValue | SetValue]):
        async for msg, ctx in mailbox:
            match msg:
                case GetValue():
                    await ctx.reply(state.value)
                case SetValue(value):
                    state.set(state.value + value)

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(accumulator(0), name="acc")

        for i in range(10):
            await ref.send(SetValue(1))

        await asyncio.sleep(0.1)

        result = await ref.ask(GetValue())
        assert result == 10


@pytest.mark.asyncio
async def test_replicated_actor_preserves_state_across_messages():
    """State is preserved across message processing"""

    @replicated(factor=1, write_quorum=1)
    @actor
    async def stateful(state: State[list], *, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            if msg == "get":
                await ctx.reply(state.value)
            else:
                state.set(state.value + [msg])

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(stateful([]), name="list")

        await ref.send("a")
        await ref.send("b")
        await ref.send("c")
        await asyncio.sleep(0.05)

        result = await ref.ask("get")
        assert result == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_non_replicated_actor_still_works():
    """Non-replicated actors should continue to work normally"""

    @actor
    async def echo(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            await ctx.reply(f"echo: {msg}")

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(echo(), name="echo")

        result = await ref.ask("hello")
        assert result == "echo: hello"


@pytest.mark.asyncio
async def test_replicated_actor_context_has_is_leader():
    """Context should have is_leader flag"""

    @replicated(factor=1, write_quorum=1)
    @actor
    async def leader_aware(state: State[str], *, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            if msg == "check":
                await ctx.reply(ctx.is_leader)

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(leader_aware(""), name="aware")

        result = await ref.ask("check")
        assert result is True
