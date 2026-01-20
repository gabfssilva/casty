# tests/clustered/test_failover.py
import pytest
import asyncio
from casty import actor, Mailbox, message
from casty.state import State
from casty.cluster.replication import replicated, Routing
from casty.cluster import DevelopmentCluster


@message(readonly=True)
class Get:
    pass


@message
class Set:
    value: int


@pytest.mark.asyncio
async def test_failover_on_leader_down():
    @replicated(factor=2, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def counter(state: State[int], mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Set(value):
                    state.set(value)

    async with DevelopmentCluster(nodes=3) as cluster:
        ref = await cluster.nodes[0].actor(counter(0), name="failover-test")

        # Set initial value
        await ref.send(Set(42))
        result = await ref.ask(Get())
        assert result == 42

        # TODO: Simulate leader failure
        # TODO: Verify replica takes over


@pytest.mark.asyncio
async def test_basic_replication_setup():
    @replicated(factor=2, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def storage(state: State[int], mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Set(value):
                    state.set(value)

    async with DevelopmentCluster(nodes=2) as cluster:
        ref = await cluster.nodes[0].actor(storage(0), name="replicated")

        await ref.send(Set(100))
        await asyncio.sleep(0.1)

        result = await ref.ask(Get())
        assert result == 100
