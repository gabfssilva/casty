# tests/clustered/test_state_replication.py
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
async def test_state_replicated_after_mutation():
    @replicated(factor=2, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def counter(state: State[int], mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Set(value):
                    state.set(value)

    async with DevelopmentCluster(nodes=2) as cluster:
        ref = await cluster.nodes[0].actor(counter(0), name="test")

        # Mutate state
        await ref.send(Set(100))
        await asyncio.sleep(0.1)

        # Verify state was updated
        result = await ref.ask(Get())
        assert result == 100


@pytest.mark.asyncio
async def test_state_change_detection():
    changes_detected: list[int] = []

    @replicated(factor=2, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def tracker(state: State[int], mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Set(value):
                    old = state.value
                    state.set(value)
                    if state.changed():
                        changes_detected.append(state.version)

    async with DevelopmentCluster(nodes=2) as cluster:
        ref = await cluster.nodes[0].actor(tracker(0), name="tracker")

        await ref.send(Set(10))
        await ref.send(Set(20))
        await ref.send(Set(30))
        await asyncio.sleep(0.1)

        result = await ref.ask(Get())
        assert result == 30
        assert len(changes_detected) == 3
        assert changes_detected == [1, 2, 3]
