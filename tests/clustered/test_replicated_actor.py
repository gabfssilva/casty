import pytest

from casty import actor, Mailbox, message, State
from casty.cluster.replication import replicated, Routing
from casty.cluster import DevelopmentCluster


@message(readonly=True)
class Get:
    pass


@message
class Increment:
    amount: int


@pytest.mark.asyncio
async def test_replicated_actor_creation():
    @replicated(factor=2, write_quorum=2, routing=Routing.LEADER)
    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Increment(amount):
                    state.set(state.value + amount)

    async with DevelopmentCluster(nodes=3) as cluster:
        node = cluster.nodes[0]
        ref = await node.actor(counter(0), name="main")

        result = await ref.ask(Get())
        assert result == 0

        await ref.send(Increment(5))
        result = await ref.ask(Get())
        assert result == 5


@pytest.mark.asyncio
async def test_replicated_actor_with_override():
    @replicated(factor=2, write_quorum=1)
    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Increment(amount):
                    state.set(state.value + amount)

    async with DevelopmentCluster(nodes=3) as cluster:
        node = cluster.nodes[0]
        ref = await node.actor(counter(0), name="main", replicas=3)

        result = await ref.ask(Get())
        assert result == 0


@pytest.mark.asyncio
async def test_non_replicated_actor_with_replicas_param():
    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Increment(amount):
                    state.set(state.value + amount)

    async with DevelopmentCluster(nodes=3) as cluster:
        node = cluster.nodes[0]
        ref = await node.actor(
            counter(0),
            name="main",
            replicas=2,
            write_quorum=1,
            routing=Routing.ANY,
        )

        result = await ref.ask(Get())
        assert result == 0

        await ref.send(Increment(10))
        result = await ref.ask(Get())
        assert result == 10


@pytest.mark.asyncio
async def test_replicated_decorator_config_preserved():
    @replicated(factor=5, write_quorum=3, routing=Routing.LOCAL_FIRST)
    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Increment(amount):
                    state.set(state.value + amount)

    assert hasattr(counter, "__replication__")
    config = counter.__replication__
    assert config.factor == 5
    assert config.write_quorum == 3
    assert config.routing == Routing.LOCAL_FIRST

    async with DevelopmentCluster(nodes=3) as cluster:
        node = cluster.nodes[0]
        ref = await node.actor(counter(0), name="main")

        result = await ref.ask(Get())
        assert result == 0
