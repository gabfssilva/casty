import pytest
from dataclasses import dataclass

from casty import Actor
from casty.cluster import DevelopmentCluster


@dataclass
class Ping:
    pass


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


class PingActor(Actor[Ping]):
    def __init__(self):
        self.pings = 0

    async def receive(self, msg, ctx):
        match msg:
            case Ping():
                self.pings += 1
                await ctx.reply(self.pings)


class Counter(Actor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg, ctx):
        match msg:
            case Increment(amount=amount):
                self.count += amount
            case GetCount():
                await ctx.reply(self.count)


class TestMultiNodeCluster:
    @pytest.mark.asyncio
    async def test_single_node_clustered_actor(self):
        async with DevelopmentCluster(1) as (node,):
            ref = await node.spawn(PingActor, clustered=True)

            result = await ref.ask(Ping())
            assert result == 1

            result = await ref.ask(Ping())
            assert result == 2

    @pytest.mark.asyncio
    async def test_single_node_local_ref_optimization(self):
        async with DevelopmentCluster(1) as (node,):
            ref = await node.spawn(Counter, clustered=True)

            assert ref.local_ref is not None

            await ref.send(Increment(100))
            result = await ref.ask(GetCount())
            assert result == 100

    @pytest.mark.asyncio
    async def test_single_node_multiple_actors(self):
        async with DevelopmentCluster(1) as (node,):
            counter1 = await node.spawn(Counter, clustered=True)
            counter2 = await node.spawn(Counter, clustered=True)

            await counter1.send(Increment(10))
            await counter2.send(Increment(20))

            r1 = await counter1.ask(GetCount())
            r2 = await counter2.ask(GetCount())

            assert r1 == 10
            assert r2 == 20

    @pytest.mark.asyncio
    async def test_custom_node_prefix(self):
        async with DevelopmentCluster(1, node_id_prefix="worker") as (w,):
            assert w.node_id == "worker-0"
