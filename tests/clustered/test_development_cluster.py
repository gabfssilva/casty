import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox
from casty.serializable import serializable
from casty.cluster import DevelopmentCluster


@serializable
@dataclass
class Ping:
    value: int


@serializable
@dataclass
class Pong:
    value: int


@actor
async def pong_actor(*, mailbox: Mailbox[Ping]):
    async for msg, ctx in mailbox:
        await ctx.reply(Pong(msg.value * 2))


@serializable
@dataclass
class Inc:
    amount: int


@serializable
@dataclass
class Get:
    pass


@actor
async def counter(initial: int, *, mailbox: Mailbox[Inc | Get]):
    count = initial
    async for msg, ctx in mailbox:
        match msg:
            case Inc(amount):
                count += amount
            case Get():
                await ctx.reply(count)


@pytest.mark.asyncio
async def test_development_cluster_basic():
    async with DevelopmentCluster(3) as cluster:
        assert len(cluster) == 3

        node0 = cluster[0]
        assert node0.node_id == "node-0"
        assert node0.port > 0


@pytest.mark.asyncio
async def test_development_cluster_two_nodes():
    async with DevelopmentCluster(2) as cluster:
        assert len(cluster) == 2

        node0 = cluster[0]
        node1 = cluster[1]

        assert node0.node_id == "node-0"
        assert node1.node_id == "node-1"
        assert node0.port != node1.port


@pytest.mark.asyncio
async def test_development_cluster_local_actor():
    async with DevelopmentCluster(2) as cluster:
        pong_ref = await cluster[0].actor(pong_actor(), name="pong")

        result = await pong_ref.ask(Ping(10))
        assert result == Pong(20)


@pytest.mark.asyncio
async def test_development_cluster_counter_actor():
    async with DevelopmentCluster(2) as cluster:
        ref = await cluster[0].actor(counter(0), name="counter")

        await ref.send(Inc(5))
        await asyncio.sleep(0.05)
        result = await ref.ask(Get())

        assert result == 5


@pytest.mark.asyncio
async def test_development_cluster_actors_on_different_nodes():
    async with DevelopmentCluster(3) as cluster:
        ref0 = await cluster[0].actor(counter(0), name="counter0")
        ref1 = await cluster[1].actor(counter(100), name="counter1")
        ref2 = await cluster[2].actor(counter(200), name="counter2")

        await ref0.send(Inc(10))
        await ref1.send(Inc(20))
        await ref2.send(Inc(30))

        await asyncio.sleep(0.05)

        result0 = await ref0.ask(Get())
        result1 = await ref1.ask(Get())
        result2 = await ref2.ask(Get())

        assert result0 == 10
        assert result1 == 120
        assert result2 == 230


@pytest.mark.asyncio
async def test_development_cluster_iteration():
    async with DevelopmentCluster(3) as cluster:
        node_ids = [node.node_id for node in cluster]
        assert node_ids == ["node-0", "node-1", "node-2"]


@pytest.mark.asyncio
async def test_development_cluster_nodes_property():
    async with DevelopmentCluster(2) as cluster:
        nodes = cluster.nodes
        assert len(nodes) == 2
        assert nodes[0].node_id == "node-0"
        assert nodes[1].node_id == "node-1"


@pytest.mark.asyncio
async def test_development_cluster_single_node():
    async with DevelopmentCluster(1) as cluster:
        assert len(cluster) == 1
        assert cluster[0].node_id == "node-0"

        ref = await cluster[0].actor(counter(0), name="counter")
        await ref.send(Inc(42))
        await asyncio.sleep(0.05)
        result = await ref.ask(Get())
        assert result == 42


@pytest.mark.asyncio
async def test_development_cluster_address_property():
    async with DevelopmentCluster(2) as cluster:
        assert cluster[0].address.startswith("127.0.0.1:")
        assert cluster[1].address.startswith("127.0.0.1:")
        assert cluster[0].address != cluster[1].address
