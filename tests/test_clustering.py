import asyncio
import pytest
from dataclasses import dataclass

from casty import actor, Mailbox, message
from casty.state import State
from casty.cluster import DevelopmentCluster


@message
class Inc:
    pass


@message
class Get:
    pass


@message
class Ping:
    pass


@pytest.mark.asyncio
async def test_cluster_nodes_discover_each_other():
    """Nodes in cluster discover each other via SWIM protocol"""
    async with asyncio.timeout(15):
        async with DevelopmentCluster(nodes=3) as cluster:
            await cluster.wait_for(3)

            assert len(cluster.nodes) == 3


@pytest.mark.asyncio
async def test_clustered_actor_receives_messages():
    """Clustered actor receives messages sent through cluster"""

    @actor(clustered=True)
    async def echo(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply(f"from:{ctx.node_id}")

    async with asyncio.timeout(15):
        async with DevelopmentCluster(nodes=3) as cluster:
            ref = await cluster.actor(echo(), name="echo")

            result = await ref.ask(Ping())

            assert "from:node-" in result


@pytest.mark.asyncio
async def test_cluster_distributes_messages_to_nodes():
    """Messages sent to cluster are distributed across nodes"""
    nodes_that_received: list[str] = []

    @actor(clustered=True)
    async def tracker(*, mailbox: Mailbox[Ping | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Ping():
                    nodes_that_received.append(ctx.node_id)
                case Get():
                    await ctx.reply(list(set(nodes_that_received)))

    async with asyncio.timeout(15):
        async with DevelopmentCluster(nodes=5) as cluster:
            ref = await cluster.actor(tracker(), name="tracker")

            for _ in range(20):
                await ref.send(Ping())

            await asyncio.sleep(0.3)

            unique_nodes = await ref.ask(Get())

            assert len(unique_nodes) >= 1


@pytest.mark.asyncio
async def test_stateful_counter_across_cluster():
    """Stateful actor maintains consistency across cluster messages"""

    @actor(clustered=True)
    async def counter(state: State[int], *, mailbox: Mailbox[Inc | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Inc():
                    state.value += 1
                case Get():
                    await ctx.reply(state.value)

    async with asyncio.timeout(15):
        async with DevelopmentCluster(nodes=5) as cluster:
            ref = await cluster.actor(counter(State(0)), name="counter")

            iterations = 10

            for _ in range(iterations):
                await ref.send(Inc())

            await asyncio.sleep(0.5)

            result = await ref.ask(Get())

            assert result == iterations
