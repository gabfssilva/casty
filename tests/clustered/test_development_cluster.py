import pytest
from dataclasses import dataclass

from casty import Actor


@dataclass
class Ping:
    pass


class PingActor(Actor[Ping]):
    def __init__(self):
        self.pings = 0

    async def receive(self, msg, ctx):
        match msg:
            case Ping():
                self.pings += 1
                await ctx.reply(self.pings)


class TestDevelopmentCluster:
    @pytest.mark.asyncio
    async def test_single_node(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(1) as cluster:
            assert cluster.node(0).node_id == "node-0"
            assert len(cluster) == 1

    @pytest.mark.asyncio
    async def test_multiple_nodes(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(2) as cluster:
            assert cluster.node(0).node_id == "node-0"
            assert cluster.node(1).node_id == "node-1"
            assert cluster.node("node-0").node_id == "node-0"
            assert cluster.node("node-1").node_id == "node-1"
            assert len(cluster) == 2

    @pytest.mark.asyncio
    async def test_custom_prefix(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(2, node_id_prefix="worker") as cluster:
            assert cluster[0].node_id == "worker-0"
            assert cluster[1].node_id == "worker-1"

    @pytest.mark.asyncio
    async def test_actor_clustered(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(1) as cluster:
            ref = await cluster.actor(PingActor, name="ping", scope="cluster")
            result = await ref.ask(Ping())
            assert result == 1

    @pytest.mark.asyncio
    async def test_iteration(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(3) as cluster:
            node_ids = [node.node_id for node in cluster]
            assert node_ids == ["node-0", "node-1", "node-2"]

    @pytest.mark.asyncio
    async def test_nodes_property(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(2) as cluster:
            nodes = cluster.nodes
            assert len(nodes) == 2
            assert nodes[0].node_id == "node-0"

    @pytest.mark.asyncio
    async def test_node_access_by_index_error(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(2) as cluster:
            with pytest.raises(IndexError):
                cluster.node(5)

    @pytest.mark.asyncio
    async def test_node_access_by_name_error(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(2) as cluster:
            with pytest.raises(KeyError):
                cluster.node("nonexistent")

    @pytest.mark.asyncio
    async def test_round_robin_strategy(self):
        from casty.cluster.development import DevelopmentCluster, DistributionStrategy

        async with DevelopmentCluster(3, strategy=DistributionStrategy.ROUND_ROBIN) as cluster:
            # Access _next_node multiple times to verify round-robin
            nodes = [cluster._next_node() for _ in range(6)]
            node_ids = [n.node_id for n in nodes]
            assert node_ids == ["node-0", "node-1", "node-2", "node-0", "node-1", "node-2"]
