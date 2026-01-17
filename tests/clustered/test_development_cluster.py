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
                ctx.reply(self.pings)


class TestDevelopmentCluster:
    @pytest.mark.asyncio
    async def test_single_node(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(1) as (system,):
            assert system.node_id == "node-0"

    @pytest.mark.asyncio
    async def test_multiple_nodes(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(2) as (sys1, sys2):
            assert sys1.node_id == "node-0"
            assert sys2.node_id == "node-1"

    @pytest.mark.asyncio
    async def test_custom_prefix(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(2, node_id_prefix="worker") as (w1, w2):
            assert w1.node_id == "worker-0"
            assert w2.node_id == "worker-1"

    @pytest.mark.asyncio
    async def test_spawn_clustered_actor(self):
        from casty.cluster.development import DevelopmentCluster

        async with DevelopmentCluster(1) as (system,):
            ref = await system.spawn(PingActor, clustered=True)
            result = await ref.ask(Ping())
            assert result == 1
