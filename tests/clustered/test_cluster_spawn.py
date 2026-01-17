import pytest
from dataclasses import dataclass

from casty import ActorSystem
from casty.cluster import Cluster, ClusterConfig
from casty.cluster.clustered_actor import ClusteredActor
from casty.cluster.messages import RegisterClusteredActor, GetClusteredActor


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


class CounterActor(ClusteredActor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg, ctx):
        match msg:
            case Increment(amount=amount):
                self.count += amount
            case GetCount():
                ctx.reply(self.count)


class TestClusterActorRegistry:
    @pytest.mark.asyncio
    async def test_register_clustered_actor(self):
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17946),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="counter-1",
                    actor_cls=CounterActor,
                    replication=1,
                    singleton=False,
                )
            )

            result = await cluster.ask(GetClusteredActor(actor_id="counter-1"))
            assert result is not None

    @pytest.mark.asyncio
    async def test_spawn_creates_local_actor(self):
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17947),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="counter-2",
                    actor_cls=CounterActor,
                    replication=1,
                    singleton=False,
                )
            )

            result = await cluster.ask(GetClusteredActor(actor_id="counter-2"))
            assert result is not None
