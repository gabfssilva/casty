import pytest
from dataclasses import dataclass

from casty import Actor, LocalActorRef
from casty.cluster import ClusteredActorRef
from casty.cluster.clustered_system import ClusteredSystem
from casty.cluster.config import ClusterConfig
from casty.cluster.scope import ClusterScope


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


class Counter(Actor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg, ctx):
        match msg:
            case Increment(amount=amount):
                self.count += amount
            case GetCount():
                await ctx.reply(self.count)


class TestClusteredActorSystem:
    @pytest.mark.asyncio
    async def test_factory_returns_clustered_system(self):
        system = ClusteredSystem(ClusterConfig(bind_port=18000))
        assert isinstance(system, ClusteredSystem)

    @pytest.mark.asyncio
    async def test_spawn_local_actor(self):
        async with ClusteredSystem(ClusterConfig(bind_port=18000)) as system:
            ref = await system.actor(Counter, name="counter")
            assert isinstance(ref, LocalActorRef)

    @pytest.mark.asyncio
    async def test_spawn_clustered_actor(self):
        async with ClusteredSystem(ClusterConfig(bind_port=18001)) as system:
            ref = await system.actor(Counter, name="counter", scope='cluster')
            assert isinstance(ref, ClusteredActorRef)

    @pytest.mark.asyncio
    async def test_clustered_actor_has_local_ref(self):
        async with ClusteredSystem(ClusterConfig(bind_port=18002)) as system:
            ref = await system.actor(Counter, name="counter", scope=ClusterScope(replication=1))
            assert ref.local_ref is not None

    @pytest.mark.asyncio
    async def test_clustered_send_and_ask(self):
        async with ClusteredSystem(ClusterConfig(bind_port=18003)) as system:
            ref = await system.actor(Counter, name="counter", scope='cluster')

            await ref.send(Increment(10))
            await ref.send(Increment(5))

            result = await ref.ask(GetCount())
            assert result == 15

    @pytest.mark.asyncio
    async def test_node_id_property(self):
        async with ClusteredSystem(ClusterConfig(bind_port=18004, node_id="my-node")) as system:
            assert system.node_id == "my-node"
