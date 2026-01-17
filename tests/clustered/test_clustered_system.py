import pytest
from dataclasses import dataclass

from casty.cluster.clustered_actor import ClusteredActor


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


class Counter(ClusteredActor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg, ctx):
        match msg:
            case Increment(amount=amount):
                self.count += amount
            case GetCount():
                ctx.reply(self.count)


class TestClusteredActorSystem:
    @pytest.mark.asyncio
    async def test_inherits_from_actor_system(self):
        from casty import ActorSystem
        from casty.cluster.clustered_system import ClusteredActorSystem

        assert issubclass(ClusteredActorSystem, ActorSystem)

    @pytest.mark.asyncio
    async def test_spawn_local_actor(self):
        from casty.cluster.clustered_system import ClusteredActorSystem
        from casty.cluster import ClusterConfig
        from casty import LocalRef

        async with ClusteredActorSystem(ClusterConfig(bind_port=18000)) as system:
            ref = await system.spawn(Counter)
            assert isinstance(ref, LocalRef)

    @pytest.mark.asyncio
    async def test_spawn_clustered_actor(self):
        from casty.cluster.clustered_system import ClusteredActorSystem
        from casty.cluster import ClusterConfig, ClusteredRef

        async with ClusteredActorSystem(ClusterConfig(bind_port=18001)) as system:
            ref = await system.spawn(Counter, clustered=True)
            assert isinstance(ref, ClusteredRef)

    @pytest.mark.asyncio
    async def test_clustered_actor_has_local_ref(self):
        from casty.cluster.clustered_system import ClusteredActorSystem
        from casty.cluster import ClusterConfig

        async with ClusteredActorSystem(ClusterConfig(bind_port=18002)) as system:
            ref = await system.spawn(Counter, clustered=True, replication=1)
            assert ref.local_ref is not None

    @pytest.mark.asyncio
    async def test_clustered_send_and_ask(self):
        from casty.cluster.clustered_system import ClusteredActorSystem
        from casty.cluster import ClusterConfig

        async with ClusteredActorSystem(ClusterConfig(bind_port=18003)) as system:
            ref = await system.spawn(Counter, clustered=True)

            await ref.send(Increment(10))
            await ref.send(Increment(5))

            result = await ref.ask(GetCount())
            assert result == 15

    @pytest.mark.asyncio
    async def test_node_id_property(self):
        from casty.cluster.clustered_system import ClusteredActorSystem
        from casty.cluster import ClusterConfig

        config = ClusterConfig(bind_port=18004, node_id="my-node")
        async with ClusteredActorSystem(config) as system:
            assert system.node_id == "my-node"
