import pytest
import asyncio
from dataclasses import dataclass

import msgpack

from casty import Actor, LocalSystem
from casty.cluster import (
    Cluster,
    ClusterConfig,
    ClusteredActorRef,
    RegisterClusteredActor,
    ClusteredSend,
    ClusteredAsk,
    GetClusteredActor,
)
from casty.persistent_actor import GetCurrentVersion


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


class TestActorIntegration:
    @pytest.mark.asyncio
    async def test_full_flow_single_node(self):
        async with LocalSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17970),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="my-counter",
                    actor_cls=Counter,
                    replication=1,
                    singleton=False,
                )
            )

            for i in range(5):
                payload = Increment(amount=10)
                payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
                payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

                await cluster.send(
                    ClusteredSend(
                        actor_id="my-counter",
                        request_id=f"req-{i}",
                        payload_type=payload_type,
                        payload=payload_bytes,
                        consistency=1,
                    )
                )

            await asyncio.sleep(0.2)

            get_payload = GetCount()
            get_type = f"{type(get_payload).__module__}.{type(get_payload).__qualname__}"
            get_bytes = msgpack.packb(get_payload.__dict__, use_bin_type=True)

            result = await cluster.ask(
                ClusteredAsk(
                    actor_id="my-counter",
                    request_id="final",
                    payload_type=get_type,
                    payload=get_bytes,
                    consistency=1,
                )
            )

            assert result == 50

    @pytest.mark.asyncio
    async def test_singleton_flag_preserved(self):
        async with LocalSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17971),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="singleton-counter",
                    actor_cls=Counter,
                    replication=1,
                    singleton=True,
                )
            )

            info = await cluster.ask(GetClusteredActor(actor_id="singleton-counter"))
            assert info is not None
            assert info.singleton is True

    @pytest.mark.asyncio
    async def test_version_tracked_across_operations(self):
        async with LocalSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17972),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="tracked-counter",
                    actor_cls=Counter,
                    replication=1,
                    singleton=False,
                )
            )

            for i in range(3):
                payload = Increment(amount=1)
                payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
                payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

                await cluster.send(
                    ClusteredSend(
                        actor_id="tracked-counter",
                        request_id=f"req-{i}",
                        payload_type=payload_type,
                        payload=payload_bytes,
                        consistency=1,
                    )
                )

            await asyncio.sleep(0.2)

            info = await cluster.ask(GetClusteredActor(actor_id="tracked-counter"))
            assert info is not None

            cluster_node = system._supervision_tree.get_node(cluster.id)
            cluster_instance = cluster_node.actor_instance
            local_ref = cluster_instance._local_actors.get("tracked-counter")
            version = await local_ref.ask(GetCurrentVersion())
            node_id = cluster_instance._node_id
            assert version.clock.get(node_id, 0) == 3

            get_payload = GetCount()
            get_type = f"{type(get_payload).__module__}.{type(get_payload).__qualname__}"
            get_bytes = msgpack.packb(get_payload.__dict__, use_bin_type=True)

            result = await cluster.ask(
                ClusteredAsk(
                    actor_id="tracked-counter",
                    request_id="check",
                    payload_type=get_type,
                    payload=get_bytes,
                    consistency=1,
                )
            )

            assert result == 3
