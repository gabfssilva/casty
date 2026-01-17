import pytest
import asyncio
from dataclasses import dataclass

import msgpack

from casty import Actor, ActorSystem
from casty.cluster import Cluster, ClusterConfig
from casty.cluster.messages import (
    RegisterClusteredActor,
    ClusteredSend,
    GetClusteredActor,
)
from casty.persistent_actor import GetCurrentVersion
from casty.wal import VectorClock


@dataclass
class SetValue:
    value: int


class ValueActor(Actor[SetValue]):
    def __init__(self):
        self.value = 0

    async def receive(self, msg, ctx):
        match msg:
            case SetValue(value=value):
                self.value = value


@dataclass
class ReadOnly:
    pass


class ReadOnlyActor(Actor[ReadOnly]):
    def __init__(self):
        self.value = 42

    async def receive(self, msg, ctx):
        pass


class TestReplication:
    @pytest.mark.asyncio
    async def test_state_change_triggers_version_increment(self):
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17960),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="value-actor",
                    actor_cls=ValueActor,
                    replication=1,
                    singleton=False,
                )
            )

            payload = SetValue(value=42)
            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

            await cluster.send(
                ClusteredSend(
                    actor_id="value-actor",
                    request_id="req-1",
                    payload_type=payload_type,
                    payload=payload_bytes,
                    consistency=1,
                )
            )

            await asyncio.sleep(0.1)

            info = await cluster.ask(GetClusteredActor(actor_id="value-actor"))
            assert info is not None

            cluster_node = system._supervision_tree.get_node(cluster.id)
            cluster_instance = cluster_node.actor_instance
            local_ref = cluster_instance._local_actors.get("value-actor")
            version = await local_ref.ask(GetCurrentVersion())
            assert version != VectorClock()

    @pytest.mark.asyncio
    async def test_no_version_increment_if_state_unchanged(self):
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17961),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="readonly-actor",
                    actor_cls=ReadOnlyActor,
                    replication=1,
                    singleton=False,
                )
            )

            await asyncio.sleep(0.1)

            cluster_node = system._supervision_tree.get_node(cluster.id)
            cluster_instance = cluster_node.actor_instance
            local_ref = cluster_instance._local_actors.get("readonly-actor")
            version_before = await local_ref.ask(GetCurrentVersion())

            payload = ReadOnly()
            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

            await cluster.send(
                ClusteredSend(
                    actor_id="readonly-actor",
                    request_id="req-1",
                    payload_type=payload_type,
                    payload=payload_bytes,
                    consistency=1,
                )
            )

            await asyncio.sleep(0.1)

            info = await cluster.ask(GetClusteredActor(actor_id="readonly-actor"))
            assert info is not None

            version_after = await local_ref.ask(GetCurrentVersion())
            assert version_before == version_after

    @pytest.mark.asyncio
    async def test_multiple_state_changes_increment_version(self):
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17962),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="counter",
                    actor_cls=ValueActor,
                    replication=1,
                    singleton=False,
                )
            )

            for i in range(1, 6):
                payload = SetValue(value=i)
                payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
                payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

                await cluster.send(
                    ClusteredSend(
                        actor_id="counter",
                        request_id=f"req-{i}",
                        payload_type=payload_type,
                        payload=payload_bytes,
                        consistency=1,
                    )
                )

            await asyncio.sleep(0.2)

            info = await cluster.ask(GetClusteredActor(actor_id="counter"))
            assert info is not None

            cluster_node = system._supervision_tree.get_node(cluster.id)
            cluster_instance = cluster_node.actor_instance
            local_ref = cluster_instance._local_actors.get("counter")
            version = await local_ref.ask(GetCurrentVersion())
            node_id = cluster_instance._node_id
            assert version.clock.get(node_id, 0) == 5
