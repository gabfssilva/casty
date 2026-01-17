import pytest
import asyncio
from dataclasses import dataclass

import msgpack

from casty import ActorSystem
from casty.cluster import Cluster, ClusterConfig
from casty.cluster.clustered_actor import ClusteredActor
from casty.cluster.messages import (
    RegisterClusteredActor,
    ClusteredSend,
    GetClusteredActor,
)


@dataclass
class SetValue:
    value: int


class ValueActor(ClusteredActor[SetValue]):
    def __init__(self):
        self.value = 0

    async def receive(self, msg, ctx):
        match msg:
            case SetValue(value=value):
                self.value = value


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
            assert info.version == 1

    @pytest.mark.asyncio
    async def test_no_version_increment_if_state_unchanged(self):
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17961),
            )

            @dataclass
            class ReadOnly:
                pass

            class ReadOnlyActor(ClusteredActor[ReadOnly]):
                def __init__(self):
                    self.value = 42

                async def receive(self, msg, ctx):
                    pass

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="readonly-actor",
                    actor_cls=ReadOnlyActor,
                    replication=1,
                    singleton=False,
                )
            )

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
            assert info.version == 0

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
            assert info.version == 5
