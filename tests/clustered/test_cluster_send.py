import pytest
import asyncio
from dataclasses import dataclass

import msgpack

from casty import Actor, ActorSystem
from casty.cluster import Cluster, ClusterConfig
from casty.cluster.messages import RegisterClusteredActor, ClusteredSend, ClusteredAsk


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


class CounterActor(Actor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg, ctx):
        match msg:
            case Increment(amount=amount):
                self.count += amount
            case GetCount():
                await ctx.reply(self.count)


class TestClusterSend:
    @pytest.mark.asyncio
    async def test_send_to_local_actor(self):
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17950),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="counter",
                    actor_cls=CounterActor,
                    replication=1,
                    singleton=False,
                )
            )

            payload = Increment(amount=5)
            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

            await cluster.send(
                ClusteredSend(
                    actor_id="counter",
                    request_id="req-1",
                    payload_type=payload_type,
                    payload=payload_bytes,
                    consistency=1,
                )
            )

            await asyncio.sleep(0.1)

            get_payload = GetCount()
            get_type = f"{type(get_payload).__module__}.{type(get_payload).__qualname__}"
            get_bytes = msgpack.packb(get_payload.__dict__, use_bin_type=True)

            result = await cluster.ask(
                ClusteredAsk(
                    actor_id="counter",
                    request_id="req-1",
                    payload_type=get_type,
                    payload=get_bytes,
                    consistency=1,
                )
            )

            assert result == 5

    @pytest.mark.asyncio
    async def test_ask_returns_value(self):
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig(bind_port=17951),
            )

            await cluster.send(
                RegisterClusteredActor(
                    actor_id="counter",
                    actor_cls=CounterActor,
                    replication=1,
                    singleton=False,
                )
            )

            get_payload = GetCount()
            get_type = f"{type(get_payload).__module__}.{type(get_payload).__qualname__}"
            get_bytes = msgpack.packb(get_payload.__dict__, use_bin_type=True)

            result = await cluster.ask(
                ClusteredAsk(
                    actor_id="counter",
                    request_id="req-1",
                    payload_type=get_type,
                    payload=get_bytes,
                    consistency=1,
                )
            )

            assert result == 0
