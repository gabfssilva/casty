import pytest
import asyncio

from casty import ActorSystem
from casty.cluster.router import router_actor, RegisterPending
from casty.cluster.transport_messages import RegisterReplication
from casty.cluster.replication import ReplicationConfig, Routing
from casty.cluster.messages import MarkDown


@pytest.mark.asyncio
async def test_router_handles_mark_down():
    async with ActorSystem() as system:
        router = await system.actor(router_actor(), name="router")

        config = ReplicationConfig(factor=2, write_quorum=1, routing=Routing.LEADER)
        await router.send(RegisterReplication(
            actor_id="counter/main",
            config=config,
        ))

        await router.send(MarkDown(node_id="node-1"))

        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_router_logs_leader_down(caplog):
    import logging

    caplog.set_level(logging.WARNING)

    async with ActorSystem() as system:
        router = await system.actor(router_actor(), name="router")

        config = ReplicationConfig(factor=2, write_quorum=1, routing=Routing.LEADER)
        await router.send(RegisterReplication(
            actor_id="test/actor",
            config=config,
        ))

        await router.send(MarkDown(node_id="leader-node"))

        await asyncio.sleep(0.01)
