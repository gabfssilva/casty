from __future__ import annotations

import pytest

from casty import ActorSystem, actor, Mailbox
from casty.cluster.router import router_actor, RegisterPending
from casty.cluster.transport_messages import Register, RegisterReplication
from casty.cluster.replication import ReplicationConfig, Routing


@pytest.mark.asyncio
async def test_router_accepts_replication_config():
    @actor
    async def dummy(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass

    async with ActorSystem() as system:
        router = await system.actor(router_actor(), name="router")

        config = ReplicationConfig(factor=3, write_quorum=2, routing=Routing.LEADER)
        await router.send(RegisterReplication(
            actor_id="counter/main",
            config=config,
        ))
