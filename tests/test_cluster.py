# tests/test_cluster.py
from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty.cluster import Cluster, ClusterConfig
from casty.cluster_state import MemberStatus


async def test_cluster_single_node_becomes_leader() -> None:
    """A single-node cluster should mark itself as Up and become leader."""
    config = ClusterConfig(host="127.0.0.1", port=0, seed_nodes=[])

    async with ActorSystem(name="test") as system:
        cluster = Cluster(system=system, config=config)
        await cluster.start()
        await asyncio.sleep(0.3)

        state = await cluster.get_state(timeout=2.0)
        assert len(state.members) == 1
        member = next(iter(state.members))
        assert member.status == MemberStatus.up
        assert state.leader is not None

        await cluster.shutdown()
