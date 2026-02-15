"""Distributed data structures on multi-node in-process clusters."""
from __future__ import annotations

import asyncio

from casty.distributed import Distributed
from casty.cluster.system import ClusteredActorSystem


async def test_distributed_counter_two_nodes() -> None:
    """Counter via Distributed facade on 2-node cluster."""
    async with ClusteredActorSystem(
        name="ddata2",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system_a:
        port_a = system_a.self_node.port

        async with ClusteredActorSystem(
            name="ddata2",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
            seed_nodes=(("127.0.0.1", port_a),),
        ) as system_b:
            await system_a.wait_for(2, timeout=10.0)

            d1 = system_a.distributed()
            d2 = system_b.distributed()
            await asyncio.sleep(0.5)

            counter_a = d1.counter("hits", shards=10)
            val = await counter_a.increment(5)
            assert val == 5

            counter_b = d2.counter("hits", shards=10)
            val2 = await counter_b.get()
            assert val2 == 5


async def test_distributed_counter_five_nodes() -> None:
    """Counter on 5-node cluster: all nodes spawn the same regions."""
    systems: list[ClusteredActorSystem] = []
    base_port = 26540

    try:
        for idx in range(5):
            port = base_port + idx
            seeds = tuple(("127.0.0.1", base_port + j) for j in range(5) if j != idx)
            s = ClusteredActorSystem(
                name="ddata5",
                host="127.0.0.1",
                port=port,
                node_id=f"node-{idx + 1}",
                seed_nodes=seeds,
            )
            await s.__aenter__()
            systems.append(s)

        await systems[0].wait_for(5, timeout=10.0)

        dists = [Distributed(s) for s in systems]
        await asyncio.sleep(0.5)

        views = dists[0].counter("page-views", shards=50)
        likes = dists[2].counter("likes", shards=50)

        await views.increment(100)
        await views.increment(50)
        await likes.increment(42)

        views_from_5 = dists[4].counter("page-views", shards=50)
        assert await views_from_5.get() == 150

        likes_from_1 = dists[0].counter("likes", shards=50)
        assert await likes_from_1.get() == 42
    finally:
        for s in reversed(systems):
            await s.__aexit__(None, None, None)
