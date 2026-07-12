from __future__ import annotations

import asyncio

import pytest

import casty
from tests.integration.actors import FAST_CONFIG, start_nodes, stop_all

pytestmark = pytest.mark.asyncio


async def test_lock_mutual_exclusion_across_nodes_and_client() -> None:
    nodes = await start_nodes(3)
    client: casty.Client | None = None
    try:
        lock: casty.Lock = nodes[0].lock("mutex")
        assert await lock.locked() is False

        held = await lock.try_lock()
        assert held is not None
        assert await lock.locked() is True

        # no other holder gets in, from any node or a lite member
        assert await nodes[1].lock("mutex").try_lock() is None
        client = await casty.connect([nodes[0].member.addr], config=FAST_CONFIG)
        assert await client.lock("mutex").try_lock() is None

        assert await held.release() is True
        assert await nodes[1].lock("mutex").locked() is False
    finally:
        if client is not None:
            await client.close()
        await stop_all(nodes)


async def test_lock_context_manager_releases_on_exit() -> None:
    nodes = await start_nodes(3)
    try:
        async with nodes[0].lock("scoped", timeout=1.0) as held:
            assert isinstance(held.token, int)
            assert await nodes[1].lock("scoped").locked() is True
        assert await nodes[0].lock("scoped").locked() is False
    finally:
        await stop_all(nodes)


async def test_lock_acquire_times_out_while_held() -> None:
    nodes = await start_nodes(3)
    try:
        lock: casty.Lock = nodes[0].lock("contended")
        held = await lock.acquire()

        with pytest.raises(casty.CastyTimeoutError):
            await nodes[1].lock("contended").acquire(timeout=0.3)

        # once released, a waiter succeeds with a greater fencing token
        waiter = asyncio.create_task(nodes[1].lock("contended").acquire(timeout=5.0))
        await asyncio.sleep(0.2)
        await held.release()
        acquired = await waiter
        assert acquired.token > held.token
    finally:
        await stop_all(nodes)
