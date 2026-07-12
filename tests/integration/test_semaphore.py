from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import casty
from casty.errors import QuorumUnavailableError
from tests.integration.actors import (
    FAST_CONFIG,
    kill_node,
    owner_of,
    partition,
    start_nodes,
    stop_all,
    wait_view,
)

pytestmark = pytest.mark.asyncio


async def test_semaphore_full_api_across_nodes_and_client() -> None:
    systems = await start_nodes(3)
    client: casty.Client | None = None
    try:
        sem: casty.Semaphore = systems[0].semaphore("gate", capacity=2)
        assert await sem.available() == 2

        a = await sem.try_acquire()
        b = await sem.try_acquire()
        assert a is not None and b is not None
        assert a.token != b.token
        assert await sem.available() == 0

        # bound reached: same semaphore seen from another system also refuses
        sem_b: casty.Semaphore = systems[1].semaphore("gate", capacity=2)
        assert await sem_b.try_acquire() is None

        assert await a.release() is True
        assert await sem_b.available() == 1

        # a fresh grant gets a strictly greater fencing token
        c = await sem_b.try_acquire()
        assert c is not None and c.token > b.token

        # and from a lite member
        client = await casty.connect([systems[0].member.addr], config=FAST_CONFIG)
        sem_c: casty.Semaphore = client.semaphore("gate", capacity=2)
        assert await sem_c.available() == 0
        assert await sem_c.try_acquire() is None
    finally:
        if client is not None:
            await client.close()
        await stop_all(systems)


async def test_semaphore_capacity_bound_under_contention() -> None:
    systems = await start_nodes(3)
    try:
        sem: casty.Semaphore = systems[0].semaphore("bounded", capacity=3)
        grants = await asyncio.gather(*(sem.try_acquire() for _ in range(10)))
        held = [g for g in grants if g is not None]
        assert len(held) == 3
        assert len({g.token for g in held}) == 3
        assert await sem.available() == 0
    finally:
        await stop_all(systems)


async def test_semaphore_lease_expiry_reclaims_permit() -> None:
    systems = await start_nodes(3)
    try:
        sem: casty.Semaphore = systems[0].semaphore("leased", capacity=1)
        lease = await sem.acquire(ttl=0.2)
        assert await sem.available() == 0

        await asyncio.sleep(0.4)
        assert await sem.available() == 1  # reclaimed without release
        assert await lease.renew(1.0) is False  # the expired lease is gone
        assert await lease.release() is False
    finally:
        await stop_all(systems)


async def test_semaphore_acquire_blocks_until_release() -> None:
    systems = await start_nodes(3)
    try:
        sem: casty.Semaphore = systems[0].semaphore("blocking", capacity=1)
        held = await sem.acquire(ttl=30.0)

        waiter = asyncio.create_task(sem.acquire(ttl=30.0, timeout=5.0))
        await asyncio.sleep(0.2)
        assert not waiter.done()

        await held.release()
        second = await waiter
        assert second.token > held.token

        with pytest.raises(casty.CastyTimeoutError):
            await sem.acquire(timeout=0.3)
    finally:
        await stop_all(systems)


async def test_semaphore_survives_owner_death() -> None:
    systems = await start_nodes(3)
    try:
        info = systems[0].semaphore("durable", capacity=2)._info
        owner = owner_of(systems, info.wire_name, "durable:0")
        survivor = next(n for n in systems if n is not owner)

        sem: casty.Semaphore = survivor.semaphore("durable", capacity=2)
        lease = await sem.acquire()
        assert await sem.available() == 1

        await kill_node(owner)
        await wait_view([n for n in systems if n is not owner], 2)

        deadline = asyncio.get_running_loop().time() + 10.0
        while True:
            try:
                assert await sem.available() == 1  # lease survived failover
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.1)

        assert await lease.renew(30.0) is True
        assert await lease.release() is True
        assert await sem.available() == 2
    finally:
        await stop_all(systems)


async def test_semaphore_acquire_fenced_in_minority() -> None:
    systems = await start_nodes(3)
    heal: Callable[[], None] | None = None
    try:
        info = systems[0].semaphore("fenced", capacity=1)._info
        owner = owner_of(systems, info.wire_name, "fenced:0")
        majority = [n for n in systems if n is not owner]

        heal = await partition([owner], majority)
        await wait_view(majority, 2)

        owner_sem: casty.Semaphore = owner.semaphore("fenced", capacity=1)
        with pytest.raises(QuorumUnavailableError):
            await owner_sem.try_acquire()

        majority_sem: casty.Semaphore = majority[0].semaphore("fenced", capacity=1)
        lease = await majority_sem.try_acquire()
        assert lease is not None
    finally:
        if heal is not None:
            heal()
        await stop_all(systems)
