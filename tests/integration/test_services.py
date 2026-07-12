from __future__ import annotations

import asyncio

import pytest

import casty
from casty.errors import CastyError
from tests.integration.actors import (
    FAST_CONFIG,
    Counter,
    Fetcher,
    kill_node,
    start_nodes,
    stop_all,
)

WIRE = "itest.Fetcher"


def _hosts(systems: list[casty.Node]) -> list[casty.Node]:
    return [n for n in systems if n._host.is_active(WIRE, "@")]


async def test_node_service_dispatches_locally() -> None:
    systems = await start_nodes(3)
    try:
        assert await systems[1].service(Fetcher).slow(0.0, 21) == 42
        assert _hosts(systems) == [systems[1]]  # zero hops: the caller hosts the coordinator
    finally:
        await stop_all(systems)


async def test_calls_overlap_instead_of_queueing() -> None:
    systems = await start_nodes(2)
    try:
        svc = systems[0].service(Fetcher)
        started = asyncio.get_running_loop().time()
        results = await asyncio.gather(*[svc.slow(0.4, n) for n in range(8)])
        elapsed = asyncio.get_running_loop().time() - started
        assert results == [0, 2, 4, 6, 8, 10, 12, 14]
        assert elapsed < 1.2  # serialized in a mailbox this would be 3.2s
    finally:
        await stop_all(systems)


async def test_service_reaches_a_replicated_actor() -> None:
    systems = await start_nodes(3)
    try:
        svc = systems[0].service(Fetcher)
        await asyncio.gather(*[svc.bump("sku:1", 1) for _ in range(10)])
        assert await systems[2].actor(Counter, "sku:1").read() == 10  # same key serializes
    finally:
        await stop_all(systems)


async def test_client_load_balances_across_members() -> None:
    systems = await start_nodes(3)
    client = await casty.connect([systems[0].member.addr], config=FAST_CONFIG)
    try:
        svc = client.service(Fetcher)
        await asyncio.gather(*[svc.slow(0.0, n) for n in range(9)])
        assert len(_hosts(systems)) == 3  # round-robin: every member served some calls
    finally:
        await client.close()
        await stop_all(systems)


async def test_in_flight_calls_fail_when_the_host_dies() -> None:
    systems = await start_nodes(3)
    client = await casty.connect([systems[0].member.addr], config=FAST_CONFIG)
    victim: casty.Node | None = None
    try:
        call = asyncio.create_task(client.service(Fetcher).slow(30.0, 1))
        await asyncio.sleep(0.5)
        hosts = _hosts(systems)
        assert len(hosts) == 1
        victim = hosts[0]
        await kill_node(victim)
        with pytest.raises(CastyError):  # typed error, not a hang
            await call
    finally:
        await client.close()
        await stop_all([n for n in systems if n is not victim])


async def test_failure_surfaces_as_actor_failed() -> None:
    systems = await start_nodes(2)
    client = await casty.connect([systems[0].member.addr], config=FAST_CONFIG)
    try:
        with pytest.raises(casty.ActorFailedError, match="kaboom"):
            await client.service(Fetcher).boom()
    finally:
        await client.close()
        await stop_all(systems)
