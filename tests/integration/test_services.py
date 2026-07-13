from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest

import casty
from casty.actors.registry import register_service
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
STREAMY_WIRE = "itest.Streamy"


class _Streamy:
    """Streaming service registered by hand, SpyAgent-style: the `@casty.service`
    builder rejects streaming, but the host serves it — which is what pinned
    dispatch must route."""

    async def count(self, n: int) -> AsyncIterator[int]:
        for i in range(n):
            yield i

    async def total(self, xs: AsyncIterator[int]) -> int:
        total = 0
        async for x in xs:
            total += x
        return total

    async def double(self, xs: AsyncIterator[int]) -> AsyncIterator[int]:
        async for x in xs:
            yield x * 2


Streamy: type = register_service(_Streamy, name=STREAMY_WIRE)


async def _ints(items: list[int]) -> AsyncIterator[int]:
    for item in items:
        yield item


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


async def test_client_pins_calls_to_the_chosen_member() -> None:
    systems = await start_nodes(3)
    client = await casty.connect([systems[0].member.addr], config=FAST_CONFIG)
    try:
        target = next(m for m in client.members() if m.node_id == systems[2].node_id)
        svc = client.service(Fetcher, at=target)
        await asyncio.gather(*[svc.slow(0.0, n) for n in range(9)])
        assert _hosts(systems) == [systems[2]]  # every call landed on the pin
    finally:
        await client.close()
        await stop_all(systems)


async def test_node_pins_to_another_member() -> None:
    systems = await start_nodes(3)
    try:
        target = next(m for m in systems[0].members() if m.node_id == systems[1].node_id)
        assert await systems[0].service(Fetcher, at=target).slow(0.0, 5) == 10
        assert _hosts(systems) == [systems[1]]
    finally:
        await stop_all(systems)


async def test_node_pinned_to_itself_stays_local() -> None:
    systems = await start_nodes(2)
    try:
        me = next(m for m in systems[0].members() if m.node_id == systems[0].node_id)
        assert await systems[0].service(Fetcher, at=me).slow(0.0, 4) == 8
        assert _hosts(systems) == [systems[0]]
    finally:
        await stop_all(systems)


async def test_pinned_dispatch_rejected_without_a_cluster() -> None:
    async with casty.local() as system:
        member = casty.Member(node_id=uuid.uuid4(), addr="127.0.0.1:1")
        with pytest.raises(TypeError, match="clustered system"):
            system.service(Fetcher, at=member)


async def test_pinned_streaming_from_a_client() -> None:
    systems = await start_nodes(3)
    client = await casty.connect([systems[0].member.addr], config=FAST_CONFIG)
    try:
        target = next(m for m in client.members() if m.node_id == systems[1].node_id)
        svc: _Streamy = client.service(Streamy, at=target)
        assert [v async for v in svc.count(5)] == [0, 1, 2, 3, 4]  # server-streaming
        assert await svc.total(_ints([1, 2, 3])) == 6  # client-streaming
        assert [v async for v in svc.double(_ints([1, 2]))] == [2, 4]  # duplex
        hosts = [n for n in systems if n._host.is_active(STREAMY_WIRE, "@")]
        assert hosts == [systems[1]]
    finally:
        await client.close()
        await stop_all(systems)


async def test_pinned_streaming_local_and_remote_on_a_node() -> None:
    systems = await start_nodes(2)
    try:
        me, other = (
            next(m for m in systems[0].members() if m.node_id == n.node_id) for n in systems
        )
        assert [v async for v in systems[0].service(Streamy, at=me).count(3)] == [0, 1, 2]
        assert await systems[0].service(Streamy, at=other).total(_ints([4, 5])) == 9
        hosts = [n for n in systems if n._host.is_active(STREAMY_WIRE, "@")]
        assert hosts == systems  # one activation local, one on the pinned peer
    finally:
        await stop_all(systems)


async def test_failure_surfaces_as_actor_failed() -> None:
    systems = await start_nodes(2)
    client = await casty.connect([systems[0].member.addr], config=FAST_CONFIG)
    try:
        with pytest.raises(casty.ActorFailedError, match="kaboom"):
            await client.service(Fetcher).boom()
    finally:
        await client.close()
        await stop_all(systems)
