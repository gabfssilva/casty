"""Spec 11 steps 2-3: the wire subscribe (`SpyAgent`), cluster scope, the
client spy and `state_of`, on real TCP clusters."""

from __future__ import annotations

import asyncio

import pytest

import casty
from casty import inspection
from tests.integration.actors import (
    FAST_CONFIG,
    Counter,
    kill_node,
    owner_of,
    start_nodes,
    stop_all,
    wait_view,
)
from tests.integration.cluster import free_port

pytestmark = pytest.mark.asyncio

WIRE = "itest.Counter"


async def _next_of[T: inspection.SpyEvent](
    sub: inspection.Subscription, kind: type[T], timeout: float = 10.0
) -> T:
    async def scan() -> T:
        while True:
            event = await anext(sub)
            if isinstance(event, kind):
                return event

    return await asyncio.wait_for(scan(), timeout)


async def test_cluster_spy_sees_remote_owners_and_filters_at_source() -> None:
    nodes = await start_nodes(3)
    try:
        observer = next(n for n in nodes if n is not owner_of(nodes, WIRE, "spy-remote"))
        async with observer.spy(Counter, key="spy-remote", scope="cluster") as sub:
            await asyncio.sleep(0.2)  # let the dials land before emitting
            await observer.actor(Counter, "spy-nomatch").add(1)  # filtered at source
            await observer.actor(Counter, "spy-remote").add(2)
            event = await _next_of(sub, inspection.Received)
            assert event.key == "spy-remote"
            assert event.method == "add"
            assert event.node == owner_of(nodes, WIRE, "spy-remote").node_id
    finally:
        await stop_all(nodes)


async def test_payloads_round_trip_over_the_wire() -> None:
    nodes = await start_nodes(3)
    try:
        async with nodes[0].spy(Counter, key="spy-pay", scope="cluster", payloads=True) as sub:
            await asyncio.sleep(0.2)
            await nodes[0].actor(Counter, "spy-pay").add(41)
            received = await _next_of(sub, inspection.Received)
            assert inspection.decode(received) == [41]
            completed = await _next_of(sub, inspection.Completed)
            assert inspection.decode(completed) == 41
    finally:
        await stop_all(nodes)


async def test_killed_owner_yields_gap_then_events_resume() -> None:
    nodes = await start_nodes(3)
    try:
        owner = owner_of(nodes, WIRE, "spy-kill")
        observer = next(n for n in nodes if n is not owner)
        survivors = [n for n in nodes if n is not owner]
        async with observer.spy(Counter, key="spy-kill", scope="cluster") as sub:
            await asyncio.sleep(0.2)
            await observer.actor(Counter, "spy-kill").add(1)
            await _next_of(sub, inspection.Received)
            await kill_node(owner)
            gap = await _next_of(sub, inspection.Gap)
            assert gap.node == owner.node_id
            await wait_view(survivors, 2)
            await observer.actor(Counter, "spy-kill").add(2)
            event = await _next_of(sub, inspection.Received)
            assert event.node == owner_of(survivors, WIRE, "spy-kill").node_id
    finally:
        await stop_all(nodes)


async def test_two_subscriptions_proceed_independently() -> None:
    nodes = await start_nodes(3)
    try:
        async with (
            nodes[0].spy(Counter, key="spy-a", scope="cluster") as sub_a,
            nodes[0].spy(Counter, key="spy-b", scope="cluster") as sub_b,
        ):
            await asyncio.sleep(0.2)
            await nodes[0].actor(Counter, "spy-a").add(1)
            await nodes[0].actor(Counter, "spy-b").add(1)
            assert (await _next_of(sub_a, inspection.Received)).key == "spy-a"
            assert (await _next_of(sub_b, inspection.Received)).key == "spy-b"
    finally:
        await stop_all(nodes)


async def test_client_spies_and_new_member_is_observed() -> None:
    nodes = await start_nodes(2)
    client: casty.Client | None = None
    try:
        client = await casty.connect([nodes[0].member.addr], config=FAST_CONFIG)
        with pytest.raises(ValueError):
            client.spy(Counter, scope="local")
        async with client.spy(Counter, key="spy-client-*", scope="cluster") as sub:
            await asyncio.sleep(0.2)
            await client.actor(Counter, "spy-client-0").add(1)
            assert (await _next_of(sub, inspection.Received)).key == "spy-client-0"
            joined = await casty.start(
                f"127.0.0.1:{free_port()}", seeds=[nodes[0].member.addr], config=FAST_CONFIG
            )
            nodes.append(joined)
            await wait_view(nodes, 3)
            # wait for the client's SYNC loop to pick the member up and dial it
            await asyncio.sleep(FAST_CONFIG.client_sync_interval * 3)
            key = next(
                f"spy-client-{i}"
                for i in range(1, 200)
                if owner_of(nodes, WIRE, f"spy-client-{i}") is joined
            )
            await client.actor(Counter, key).add(1)
            while True:
                event = await _next_of(sub, inspection.Received)
                if event.key == key:
                    assert event.node == joined.node_id
                    break
    finally:
        if client is not None:
            await client.close()
        await stop_all(nodes)


async def test_state_of_returns_live_values_and_none() -> None:
    nodes = await start_nodes(3)
    try:
        reader = next(n for n in nodes if n is not owner_of(nodes, WIRE, "spy-state"))
        assert await reader.state_of(Counter, "spy-state") is None
        await reader.actor(Counter, "spy-state").add(5)
        assert await reader.state_of(Counter, "spy-state") == {"value": 5}
        owner = owner_of(nodes, WIRE, "spy-state")
        assert await owner.state_of(Counter, "spy-state") == {"value": 5}
    finally:
        await stop_all(nodes)
