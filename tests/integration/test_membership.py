from __future__ import annotations

import asyncio

import pytest

from casty.membership import messages
from casty.membership.table import Status
from tests.integration.cluster import ClusterNode, start_cluster, wait_converged

pytestmark = pytest.mark.asyncio


async def stop_all(nodes: list[ClusterNode]) -> None:
    await asyncio.gather(*(node.kill() for node in nodes), return_exceptions=True)


async def test_three_nodes_converge() -> None:
    nodes = await start_cluster(3)
    try:
        expected = {node.member for node in nodes}
        await wait_converged(nodes, expected)
    finally:
        await stop_all(nodes)


async def test_abrupt_death_converges_to_dead() -> None:
    nodes = await start_cluster(4)
    try:
        expected = {node.member for node in nodes}
        await wait_converged(nodes, expected)
        victim = nodes[-1]
        survivors = nodes[:-1]
        await victim.kill()
        await wait_converged(survivors, {node.member for node in survivors})
        for node in survivors:
            dead = [e for e in node.events if e.kind == "dead" and e.member == victim.member]
            assert len(dead) == 1, f"{node.member.addr}: {node.events}"
    finally:
        await stop_all(nodes)


async def test_graceful_leave_has_no_suspicion() -> None:
    nodes = await start_cluster(3)
    try:
        expected = {node.member for node in nodes}
        await wait_converged(nodes, expected)
        leaver = nodes[-1]
        survivors = nodes[:-1]
        await leaver.leave()
        await wait_converged(survivors, {node.member for node in survivors})
        for node in survivors:
            kinds = {e.kind for e in node.events if e.member == leaver.member}
            assert "left" in kinds
            assert "suspect" not in kinds and "dead" not in kinds
    finally:
        await stop_all(nodes)


async def test_false_suspicion_is_refuted() -> None:
    nodes = await start_cluster(3)
    try:
        expected = {node.member for node in nodes}
        await wait_converged(nodes, expected)
        accuser, victim = nodes[0], nodes[1]
        record = accuser.membership.table.get(victim.member.node_id)
        assert record is not None
        # forge a suspicion without an actual failure
        accuser.membership.table.merge(
            victim.member, record.incarnation, Status.SUSPECT, now=0.0
        )
        await accuser.membership._broadcast(
            messages.NodeSuspect(
                node_id=victim.member.node_id, incarnation=record.incarnation
            )
        )
        # victim must refute and stay in every view, with a bumped incarnation
        await wait_converged(nodes, expected)
        await asyncio.sleep(1.0)  # longer than suspicion_timeout
        await wait_converged(nodes, expected)
        assert victim.membership.table.incarnation > 0
        refuted = accuser.membership.table.get(victim.member.node_id)
        assert refuted is not None
        assert refuted.status is Status.ALIVE
        assert refuted.incarnation == victim.membership.table.incarnation
    finally:
        await stop_all(nodes)


async def test_overlay_survives_30_percent_failure() -> None:
    nodes = await start_cluster(10)
    try:
        expected = {node.member for node in nodes}
        await wait_converged(nodes, expected, timeout=15.0)
        victims, survivors = nodes[7:], nodes[:7]
        await asyncio.gather(*(v.kill() for v in victims))
        await wait_converged(
            survivors, {node.member for node in survivors}, timeout=20.0
        )
    finally:
        await stop_all(nodes)
