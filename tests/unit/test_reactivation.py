"""The durable `@active` mark and the pure ring-rebuild scans."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable

import pytest

import casty
from casty.actors import reactivation
from casty.actors.host import ActorHost
from casty.actors.registry import Directive, FailureContext, info_of
from casty.errors import ActorFailedError, RangeMovingError
from tests.unit.cluster import Cluster, Node

pytestmark = pytest.mark.asyncio

ACTIVE = reactivation.ACTIVE_PAGE


@casty.actor(name="unit.Pump", replicas=3, idle_timeout=float("inf"))
class Pump:
    value: int = 0

    async def add(self, n: int) -> int:
        self.value += n
        return self.value

    async def read(self) -> int:
        return self.value

    async def retire(self) -> None:
        casty.context().deactivate()

    async def boom(self) -> None:
        raise RuntimeError("kaboom")


@casty.actor(name="unit.Passive", replicas=3)
class Passive:
    value: int = 0

    async def add(self, n: int) -> int:
        self.value += n
        return self.value


PUMP = info_of(Pump)
PASSIVE = info_of(Passive)
assert PUMP is not None and PASSIVE is not None


def majority_down(cluster: Cluster, owner: Node) -> None:
    for node in cluster.replicas(PUMP, "k"):
        if node is not owner:
            cluster.down.add(node.node_id)


async def until(check: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if check():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition did not hold in time")


async def test_only_eligible_actors_write_the_mark() -> None:
    cluster = Cluster(5)
    await cluster.owner(PUMP, "k").call(PUMP, "k", "add", 1)
    await cluster.owner(PASSIVE, "k").call(PASSIVE, "k", "add", 1)
    for node in cluster.replicas(PUMP, "k"):
        assert ACTIVE in node.pages(PUMP, "k")
    for node in cluster.replicas(PASSIVE, "k"):
        assert ACTIVE not in node.pages(PASSIVE, "k")


async def test_startup_rewrites_the_mark_and_advances_the_hlc() -> None:
    cluster = Cluster(5)
    owner = cluster.owner(PUMP, "k")
    await owner.call(PUMP, "k", "add", 1)
    first = owner.page_hlc(PUMP, "k", ACTIVE)
    owner.fresh_host()
    assert await owner.call(PUMP, "k", "read") == 1  # reserved page never restores
    assert owner.page_hlc(PUMP, "k", ACTIVE).order() > first.order()


async def test_superseded_activation_is_fenced_by_the_re_mark() -> None:
    cluster = Cluster(5)
    owner = cluster.owner(PUMP, "k")
    other = next(n for n in cluster.replicas(PUMP, "k") if n is not owner)
    await owner.call(PUMP, "k", "add", 1)
    await other.call(PUMP, "k", "read")  # dual owner: its startup re-mark wins
    with pytest.raises(RangeMovingError):
        await owner.call(PUMP, "k", "add", 1)
    assert not owner.host.is_active(PUMP.wire_name, "k")
    assert ACTIVE in other.pages(PUMP, "k")


async def test_deactivate_clears_the_mark_and_gates_on_quorum() -> None:
    cluster = Cluster(5)
    owner = cluster.owner(PUMP, "k")
    await owner.call(PUMP, "k", "add", 1)
    majority_down(cluster, owner)
    await owner.call(PUMP, "k", "retire")
    await asyncio.sleep(0.15)
    assert owner.host.is_active(PUMP.wire_name, "k")  # the clear has not landed
    cluster.down.clear()
    await until(lambda: not owner.host.is_active(PUMP.wire_name, "k"))
    assert ACTIVE not in owner.pages(PUMP, "k")


async def test_stop_enters_pending_clear_and_settles_after_heal() -> None:
    cluster = Cluster(5)
    owner = cluster.owner(PUMP, "k")

    def stop(cls: type, key: str, exc: Exception, ctx: FailureContext) -> Directive:
        return Directive.STOP

    owner.host = ActorHost(
        router=owner, replication=owner.replication, supervisor=stop, reactivation_retry=0.05
    )
    await owner.call(PUMP, "k", "add", 1)
    majority_down(cluster, owner)
    with pytest.raises(ActorFailedError):
        await owner.call(PUMP, "k", "boom")
    await until(lambda: (PUMP.wire_name, "k") in owner.host.pending_clears)
    cluster.down.clear()
    await until(lambda: (PUMP.wire_name, "k") not in owner.host.pending_clears)
    assert ACTIVE not in owner.pages(PUMP, "k")


async def test_drain_leaves_the_mark() -> None:
    cluster = Cluster(5)
    owner = cluster.owner(PUMP, "k")
    await owner.call(PUMP, "k", "add", 1)
    await owner.host.stop(1.0)
    assert ACTIVE in owner.pages(PUMP, "k")


async def test_sweep_decisions() -> None:
    me = uuid.uuid4()
    other = uuid.uuid4()
    marked = [
        reactivation.Marked(wire_name="a", key="mine", owner=me, replicas=(me, other)),
        reactivation.Marked(wire_name="a", key="active", owner=me, replicas=(me,)),
        reactivation.Marked(wire_name="a", key="pending", owner=me, replicas=(me,)),
        reactivation.Marked(wire_name="a", key="theirs", owner=other, replicas=(other,)),
    ]
    placed = [("a", "active", me), ("a", "moved", other)]
    decision = reactivation.sweep(
        marked=marked,
        placed=placed,
        active={("a", "active"), ("a", "moved")},
        pending_clears={("a", "pending")},
        node_id=me,
    )
    assert decision.pokes == [("a", "mine")]
    assert decision.discards == [("a", "moved")]
    assert decision.handoffs == [("a", "theirs")]
