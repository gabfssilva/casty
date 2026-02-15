# tests/test_singleton.py
"""Tests for cluster singleton behavior."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty import ActorContext, ActorRef, Behavior, Behaviors
from casty.config import (
    CastyConfig,
    FailureDetectorConfig,
    GossipConfig,
    HeartbeatConfig,
)
from casty.core.journal import InMemoryJournal
from casty.cluster.system import ClusteredActorSystem


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Increment:
    pass


@dataclass(frozen=True)
class GetCount:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetCount | Ping


def counter_behavior(count: int = 0) -> Behavior[CounterMsg]:
    async def receive(_ctx: Any, msg: CounterMsg) -> Behavior[CounterMsg]:
        match msg:
            case Ping(reply_to):
                reply_to.tell("pong")
                return Behaviors.same()
            case Increment():
                return counter_behavior(count + 1)
            case GetCount(reply_to):
                reply_to.tell(count)
                return Behaviors.same()
            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


async def test_single_node_singleton() -> None:
    """Spawn singleton on a single-node cluster, send messages, verify responses."""
    async with ClusteredActorSystem(
        name="singleton-1", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        ref = system.spawn(
            Behaviors.singleton(lambda: counter_behavior()),
            "counter",
        )
        await asyncio.sleep(0.5)

        ref.tell(Increment())
        ref.tell(Increment())
        ref.tell(Increment())
        await asyncio.sleep(0.2)

        result = await system.ask(
            ref,
            lambda r: GetCount(reply_to=r),
            timeout=5.0,
        )
        assert result == 3


async def test_singleton_ping_pong() -> None:
    """Singleton responds to request-reply pattern."""
    async with ClusteredActorSystem(
        name="singleton-ping", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        ref = system.spawn(
            Behaviors.singleton(lambda: counter_behavior()),
            "pinger",
        )
        await asyncio.sleep(0.5)

        result = await system.ask(
            ref,
            lambda r: Ping(reply_to=r),
            timeout=5.0,
        )
        assert result == "pong"


async def test_leader_hosts_singleton_two_nodes() -> None:
    """In a 2-node cluster, the singleton runs on the leader node."""
    async with ClusteredActorSystem(
        name="singleton-2", host="127.0.0.1", port=0, node_id="node-1"
    ) as system_a:
        port_a = system_a.self_node.port

        async with ClusteredActorSystem(
            name="singleton-2",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            await system_a.wait_for(2, timeout=10.0)

            ref_a = system_a.spawn(
                Behaviors.singleton(lambda: counter_behavior()),
                "counter",
            )
            system_b.spawn(
                Behaviors.singleton(lambda: counter_behavior()),
                "counter",
            )
            await asyncio.sleep(1.0)

            # Send from node A
            ref_a.tell(Increment())
            ref_a.tell(Increment())
            await asyncio.sleep(0.5)

            result = await system_a.ask(
                ref_a,
                lambda r: GetCount(reply_to=r),
                timeout=5.0,
            )
            assert result == 2


async def test_buffering_during_startup() -> None:
    """Messages sent before SetRole arrives are buffered and delivered after activation."""
    async with ClusteredActorSystem(
        name="singleton-buf", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        ref = system.spawn(
            Behaviors.singleton(lambda: counter_behavior()),
            "buffered",
        )

        # Send immediately — before SetRole has likely arrived
        ref.tell(Increment())
        ref.tell(Increment())
        await asyncio.sleep(1.0)

        result = await system.ask(
            ref,
            lambda r: GetCount(reply_to=r),
            timeout=5.0,
        )
        assert result == 2


async def test_event_sourced_singleton() -> None:
    """Singleton with event sourcing persists and recovers state."""

    @dataclass(frozen=True)
    class Incremented:
        pass

    journal = InMemoryJournal()

    def on_event(state: int, _event: Incremented) -> int:
        return state + 1

    async def on_command(
        _ctx: Any, state: int, msg: CounterMsg
    ) -> Behavior[CounterMsg]:
        match msg:
            case Increment():
                return Behaviors.persisted([Incremented()])
            case GetCount(reply_to):
                reply_to.tell(state)
                return Behaviors.same()
            case _:
                return Behaviors.same()

    def make_es_behavior() -> Behavior[CounterMsg]:
        return Behaviors.event_sourced(
            entity_id="singleton-counter",
            journal=journal,
            initial_state=0,
            on_event=on_event,
            on_command=on_command,
        )

    async with ClusteredActorSystem(
        name="singleton-es", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        ref = system.spawn(
            Behaviors.singleton(make_es_behavior),
            "es-counter",
        )
        await asyncio.sleep(0.5)

        ref.tell(Increment())
        ref.tell(Increment())
        ref.tell(Increment())
        await asyncio.sleep(0.5)

        result = await system.ask(
            ref,
            lambda r: GetCount(reply_to=r),
            timeout=5.0,
        )
        assert result == 3

    # Verify events were persisted to journal
    events = await journal.load("singleton-counter")
    assert len(events) == 3


@dataclass(frozen=True)
class Tick:
    pass


def ticking_singleton(ticks: list[float], interval: float = 0.3) -> Behavior[Tick]:
    """Singleton that schedules periodic ticks via pipe_to_self."""

    async def setup(ctx: ActorContext[Tick]) -> Behavior[Tick]:
        async def schedule_tick() -> Tick:
            await asyncio.sleep(interval)
            return Tick()

        ctx.pipe_to_self(schedule_tick())

        async def receive(ctx: ActorContext[Tick], msg: Tick) -> Behavior[Tick]:
            match msg:
                case Tick():
                    ticks.append(asyncio.get_running_loop().time())
                    ctx.pipe_to_self(schedule_tick())
                    return Behaviors.same()
                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


FAST_CONFIG = CastyConfig(
    heartbeat=HeartbeatConfig(interval=0.2, availability_check_interval=0.5),
    failure_detector=FailureDetectorConfig(
        threshold=3.0,
        first_heartbeat_estimate_ms=300.0,
    ),
    gossip=GossipConfig(interval=0.3),
    suppress_dead_letters_on_shutdown=True,
)


async def wait_for_ticks(
    ticks: list[float],
    *,
    min_count: int,
    timeout: float = 5.0,
) -> None:
    """Poll until ``ticks`` has at least ``min_count`` entries."""
    deadline = asyncio.get_running_loop().time() + timeout
    while len(ticks) < min_count:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(
                f"Expected at least {min_count} ticks, got {len(ticks)} "
                f"after {timeout}s"
            )
        await asyncio.sleep(0.1)


def assert_single_ticker(ticks: list[float], tick_interval: float, label: str) -> None:
    """Assert ticks came from a single source by checking minimum interval.

    If two nodes were ticking simultaneously at ``tick_interval``, intervals
    between consecutive ticks would drop to roughly ``tick_interval / 2``.
    We reject any interval below half the expected period.
    """
    if len(ticks) < 3:
        return
    intervals = [ticks[i] - ticks[i - 1] for i in range(1, len(ticks))]
    min_interval = min(intervals)
    assert min_interval > tick_interval * 0.4, (
        f"[{label}] Minimum tick interval {min_interval:.3f}s is too short — "
        f"expected ~{tick_interval}s (possible duplicate singleton). "
        f"All intervals: {[f'{x:.3f}' for x in intervals]}"
    )


@pytest.mark.timeout(60)
async def test_singleton_failover_ticking_survives_two_leader_kills() -> None:
    """3-node cluster: singleton ticks, kill leader twice, ticks keep coming."""
    ticks: list[float] = []
    tick_interval = 0.3

    def make_ticker() -> Behavior[Tick]:
        return ticking_singleton(ticks, interval=tick_interval)

    # Manually manage lifecycle — we need to shut down specific nodes mid-test
    system_a = ClusteredActorSystem(
        name="failover",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
        config=FAST_CONFIG,
    )
    await system_a.__aenter__()
    port_a = system_a.self_node.port

    system_b = ClusteredActorSystem(
        name="failover",
        host="127.0.0.1",
        port=0,
        node_id="node-2",
        seed_nodes=[("127.0.0.1", port_a)],
        config=FAST_CONFIG,
    )
    await system_b.__aenter__()
    port_b = system_b.self_node.port

    system_c = ClusteredActorSystem(
        name="failover",
        host="127.0.0.1",
        port=0,
        node_id="node-3",
        seed_nodes=[("127.0.0.1", port_a), ("127.0.0.1", port_b)],
        config=FAST_CONFIG,
    )
    await system_c.__aenter__()

    systems: list[ClusteredActorSystem] = [system_a, system_b, system_c]
    try:
        # Wait for all 3 nodes to be up
        await system_a.wait_for(3, timeout=15.0)

        # Spawn singleton on all nodes
        for s in systems:
            s.spawn(Behaviors.singleton(make_ticker), "ticker")

        # Wait for singleton to start ticking
        await wait_for_ticks(ticks, min_count=3)
        assert_single_ticker(ticks, tick_interval, "3 nodes up")

        # --- Kill leader #1 ---

        state = await systems[0].get_cluster_state(timeout=5.0)
        leader_node = state.leader
        assert leader_node is not None

        leader_system = next(s for s in systems if s.self_node == leader_node)
        ticks_before_kill_1 = len(ticks)
        await leader_system.shutdown()
        systems.remove(leader_system)

        await wait_for_ticks(ticks, min_count=ticks_before_kill_1 + 3, timeout=10.0)
        assert_single_ticker(ticks, tick_interval, "after kill 1")

        # --- Kill leader #2 ---

        state2 = await systems[0].get_cluster_state(timeout=5.0)
        leader_node_2 = state2.leader
        assert leader_node_2 is not None

        leader_system_2 = next(s for s in systems if s.self_node == leader_node_2)
        ticks_before_kill_2 = len(ticks)
        await leader_system_2.shutdown()
        systems.remove(leader_system_2)

        await wait_for_ticks(ticks, min_count=ticks_before_kill_2 + 3, timeout=10.0)
        assert_single_ticker(ticks, tick_interval, "after kill 2")
    finally:
        for s in systems:
            await s.shutdown()
