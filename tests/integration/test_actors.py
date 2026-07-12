from __future__ import annotations

import asyncio

import pytest

import casty
from casty.errors import ActorFailedError, ActorUnavailableError
from casty.membership.table import ViewEvent
from tests.integration import actors as fx
from tests.integration.actors import (
    ChainActor,
    Counter,
    Flaky,
    Lifecycle,
    kill_node,
    start_nodes,
    stop_all,
    wait_view,
)

pytestmark = pytest.mark.asyncio


async def test_same_key_hits_same_activation_across_nodes() -> None:
    systems = await start_nodes(3)
    try:
        for i, system in enumerate(systems):
            counter = system.actor(Counter, "shared")
            assert await counter.add(1) == i + 1
        assert await systems[0].actor(Counter, "shared").read() == 3
    finally:
        await stop_all(systems)


async def test_keys_distribute_across_nodes() -> None:
    systems = await start_nodes(3)
    try:
        for i in range(60):
            await systems[0].actor(Counter, f"key-{i}").add(1)
        hosting = [n._host.activation_count for n in systems]
        assert sum(hosting) == 60
        assert all(count > 0 for count in hosting), hosting
    finally:
        await stop_all(systems)


async def test_owner_death_reactivates_elsewhere() -> None:
    systems = await start_nodes(3)
    try:
        key = "reborn"
        await systems[0].actor(Counter, key).add(10)
        wire = "itest.Counter"
        owner = next(n for n in systems if n._host.is_active(wire, key))
        survivors = [n for n in systems if n is not owner]
        await kill_node(owner)
        await wait_view(survivors, 2)
        # no replication yet: state starts over, but the actor is reachable
        deadline = asyncio.get_running_loop().time() + 5.0
        while True:
            try:
                assert await survivors[0].actor(Counter, key).add(1) == 1
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.1)
    finally:
        await stop_all(systems)


async def test_reentrancy_cycle_raises_and_plain_chain_works() -> None:
    systems = await start_nodes(2)
    try:
        # a -> b -> c: legit chain across actors
        result = await systems[0].actor(ChainActor, "a").ping(["b", "c"])
        assert result == "c"
        # a -> b -> a: the cycle is detected at b's nested ask; for the external
        # caller that is a failure of the handler that attempted it
        with pytest.raises(ActorFailedError, match="ReentrancyError"):
            await systems[0].actor(ChainActor, "a").ping(["b", "a"])
        # a -> a: self-cycle, same shape
        with pytest.raises(ActorFailedError, match="ReentrancyError"):
            await systems[0].actor(ChainActor, "a").ping(["a"])
    finally:
        await stop_all(systems)


async def test_idle_deactivation_and_explicit_deactivate() -> None:
    systems = await start_nodes(1)
    try:
        fx.events.clear()
        actor = systems[0].actor(Lifecycle, "idle-key")
        assert await actor.touch() == 1
        await asyncio.sleep(0.8)  # > idle_timeout=0.3
        assert "activate:idle-key" in fx.events
        assert "deactivate:idle-key" in fx.events
        assert await actor.touch() == 1  # fresh activation (state was not replicated)

        fx.events.clear()
        other = systems[0].actor(Lifecycle, "retire-key")
        await other.touch()
        await other.retire()
        await asyncio.sleep(0.1)
        assert "deactivate:retire-key" in fx.events
    finally:
        await stop_all(systems)


async def test_supervisor_directives() -> None:
    decisions: list[casty.Directive] = []

    def supervisor(
        cls: type, key: str, exc: Exception, ctx: casty.FailureContext
    ) -> casty.Directive:
        directive = decisions.pop(0) if decisions else casty.KEEP
        return directive

    systems = await start_nodes(1, supervisor=supervisor)
    try:
        system = systems[0]
        # KEEP: state survives the failure
        keep = system.actor(Flaky, "keep")
        await keep.set(7)
        decisions.append(casty.KEEP)
        with pytest.raises(ActorFailedError, match="kaboom"):
            await keep.boom()
        assert await keep.get() == 7
        # RESET: activation discarded, state starts over
        reset = system.actor(Flaky, "reset")
        await reset.set(7)
        decisions.append(casty.RESET)
        with pytest.raises(ActorFailedError):
            await reset.boom()
        assert await reset.get() == 0
        # STOP: deactivates; next call reactivates fresh
        stop = system.actor(Flaky, "stop")
        await stop.set(7)
        decisions.append(casty.STOP)
        with pytest.raises(ActorFailedError):
            await stop.boom()
        assert not system._host.is_active("itest.Flaky", "stop")
        assert await stop.get() == 0
    finally:
        await stop_all(systems)


async def test_graceful_stop_drains_and_leaves_cleanly() -> None:
    systems = await start_nodes(3)
    try:
        leaver = systems[0]
        survivors = systems[1:]
        seen: list[tuple[str, str]] = []

        def record(e: ViewEvent) -> None:
            seen.append((e.kind, e.member.addr))

        for system in survivors:
            system.membership.subscribe(record)

        @casty.actor(name="itest.Slow")
        class Slow:
            async def nap(self, seconds: float) -> str:
                await asyncio.sleep(seconds)
                return "done"

        key = next(
            f"k{i}" for i in range(100)
            if leaver._ring is not None
            and leaver._ring.owner(f"itest.Slow/k{i}") == leaver.node_id
        )
        in_flight = asyncio.create_task(leaver.actor(Slow, key).nap(0.4))
        await asyncio.sleep(0.1)  # let the call start
        await leaver.close()
        assert await in_flight == "done"  # drained, not dropped
        with pytest.raises((ActorUnavailableError, casty.CastyError)):
            await leaver.actor(Counter, "post-stop").add(1)
        await wait_view(survivors, 2)
        about_leaver = {kind for kind, addr in seen if addr == leaver.member.addr}
        assert "left" in about_leaver
        assert "suspect" not in about_leaver and "dead" not in about_leaver, seen
    finally:
        await stop_all(systems)


async def test_lite_member_routes_and_stays_out_of_the_ring() -> None:
    systems = await start_nodes(2)
    client: casty.Client | None = None
    try:
        client = await casty.connect([systems[0].member.addr], config=fx.FAST_CONFIG)
        counter = client.actor(Counter, "via-client")
        assert await counter.add(5) == 5
        assert await systems[0].actor(Counter, "via-client").read() == 5
        for system in systems:
            ring = system._ring
            assert ring is not None
            assert len(ring.nodes) == 2  # client not placed
    finally:
        if client is not None:
            await client.close()
        await stop_all(systems)
