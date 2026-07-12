from __future__ import annotations

import asyncio
import socket
import typing

import pytest

import casty
from casty.errors import ActorFailedError, ActorUnavailableError, QuorumUnavailableError

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local mode must never touch a socket. Blow up if it tries. Patching
    connect/bind (not recv/send) leaves the loop's own self-pipe alone."""

    def boom(*args: object, **kwargs: object) -> typing.NoReturn:
        raise AssertionError("local mode touched the network")

    monkeypatch.setattr(socket.socket, "connect", boom)
    monkeypatch.setattr(socket.socket, "bind", boom)
    monkeypatch.setattr(socket.socket, "connect_ex", boom)
    monkeypatch.setattr(asyncio.base_events.BaseEventLoop, "create_connection", boom)
    monkeypatch.setattr(asyncio.base_events.BaseEventLoop, "create_server", boom)


events: list[str] = []


@casty.actor(name="ltest.Counter")
class Counter:
    value: int = 0

    async def add(self, n: int) -> int:
        self.value += n
        return self.value

    async def read(self) -> int:
        return self.value


@casty.actor(name="ltest.Lifecycle", idle_timeout=0.3)
class Lifecycle:
    activations: int = 0

    @casty.activate
    async def _up(self) -> None:
        self.activations += 1
        events.append(f"activate:{casty.context().key}")

    @casty.deactivate
    async def _down(self) -> None:
        events.append(f"deactivate:{casty.context().key}")

    async def touch(self) -> int:
        return self.activations

    async def retire(self) -> None:
        casty.context().deactivate()


@casty.actor(name="ltest.Flaky")
class Flaky:
    value: int = 0

    async def set(self, n: int) -> None:
        self.value = n

    async def get(self) -> int:
        return self.value

    async def boom(self) -> None:
        raise RuntimeError("kaboom")


@casty.actor(name="ltest.Chain")
class ChainActor:
    async def ping(self, hops: list[str]) -> str:
        if not hops:
            return casty.context().key
        head, *rest = hops
        peer = casty.context().actor(ChainActor, head)
        return await peer.ping(rest)


@casty.actor(name="ltest.Replicated", replicas=3, write=casty.MAJORITY)
class Replicated:
    value: int = 0

    async def bump(self) -> int:
        self.value += 1
        return self.value


@casty.actor(name="ltest.Slow")
class Slow:
    async def nap(self, seconds: float) -> str:
        await asyncio.sleep(seconds)
        return "done"


async def test_same_key_same_activation_distinct_keys_distinct() -> None:
    async with casty.local() as node:
        assert await node.actor(Counter, "a").add(1) == 1
        assert await node.actor(Counter, "a").add(1) == 2  # same activation accumulates
        assert await node.actor(Counter, "b").add(1) == 1  # distinct key starts fresh


async def test_lifecycle_hooks_and_idle_loses_state() -> None:
    async with casty.local(default_idle_timeout=0.3) as node:
        events.clear()
        actor = node.actor(Lifecycle, "idle")
        assert await actor.touch() == 1
        await asyncio.sleep(0.8)  # > idle_timeout
        assert "activate:idle" in events
        assert "deactivate:idle" in events
        assert await actor.touch() == 1  # fresh activation: single-copy, no restore

        events.clear()
        other = node.actor(Lifecycle, "retire")
        await other.touch()
        await other.retire()
        await asyncio.sleep(0.05)
        assert "deactivate:retire" in events


async def test_supervisor_directives() -> None:
    decisions: list[casty.Directive] = []

    def supervisor(
        cls: type, key: str, exc: Exception, ctx: casty.FailureContext
    ) -> casty.Directive:
        return decisions.pop(0) if decisions else casty.KEEP

    async with casty.local(supervisor=supervisor) as node:
        keep = node.actor(Flaky, "keep")
        await keep.set(7)
        decisions.append(casty.KEEP)
        with pytest.raises(ActorFailedError, match="kaboom"):
            await keep.boom()
        assert await keep.get() == 7  # state survives

        reset = node.actor(Flaky, "reset")
        await reset.set(7)
        decisions.append(casty.RESET)
        with pytest.raises(ActorFailedError):
            await reset.boom()
        assert await reset.get() == 0  # activation discarded, state starts over

        stop = node.actor(Flaky, "stop")
        await stop.set(7)
        decisions.append(casty.STOP)
        with pytest.raises(ActorFailedError):
            await stop.boom()
        assert await stop.get() == 0  # deactivated, reactivates fresh


async def test_reentrancy_cycle_raises_and_plain_chain_works() -> None:
    async with casty.local() as node:
        assert await node.actor(ChainActor, "a").ping(["b", "c"]) == "c"
        with pytest.raises(ActorFailedError, match="ReentrancyError"):
            await node.actor(ChainActor, "a").ping(["b", "a"])
        with pytest.raises(ActorFailedError, match="ReentrancyError"):
            await node.actor(ChainActor, "a").ping(["a"])


async def test_replicated_actor_runs_single_copy_without_quorum() -> None:
    async with casty.local() as node:
        try:
            rep = node.actor(Replicated, "k")
            assert await rep.bump() == 1
            assert await rep.bump() == 2  # same activation keeps state, no quorum needed
        except QuorumUnavailableError:  # pragma: no cover - the bug this guards against
            pytest.fail("replicated actor should degrade to single-copy locally")


async def test_close_drains_in_flight_and_rejects_after() -> None:
    node = casty.local()
    in_flight = asyncio.create_task(node.actor(Slow, "s").nap(0.3))
    await asyncio.sleep(0.05)  # let the handler start
    await node.close()
    assert await in_flight == "done"  # drained, not dropped
    with pytest.raises(ActorUnavailableError):
        await node.actor(Counter, "post-stop").add(1)


async def test_map_roundtrips_locally() -> None:
    async with casty.local() as node:
        m: casty.Map[str, int] = node.map("scores")
        await m.put("a", 1)
        await m.put("b", 2)
        assert await m.get("a") == 1
        assert await m.contains("b") is True
        assert await m.size() == 2
        assert await m.remove("a") is True
        assert await m.get("a") is None
        assert dict(await m.items()) == {"b": 2}
        await m.clear()
        assert await m.size() == 0
