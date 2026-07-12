from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator

import pytest

import casty
from casty.actors import registry
from casty.actors.context import current_context
from casty.errors import ActorFailedError, ActorUnavailableError, SerializationSchemaError


@casty.actor(name="stest.Tally")
class Tally:
    total: int = 0

    async def add(self, n: int) -> int:
        self.total += n
        return self.total


@casty.actor(name="stest.Manual", idle_timeout=0.2)
class Manual:
    """Detaches its reply by hand: the host half of spec 08, with no service."""

    async def later(self, value: int) -> int:
        reply = current_context().detach()
        _replies.append((reply, value))
        return 0  # ignored: the reply resolves out of band

    async def boom(self) -> int:
        current_context().detach()
        raise RuntimeError("after detach")


_replies: list[tuple[object, int]] = []


@casty.service(name="stest.Fanout", concurrency=None)
class Fanout:
    async def slow(self, n: int) -> int:
        await _barrier.wait()
        return n * 2

    async def tally(self, n: int) -> int:
        return await casty.context().actor(Tally, "t").add(n)

    async def boom(self) -> int:
        raise RuntimeError("kaboom")


@casty.service(name="stest.Bounded", concurrency=2)
class Bounded:
    async def hold(self, n: int) -> int:
        _live.append(n)
        _peak[0] = max(_peak[0], len(_live))
        await _gate.wait()
        _live.remove(n)
        return n


_barrier = asyncio.Event()
_gate = asyncio.Event()
_live: list[int] = []
_peak = [0]


@pytest.fixture(autouse=True)
def _reset() -> None:
    global _barrier, _gate
    _barrier = asyncio.Event()  # an Event binds to the loop that first awaits it
    _gate = asyncio.Event()
    _replies.clear()
    _live.clear()
    _peak[0] = 0


# --- host: detached reply (etapa 1) -------------------------------------------------


async def test_detached_reply_resolves_out_of_band() -> None:
    async with casty.local() as node:
        manual = node.actor(Manual, "m")
        call = asyncio.create_task(manual.later(7))
        await asyncio.sleep(0.05)
        assert not call.done()  # handler returned; the caller still waits

        reply, value = _replies[0]
        assert value == 7
        reply.set(21)  # type: ignore[attr-defined]
        assert await call == 21


async def test_detached_reply_holds_activation_open() -> None:
    async with casty.local() as node:
        manual = node.actor(Manual, "hold")
        call = asyncio.create_task(manual.later(1))
        await asyncio.sleep(0.4)  # well past idle_timeout=0.2
        assert node._host.is_active("stest.Manual", "hold")  # type: ignore[attr-defined]

        _replies[0][0].set(9)  # type: ignore[attr-defined]
        assert await call == 9
        await asyncio.sleep(0.4)
        assert not node._host.is_active("stest.Manual", "hold")  # type: ignore[attr-defined]


async def test_detached_reply_fail_propagates() -> None:
    async with casty.local() as node:
        call = asyncio.create_task(node.actor(Manual, "f").later(1))
        await asyncio.sleep(0.05)
        _replies[0][0].fail(ValueError("nope"))  # type: ignore[attr-defined]
        with pytest.raises(ValueError, match="nope"):
            await call


async def test_handler_raising_after_detach_releases_the_flight() -> None:
    async with casty.local() as node:
        with pytest.raises(ActorFailedError):
            await node.actor(Manual, "b").boom()
        await asyncio.sleep(0.4)  # idle must still fire: inflight went back to zero
        assert not node._host.is_active("stest.Manual", "b")  # type: ignore[attr-defined]


# --- builder (etapa 2) --------------------------------------------------------------


async def test_calls_run_concurrently() -> None:
    async with casty.local() as node:
        fetcher = node.service(Fanout)
        calls = [asyncio.create_task(fetcher.slow(n)) for n in range(5)]
        await asyncio.sleep(0.05)
        assert not any(c.done() for c in calls)  # all five are inside the method
        _barrier.set()
        assert await asyncio.gather(*calls) == [0, 2, 4, 6, 8]


async def test_service_reaches_actors() -> None:
    async with casty.local() as node:
        svc = node.service(Fanout)
        assert await asyncio.gather(*[svc.tally(1) for _ in range(4)]) == [1, 2, 3, 4]


async def test_service_failure_is_typed() -> None:
    async with casty.local() as node:
        with pytest.raises(ActorFailedError):
            await node.service(Fanout).boom()


async def test_concurrency_caps_in_flight_calls() -> None:
    async with casty.local() as node:
        svc = node.service(Bounded)
        calls = [asyncio.create_task(svc.hold(n)) for n in range(6)]
        await asyncio.sleep(0.05)
        assert _peak[0] == 2  # the mailbox holds the rest: backpressure
        _gate.set()
        assert sorted(await asyncio.gather(*calls)) == [0, 1, 2, 3, 4, 5]


async def test_in_flight_calls_fail_on_stop() -> None:
    node = casty.local()
    svc = node.service(Fanout)
    call = asyncio.create_task(svc.slow(1))
    await asyncio.sleep(0.05)
    await node.close()
    with pytest.raises(ActorUnavailableError):
        await call


async def test_service_proxy_rejects_actors() -> None:
    async with casty.local() as node:
        with pytest.raises(TypeError, match=re.escape("not a @casty.service")):
            node.service(Tally)


def test_registered_as_a_service() -> None:
    info = registry.info_of(Fanout)
    assert info is not None
    assert info.kind == "service"
    assert info.state_fields == ()
    assert set(info.methods) == {"slow", "tally", "boom"}
    assert info.methods["slow"].params == (("n", int),)


def test_state_field_is_rejected() -> None:
    with pytest.raises(SerializationSchemaError, match="stateless"):

        @casty.service(name="stest.Stateful")
        class _Stateful:
            count: int = 0

            async def bump(self) -> int:
                return self.count


def test_streaming_method_is_rejected() -> None:
    with pytest.raises(SerializationSchemaError, match="streaming"):

        @casty.service(name="stest.Streamer")
        class _Streamer:
            async def tail(self) -> AsyncIterator[int]:
                yield 1


def test_streaming_param_is_rejected() -> None:
    with pytest.raises(SerializationSchemaError, match="streaming"):

        @casty.service(name="stest.Uploader")
        class _Uploader:
            async def upload(self, lines: AsyncIterator[str]) -> int:
                return 0


def test_sync_method_is_rejected() -> None:
    with pytest.raises(SerializationSchemaError, match="must be async"):

        @casty.service(name="stest.Sync")
        class _Sync:
            def now(self) -> int:
                return 1


def test_user_hook_is_rejected() -> None:
    with pytest.raises(SerializationSchemaError, match="lifecycle hooks"):

        @casty.service(name="stest.Hooked")
        class _Hooked:
            @casty.activate
            async def _up(self) -> None: ...

            async def ping(self) -> int:
                return 1
