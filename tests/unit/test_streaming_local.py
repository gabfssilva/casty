from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator

import pytest

import casty
from casty.actors import registry
from casty.errors import ActorFailedError
from casty.system import ActorSystem


@casty.actor(name="stest.Log")
class Log:
    entries: list[bytes] = dataclasses.field(default_factory=list)

    async def tail(self, since: int) -> AsyncIterator[bytes]:
        for e in self.entries[since:]:
            yield e

    async def ingest(self, items: AsyncIterator[bytes]) -> int:
        n = 0
        async for item in items:
            self.entries.append(item)
            n += 1
        return n

    async def transform(self, xs: AsyncIterator[bytes]) -> AsyncIterator[int]:
        async for x in xs:
            yield len(x)

    async def size(self) -> int:
        return len(self.entries)

    async def boom(self) -> AsyncIterator[int]:
        yield 1
        raise RuntimeError("kaboom")

    async def forever(self) -> AsyncIterator[int]:
        i = 0
        while True:
            yield i
            i += 1


def _method(name: str) -> registry.MethodInfo:
    info = registry.info_of(Log)
    assert info is not None
    return info.methods[name]


def _info() -> registry.ActorInfo:
    info = registry.info_of(Log)
    assert info is not None
    return info


async def _source(items: list[bytes]) -> AsyncIterator[bytes]:
    for item in items:
        yield item


async def _drive_ingest(sys_: ActorSystem, key: str, items: list[bytes]) -> int:
    result = await sys_.stream_in(
        _info(), key, _method("ingest"), {}, _source(items), []
    )
    assert isinstance(result, int)
    return result


async def test_server_streaming_yields_state() -> None:
    async with casty.local() as sys_:
        await _drive_ingest(sys_, "a", [b"x", b"y", b"z"])
        out = [
            e
            async for e in sys_.stream_out(_info(), "a", _method("tail"), {"since": 1}, None, [])
        ]
        assert out == [b"y", b"z"]


async def test_client_streaming_folds_and_commits() -> None:
    async with casty.local() as sys_:
        n = await _drive_ingest(sys_, "b", [b"a", b"bb", b"ccc"])
        assert n == 3
        # mutation is visible to a subsequent unary call on the same activation
        size = await sys_.actor(Log, "b").size()
        assert size == 3


async def test_duplex_streaming_transforms() -> None:
    async with casty.local() as sys_:
        out = [
            v
            async for v in sys_.stream_out(
                _info(), "c", _method("transform"), {}, _source([b"a", b"bb", b"ccc"]), []
            )
        ]
        assert out == [1, 2, 3]


async def test_streaming_error_surfaces_on_consumer() -> None:
    async with casty.local() as sys_:
        received: list[int] = []
        with pytest.raises(ActorFailedError, match="kaboom"):
            async for v in sys_.stream_out(_info(), "d", _method("boom"), {}, None, []):
                received.append(typing_cast_int(v))
        assert received == [1]


async def test_break_frees_the_worker() -> None:
    async with casty.local() as sys_:
        seen = 0
        async for _v in sys_.stream_out(_info(), "e", _method("forever"), {}, None, []):
            seen += 1
            if seen >= 3:
                break
        # the actor is free again: a fresh unary call returns promptly
        size = await asyncio.wait_for(sys_.actor(Log, "e").size(), timeout=1.0)
        assert size == 0


def typing_cast_int(v: object) -> int:
    assert isinstance(v, int)
    return v


async def test_proxy_server_and_client_streaming() -> None:
    async with casty.local() as sys_:
        log = sys_.actor(Log, "proxy")
        n = await log.ingest(_source([b"a", b"b"]))
        assert n == 2
        out = [e async for e in log.tail(0)]
        assert out == [b"a", b"b"]


async def test_proxy_duplex_streaming() -> None:
    async with casty.local() as sys_:
        log = sys_.actor(Log, "proxy-dup")
        out = [v async for v in log.transform(_source([b"a", b"bb", b"ccc"]))]
        assert out == [1, 2, 3]
