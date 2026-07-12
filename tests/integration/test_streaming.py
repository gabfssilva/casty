from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

import casty
from casty.actors import registry
from tests.integration.actors import Log, kill_node, owner_of, start_nodes, stop_all, wait_view

pytestmark = pytest.mark.asyncio

WIRE = "itest.Log"


def _info() -> registry.ActorInfo:
    info = registry.info_of(Log)
    assert info is not None
    return info


def _stream_out(
    node: casty.Node,
    key: str,
    method: str,
    *,
    kwargs: dict[str, object] | None = None,
    in_iter: AsyncIterator[object] | None = None,
) -> AsyncIterator[object]:
    return node.stream_out(_info(), key, _info().methods[method], kwargs or {}, in_iter, [])


async def _stream_in(
    node: casty.Node,
    key: str,
    method: str,
    in_iter: AsyncIterator[object],
    *,
    kwargs: dict[str, object] | None = None,
) -> object:
    return await node.stream_in(_info(), key, _info().methods[method], kwargs or {}, in_iter, [])


async def _source(items: list[bytes]) -> AsyncIterator[bytes]:
    for item in items:
        yield item


def _remote(nodes: list[casty.Node], key: str) -> casty.Node:
    owner = owner_of(nodes, WIRE, key)
    return next(n for n in nodes if n is not owner)


async def test_server_streaming_cross_node() -> None:
    nodes = await start_nodes(3)
    try:
        key = "srv"
        remote = _remote(nodes, key)
        await _stream_in(remote, key, "ingest", _source([b"a", b"b", b"c"]))
        out = [e async for e in _stream_out(remote, key, "tail", kwargs={"since": 1})]
        assert out == [b"b", b"c"]
        # the actor really activated on its ring owner, not the caller
        assert owner_of(nodes, WIRE, key)._host.is_active(WIRE, key)
    finally:
        await stop_all(nodes)


async def test_client_streaming_cross_node() -> None:
    nodes = await start_nodes(3)
    try:
        key = "cli"
        remote = _remote(nodes, key)
        n = await _stream_in(remote, key, "ingest", _source([b"x", b"yy", b"zzz"]))
        assert n == 3
        size = await remote.actor(Log, key).size()
        assert size == 3
    finally:
        await stop_all(nodes)


async def test_duplex_cross_node() -> None:
    nodes = await start_nodes(3)
    try:
        key = "dup"
        remote = _remote(nodes, key)
        out = [
            v
            async for v in _stream_out(
                remote, key, "transform", in_iter=_source([b"a", b"bb", b"ccc"])
            )
        ]
        assert out == [1, 2, 3]
    finally:
        await stop_all(nodes)


async def test_local_and_remote_both_work() -> None:
    nodes = await start_nodes(3)
    try:
        key = "both"
        owner = owner_of(nodes, WIRE, key)
        remote = next(n for n in nodes if n is not owner)
        await _stream_in(owner, key, "ingest", _source([b"1"]))  # local short-circuit
        await _stream_in(remote, key, "ingest", _source([b"2"]))  # over the wire
        out = [e async for e in _stream_out(owner, key, "tail", kwargs={"since": 0})]
        assert out == [b"1", b"2"]
    finally:
        await stop_all(nodes)


async def test_large_stream_transfers_intact() -> None:
    nodes = await start_nodes(3)
    try:
        key = "big"
        remote = _remote(nodes, key)
        payloads = [f"item-{i}".encode() for i in range(2000)]
        n = await _stream_in(remote, key, "ingest", _source(payloads))
        assert n == 2000
        got = [e async for e in _stream_out(remote, key, "tail", kwargs={"since": 0})]
        assert got == payloads
    finally:
        await stop_all(nodes)


async def test_backpressure_throttles_producer() -> None:
    nodes = await start_nodes(3)
    try:
        key = "bp"
        remote = _remote(nodes, key)
        # Read slowly; the owner cannot run far ahead because credit only returns
        # as we consume. We just assert correctness under a paced consumer.
        got = []
        async for v in _stream_out(remote, key, "count_up", kwargs={"n": 200}):
            got.append(v)
            if len(got) % 20 == 0:
                await asyncio.sleep(0.01)
        assert got == list(range(200))
    finally:
        await stop_all(nodes)


async def test_streaming_error_propagates_cross_node() -> None:
    nodes = await start_nodes(3)
    try:
        key = "err"
        remote = _remote(nodes, key)
        received: list[object] = []
        with pytest.raises(casty.ActorFailedError, match="kaboom"):
            async for v in _stream_out(remote, key, "boom"):
                received.append(v)
        assert received == [1]
    finally:
        await stop_all(nodes)


async def test_proxy_surface_cross_node() -> None:
    nodes = await start_nodes(3)
    try:
        key = "proxy"
        remote = _remote(nodes, key)
        log = remote.actor(Log, key)
        assert await log.ingest(_source([b"a", b"bb"])) == 2
        assert [e async for e in log.tail(0)] == [b"a", b"bb"]
        assert [v async for v in log.transform(_source([b"x", b"yy"]))] == [1, 2]
    finally:
        await stop_all(nodes)


async def test_owner_death_mid_stream_raises() -> None:
    nodes = await start_nodes(3)
    try:
        key = "die"
        owner = owner_of(nodes, WIRE, key)
        remote = next(n for n in nodes if n is not owner)
        survivors = [n for n in nodes if n is not owner]
        it = _stream_out(remote, key, "count_up", kwargs={"n": 1_000_000}).__aiter__()
        assert await it.__anext__() == 0  # stream is live
        await kill_node(owner)
        await wait_view(survivors, 2)
        with pytest.raises(casty.CastyError):
            for _ in range(1_000_000):
                await it.__anext__()
    finally:
        await stop_all([n for n in nodes if n is not owner])
