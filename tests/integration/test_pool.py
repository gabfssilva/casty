from __future__ import annotations

import asyncio
import time

import pytest

from casty.config import TransportConfig
from casty.errors import CastyError
from casty.transport.connection import Connection
from casty.transport.pool import Pool
from casty.transport.server import Server
from tests.integration.conftest import make_local


async def echo(conn: Connection, msg_type: int, body: bytes) -> bytes:
    return body


async def start_server() -> Server:
    return await Server.start(
        "127.0.0.1", 0, local=make_local(), on_request=echo
    )


async def test_pool_reuses_connection() -> None:
    server = await start_server()
    pool = Pool(local=make_local())
    try:
        addr = f"127.0.0.1:{server.port}"
        first = await pool.get(addr)
        second = await pool.get(addr)
        assert first is second
        assert await first.ask(0x40, b"x") == b"x"
    finally:
        await pool.close()
        await server.close()


async def test_pool_redials_after_connection_dies() -> None:
    server = await start_server()
    pool = Pool(local=make_local())
    try:
        addr = f"127.0.0.1:{server.port}"
        first = await pool.get(addr)
        await first.close()
        second = await pool.get(addr)
        assert second is not first
        assert await second.ask(0x40, b"y") == b"y"
    finally:
        await pool.close()
        await server.close()


async def test_backoff_between_attempts() -> None:
    config = TransportConfig(reconnect_base=0.1, connect_timeout=0.2)
    pool = Pool(local=make_local(), config=config)
    try:
        started = time.monotonic()
        with pytest.raises((CastyError, OSError)):
            await pool.get("127.0.0.1:1", max_attempts=3)  # nothing listens on port 1
        elapsed = time.monotonic() - started
        # two backoff sleeps: ~0.1 + ~0.2 (±25% jitter) => at least 0.2s total
        assert elapsed >= 0.2
    finally:
        await pool.close()


async def test_address_map_rewrites_the_dialed_address() -> None:
    server = await start_server()
    real = f"127.0.0.1:{server.port}"
    announced = "10.0.0.1:7001"
    pool = Pool(local=make_local(), address_map=lambda addr: real if addr == announced else addr)
    try:
        conn = await pool.get(announced)
        assert await conn.ask(0x40, b"tunneled") == b"tunneled"
        # the connection stays keyed by the announced address
        assert await pool.get(announced) is conn
    finally:
        await pool.close()
        await server.close()


async def test_duplicate_connections_resolved_by_node_id() -> None:
    local_a = make_local()
    local_b = make_local()
    inbound_a: list[Connection] = []
    inbound_b: list[Connection] = []

    pool_a = Pool(local=local_a, on_request=echo)
    pool_b = Pool(local=local_b, on_request=echo)

    async def register_a(conn: Connection) -> None:
        inbound_a.append(pool_a.register(conn))

    async def register_b(conn: Connection) -> None:
        inbound_b.append(pool_b.register(conn))

    server_a = await Server.start(
        "127.0.0.1", 0, local=local_a, on_request=echo, on_connection=register_a
    )
    server_b = await Server.start(
        "127.0.0.1", 0, local=local_b, on_request=echo, on_connection=register_b
    )
    try:
        # both sides dial each other simultaneously
        await asyncio.gather(
            pool_a.get(f"127.0.0.1:{server_b.port}"),
            pool_b.get(f"127.0.0.1:{server_a.port}"),
        )
        await asyncio.sleep(0.1)  # let register callbacks run duplicate resolution
        surviving_a = pool_a.by_node(local_b.node_id)
        surviving_b = pool_b.by_node(local_a.node_id)
        assert surviving_a is not None and not surviving_a.is_closed
        assert surviving_b is not None and not surviving_b.is_closed
        assert await surviving_a.ask(0x40, b"a->b") == b"a->b"
        assert await surviving_b.ask(0x40, b"b->a") == b"b->a"
    finally:
        await pool_a.close()
        await pool_b.close()
        await server_a.close()
        await server_b.close()
