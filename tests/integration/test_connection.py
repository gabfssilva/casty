from __future__ import annotations

import asyncio

import pytest

from casty.config import TLS, CompressionConfig, TransportConfig
from casty.errors import (
    CastyError,
    CastyTimeoutError,
    ConnectionLostError,
    HandshakeError,
    RemoteError,
)
from casty.transport.connection import BulkReceiver, Connection
from casty.transport.server import Server
from tests.integration.conftest import Certs, make_local

ECHO = 0x40  # test msg_type in the actor range
SLOW = 0x41
BOOM = 0x42


async def echo_handler(conn: Connection, msg_type: int, body: bytes) -> bytes:
    if msg_type == SLOW:
        await asyncio.sleep(float(body.decode()))
        return body
    if msg_type == BOOM:
        raise RemoteError(7, "handler exploded")
    return body.upper()


@pytest.fixture
async def server_conn_holder() -> list[Connection]:
    return []


async def start_pair(
    *,
    config: TransportConfig | None = None,
    server_tls: TLS | None = None,
    client_tls: TLS | None = None,
    server_cluster: str = "test-cluster",
    client_cluster: str = "test-cluster",
    on_bulk: object = None,
) -> tuple[Server, Connection, list[Connection]]:
    inbound: list[Connection] = []

    async def on_connection(conn: Connection) -> None:
        inbound.append(conn)

    server = await Server.start(
        "127.0.0.1",
        0,
        local=make_local(server_cluster),
        config=config,
        tls=server_tls,
        on_connection=on_connection,
        on_request=echo_handler,
        on_bulk=on_bulk,  # type: ignore[arg-type]
    )
    client = await Connection.connect(
        "127.0.0.1",
        server.port,
        local=make_local(client_cluster),
        config=config,
        ssl_context=client_tls.client_context() if client_tls else None,
    )
    return server, client, inbound


async def test_ask_roundtrip_plaintext() -> None:
    server, client, _ = await start_pair()
    try:
        assert await client.ask(ECHO, b"hello") == b"HELLO"
    finally:
        await client.close()
        await server.close()


async def test_out_of_order_responses() -> None:
    server, client, _ = await start_pair()
    try:
        slow = asyncio.create_task(client.ask(SLOW, b"0.2"))
        fast = asyncio.create_task(client.ask(SLOW, b"0.01"))
        done, _pending = await asyncio.wait({slow, fast}, return_when=asyncio.FIRST_COMPLETED)
        assert fast in done and slow not in done  # response order inverted vs request order
        assert await slow == b"0.2" and await fast == b"0.01"
    finally:
        await client.close()
        await server.close()


async def test_remote_error_propagates() -> None:
    server, client, _ = await start_pair()
    try:
        with pytest.raises(RemoteError) as excinfo:
            await client.ask(BOOM, b"")
        assert excinfo.value.code == 7
    finally:
        await client.close()
        await server.close()


async def test_ask_timeout() -> None:
    server, client, _ = await start_pair()
    try:
        with pytest.raises(CastyTimeoutError):
            await client.ask(SLOW, b"5.0", timeout=0.05)
    finally:
        await client.close()
        await server.close()


async def test_tls_and_mtls_roundtrip(certs: Certs) -> None:
    server, client, _ = await start_pair(server_tls=certs.node_tls, client_tls=certs.node_tls)
    try:
        assert await client.ask(ECHO, b"secure") == b"SECURE"
    finally:
        await client.close()
        await server.close()


async def test_plaintext_client_against_tls_server_fails(certs: Certs) -> None:
    server, client, _ = await start_pair(server_tls=certs.node_tls, client_tls=certs.node_tls)
    try:
        with pytest.raises((CastyError, OSError)):
            await Connection.connect(
                "127.0.0.1",
                server.port,
                local=make_local(),
                config=TransportConfig(handshake_timeout=0.5, connect_timeout=0.5),
            )
    finally:
        await client.close()
        await server.close()


async def test_wrong_cluster_rejected() -> None:
    server, client, _ = await start_pair()
    try:
        with pytest.raises(HandshakeError) as excinfo:
            await Connection.connect(
                "127.0.0.1", server.port, local=make_local("other-cluster")
            )
        assert excinfo.value.code == 1
    finally:
        await client.close()
        await server.close()


async def test_connection_lost_fails_pending_asks() -> None:
    server, client, inbound = await start_pair()
    try:
        pending = asyncio.create_task(client.ask(SLOW, b"5.0"))
        await asyncio.sleep(0.05)
        await inbound[0].close()
        with pytest.raises(ConnectionLostError):
            await pending
    finally:
        await client.close()
        await server.close()


async def test_keepalive_detects_dead_peer() -> None:
    config = TransportConfig(keepalive_interval=0.1, keepalive_timeout=0.1)
    server, client, inbound = await start_pair(config=config)
    try:
        # freeze the server side: reads stop, so pings go unanswered
        assert inbound[0]._read_task is not None
        inbound[0]._read_task.cancel()
        await asyncio.wait_for(client.wait_closed(), timeout=2.0)
        assert client.is_closed
    finally:
        await client.close()
        await server.close()


async def test_bulk_transfer_with_interleaved_ask() -> None:
    received: list[bytes] = []
    done = asyncio.Event()

    async def on_bulk(conn: Connection, bulk: BulkReceiver) -> None:
        assert bulk.kind == "blob"
        async for chunk in bulk:
            received.append(chunk)
        done.set()

    config = TransportConfig(initial_window_bytes=64 * 1024, max_frame_bytes=16 * 1024)
    server, client, _ = await start_pair(config=config, on_bulk=on_bulk)
    try:
        payload = bytes(range(256)) * 4096  # 1 MiB
        sender = await client.open_bulk("blob", {"size": len(payload)})

        async def send_all() -> None:
            view = memoryview(payload)
            step = 64 * 1024
            for i in range(0, len(payload), step):
                await sender.send(bytes(view[i : i + step]))
            await sender.close()

        bulk_task = asyncio.create_task(send_all())
        # an ask completes while the 1 MiB transfer is in flight
        assert await client.ask(ECHO, b"ping") == b"PING"
        assert not bulk_task.done()
        await bulk_task
        await asyncio.wait_for(done.wait(), timeout=5.0)
        assert b"".join(received) == payload
    finally:
        await client.close()
        await server.close()


async def test_bulk_backpressure_slow_reader() -> None:
    progress: list[int] = []
    done = asyncio.Event()

    async def on_bulk(conn: Connection, bulk: BulkReceiver) -> None:
        async for chunk in bulk:
            progress.append(len(chunk))
            await asyncio.sleep(0.01)  # slow consumer
        done.set()

    config = TransportConfig(initial_window_bytes=8 * 1024, max_frame_bytes=4 * 1024)
    server, client, _ = await start_pair(config=config, on_bulk=on_bulk)
    try:
        payload = b"z" * (64 * 1024)
        sender = await client.open_bulk("throttled")
        sent_fast = 0

        async def send_all() -> None:
            nonlocal sent_fast
            step = 8 * 1024
            for i in range(0, len(payload), step):
                await sender.send(payload[i : i + step])
                sent_fast += step
            await sender.close()

        task = asyncio.create_task(send_all())
        await asyncio.sleep(0.05)
        # sender cannot be far ahead of the reader: window is 8 KiB
        assert sent_fast <= sum(progress) + 2 * 8 * 1024
        await task
        await asyncio.wait_for(done.wait(), timeout=5.0)
        assert sum(progress) == len(payload)
    finally:
        await client.close()
        await server.close()


async def test_compression_negotiated_and_used() -> None:
    config = TransportConfig(compression=CompressionConfig(codecs=["zlib"], min_bytes=128))
    server, client, _ = await start_pair(config=config)
    try:
        assert client.compression == "zlib"
        body = b"A" * 4096
        assert await client.ask(ECHO, body) == body.upper()
    finally:
        await client.close()
        await server.close()


async def test_compression_no_intersection_still_works() -> None:
    config = TransportConfig(compression=CompressionConfig(codecs=[], min_bytes=128))
    server, client, _ = await start_pair(config=config)
    try:
        assert client.compression is None
        assert await client.ask(ECHO, b"plain") == b"PLAIN"
    finally:
        await client.close()
        await server.close()
