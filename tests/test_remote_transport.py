from __future__ import annotations

import asyncio
import ssl
import struct
from dataclasses import dataclass

import pytest
import trustme

from casty.address import ActorAddress
from casty.cluster_state import NodeAddress
from casty.ref import ActorRef
from casty.remote_transport import (
    FRAME_HANDSHAKE,
    FRAME_MESSAGE,
    GetPort,
    InboundHandler,
    MessageEnvelope,
    RemoteTransport,
    SendToNode,
    TcpTransportConfig,
    TcpTransportMsg,
    tcp_transport,
)
from casty.shard_coordinator_actor import GetShardLocation, ShardLocation
from casty.serialization import JsonSerializer, TypeRegistry
from casty.system import ActorSystem
from casty.transport import LocalTransport


@dataclass(frozen=True)
class Ping:
    value: int


@dataclass(frozen=True)
class Pong:
    value: int


class _PlaceholderHandler:
    """Test handler that either delegates to a ``RemoteTransport`` or collects raw bytes."""

    def __init__(self) -> None:
        self.received: list[bytes] = []
        self.delegate: InboundHandler | None = None

    async def on_message(self, data: bytes) -> None:
        if self.delegate is not None:
            await self.delegate.on_message(data)
        else:
            self.received.append(data)


async def _spawn_tcp(
    system: ActorSystem,
    handler: _PlaceholderHandler,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    client_only: bool = False,
    address_map: dict[tuple[str, int], tuple[str, int]] | None = None,
    server_ssl: ssl.SSLContext | None = None,
    client_ssl: ssl.SSLContext | None = None,
) -> tuple[ActorRef[TcpTransportMsg], int]:
    """Spawn a ``tcp_transport`` actor and return its ref + resolved port."""
    config = TcpTransportConfig(
        host=host,
        port=port,
        client_only=client_only,
        server_ssl=server_ssl,
        client_ssl=client_ssl,
        address_map=address_map or {},
    )
    ref = system.spawn(tcp_transport(config, handler), "_tcp")
    actual_port: int = await system.ask(ref, lambda r: GetPort(reply_to=r), timeout=5.0)
    return ref, actual_port


async def _start_remote_pair() -> tuple[
    ActorSystem, ActorSystem,
    RemoteTransport, RemoteTransport,
    LocalTransport, LocalTransport,
    int, int,
]:
    """Create two actor systems with transport actors and wired RemoteTransports."""
    sys_a = ActorSystem(name="sys-a")
    sys_b = ActorSystem(name="sys-b")
    await sys_a.__aenter__()
    await sys_b.__aenter__()

    ph_a = _PlaceholderHandler()
    ph_b = _PlaceholderHandler()

    ref_a, port_a = await _spawn_tcp(sys_a, ph_a)
    ref_b, port_b = await _spawn_tcp(sys_b, ph_b)

    local_a: LocalTransport = sys_a._local_transport  # pyright: ignore[reportPrivateUsage]
    local_b: LocalTransport = sys_b._local_transport  # pyright: ignore[reportPrivateUsage]

    remote_a = RemoteTransport(
        local=local_a,
        tcp=ref_a,
        serializer=JsonSerializer(TypeRegistry()),
        local_host="127.0.0.1",
        local_port=port_a,
        system_name="sys-a",
    )
    remote_b = RemoteTransport(
        local=local_b,
        tcp=ref_b,
        serializer=JsonSerializer(TypeRegistry()),
        local_host="127.0.0.1",
        local_port=port_b,
        system_name="sys-b",
    )

    ph_a.delegate = remote_a
    ph_b.delegate = remote_b

    return sys_a, sys_b, remote_a, remote_b, local_a, local_b, port_a, port_b


async def test_envelope_roundtrip() -> None:
    envelope = MessageEnvelope(
        target="casty://sys@localhost:25520/actor",
        sender="casty://sys@localhost:25521/sender",
        payload=b'{"_type":"Greet","name":"Alice"}',
        type_hint="test.Greet",
    )
    data = envelope.to_bytes()
    restored = MessageEnvelope.from_bytes(data)
    assert restored == envelope


async def test_tcp_transport_send_receive() -> None:
    handler_a = _PlaceholderHandler()
    handler_b = _PlaceholderHandler()

    sys_a = ActorSystem(name="test-a")
    sys_b = ActorSystem(name="test-b")
    await sys_a.__aenter__()
    await sys_b.__aenter__()

    ref_a, port_a = await _spawn_tcp(sys_a, handler_a)
    _, port_b = await _spawn_tcp(sys_b, handler_b)

    try:
        envelope = MessageEnvelope(
            target=f"casty://sys@127.0.0.1:{port_b}/actor",
            sender=f"casty://sys@127.0.0.1:{port_a}/sender",
            payload=b"hello",
            type_hint="str",
        )
        ref_a.tell(SendToNode(host="127.0.0.1", port=port_b, data=envelope.to_bytes()))
        await asyncio.sleep(0.2)
        assert len(handler_b.received) == 1
        restored = MessageEnvelope.from_bytes(handler_b.received[0])
        assert restored.payload == b"hello"
    finally:
        await sys_a.shutdown()
        await sys_b.shutdown()


async def test_tcp_transport_connection_reuse() -> None:
    handler_a = _PlaceholderHandler()
    handler_b = _PlaceholderHandler()

    sys_a = ActorSystem(name="test-a")
    sys_b = ActorSystem(name="test-b")
    await sys_a.__aenter__()
    await sys_b.__aenter__()

    ref_a, port_a = await _spawn_tcp(sys_a, handler_a)
    _, port_b = await _spawn_tcp(sys_b, handler_b)

    try:
        for i in range(5):
            envelope = MessageEnvelope(
                target=f"casty://sys@127.0.0.1:{port_b}/actor",
                sender=f"casty://sys@127.0.0.1:{port_a}/sender",
                payload=f"msg-{i}".encode(),
                type_hint="str",
            )
            ref_a.tell(SendToNode(host="127.0.0.1", port=port_b, data=envelope.to_bytes()))
        await asyncio.sleep(0.3)
        assert len(handler_b.received) == 5
    finally:
        await sys_a.shutdown()
        await sys_b.shutdown()


async def test_remote_transport_local_delivery() -> None:
    """Local delivery goes through LocalTransport."""
    sys = ActorSystem(name="test")
    await sys.__aenter__()

    handler = _PlaceholderHandler()
    ref, port = await _spawn_tcp(sys, handler, client_only=True)
    local: LocalTransport = sys._local_transport  # pyright: ignore[reportPrivateUsage]

    remote = RemoteTransport(
        local=local,
        tcp=ref,
        serializer=JsonSerializer(TypeRegistry()),
        local_host="127.0.0.1",
        local_port=25520,
        system_name="test",
    )

    try:
        received: list[object] = []
        local.register("/test-actor", lambda msg: received.append(msg))

        addr = ActorAddress(system="test", path="/test-actor", host="127.0.0.1", port=25520)
        remote.deliver(addr, Ping(value=42))

        assert len(received) == 1
        assert isinstance(received[0], Ping)
        assert received[0].value == 42
    finally:
        await sys.shutdown()


async def test_remote_transport_tcp_delivery() -> None:
    """Two RemoteTransports: message from A arrives at B's local handler."""
    sys_a, sys_b, remote_a, _, _, local_b, _, port_b = await _start_remote_pair()

    try:
        received: list[object] = []
        local_b.register("/target-actor", lambda msg: received.append(msg))

        target_addr = ActorAddress(
            system="sys-b", path="/target-actor", host="127.0.0.1", port=port_b,
        )
        remote_a.deliver(target_addr, Ping(value=99))

        await asyncio.sleep(0.3)

        assert len(received) == 1
        assert isinstance(received[0], Ping)
        assert received[0].value == 99
    finally:
        await sys_a.shutdown()
        await sys_b.shutdown()


async def test_remote_transport_actor_ref_roundtrip() -> None:
    """Send message containing ActorRef, verify reply comes back via TCP."""
    sys_a, sys_b, remote_a, _, local_a, local_b, port_a, port_b = await _start_remote_pair()

    try:
        received_on_a: list[object] = []
        local_a.register("/reply", lambda msg: received_on_a.append(msg))

        reply_addr = ActorAddress(
            system="sys-a", path="/reply", host="127.0.0.1", port=port_a,
        )
        reply_ref = ActorRef[ShardLocation](address=reply_addr, _transport=remote_a)

        received_on_b: list[object] = []

        def handle_on_b(msg: object) -> None:
            received_on_b.append(msg)
            match msg:
                case GetShardLocation(shard_id=shard_id, reply_to=reply_to):
                    reply_to.tell(
                        ShardLocation(
                            shard_id=shard_id,
                            node=NodeAddress(host="127.0.0.1", port=port_b),
                        )
                    )
                case _:
                    pass

        local_b.register("/coordinator", handle_on_b)

        coord_addr = ActorAddress(
            system="sys-b", path="/coordinator", host="127.0.0.1", port=port_b,
        )
        remote_a.deliver(coord_addr, GetShardLocation(shard_id=5, reply_to=reply_ref))

        await asyncio.sleep(0.5)

        assert len(received_on_b) == 1
        assert isinstance(received_on_b[0], GetShardLocation)
        assert received_on_b[0].shard_id == 5

        assert len(received_on_a) == 1
        assert isinstance(received_on_a[0], ShardLocation)
        assert received_on_a[0].shard_id == 5
    finally:
        await sys_a.shutdown()
        await sys_b.shutdown()


async def test_tcp_transport_reconnect_after_peer_shutdown() -> None:
    """Transport connects to a new peer after the old one shuts down."""
    handler_a = _PlaceholderHandler()

    sys_a = ActorSystem(name="test-a")
    await sys_a.__aenter__()
    ref_a, _ = await _spawn_tcp(sys_a, handler_a)

    try:
        handler_b1 = _PlaceholderHandler()
        sys_b1 = ActorSystem(name="test-b1")
        await sys_b1.__aenter__()
        _, port_b1 = await _spawn_tcp(sys_b1, handler_b1)

        ref_a.tell(SendToNode(host="127.0.0.1", port=port_b1, data=b"msg1"))
        await asyncio.sleep(0.1)
        assert len(handler_b1.received) == 1

        await sys_b1.shutdown()
        await asyncio.sleep(0.3)

        handler_b2 = _PlaceholderHandler()
        sys_b2 = ActorSystem(name="test-b2")
        await sys_b2.__aenter__()
        _, port_b2 = await _spawn_tcp(sys_b2, handler_b2)

        ref_a.tell(SendToNode(host="127.0.0.1", port=port_b2, data=b"msg2"))
        await asyncio.sleep(0.2)
        assert len(handler_b2.received) == 1

        await sys_b2.shutdown()
    finally:
        await sys_a.shutdown()
        await asyncio.sleep(0)


@pytest.fixture()
def tls_contexts() -> tuple[ssl.SSLContext, ssl.SSLContext]:
    """Generate TLS contexts for testing using trustme."""
    ca = trustme.CA()
    server_cert = ca.issue_cert("127.0.0.1")

    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_cert.configure_cert(server_ctx)
    ca.configure_trust(server_ctx)

    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ca.configure_trust(client_ctx)

    return server_ctx, client_ctx


async def _read_raw_frame(reader: asyncio.StreamReader) -> bytes:
    """Read a single length-prefixed frame from a raw TCP stream."""
    length_bytes = await reader.readexactly(4)
    msg_len = struct.unpack("!I", length_bytes)[0]
    return await reader.readexactly(msg_len)


def _make_server_handshake_frame(host: str, port: int) -> bytes:
    """Build a handshake frame that a raw test server can send back."""
    import json as _json

    payload = _json.dumps({"host": host, "port": port}).encode("utf-8")
    frame = bytes([FRAME_HANDSHAKE]) + payload
    return struct.pack("!I", len(frame)) + frame


async def test_tcp_address_map_translates_on_send() -> None:
    """Transport with address_map connects to the mapped address, not the logical one."""
    received: list[bytes] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        handshake = await _read_raw_frame(reader)
        assert handshake[0] == FRAME_HANDSHAKE
        writer.write(_make_server_handshake_frame("127.0.0.1", actual_port))
        await writer.drain()
        frame = await _read_raw_frame(reader)
        assert frame[0] == FRAME_MESSAGE
        received.append(frame[1:])

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    actual_port = server.sockets[0].getsockname()[1]

    sys = ActorSystem(name="test")
    await sys.__aenter__()
    handler = _PlaceholderHandler()
    ref, _ = await _spawn_tcp(
        sys,
        handler,
        client_only=True,
        address_map={("10.0.1.10", 25520): ("127.0.0.1", actual_port)},
    )

    try:
        ref.tell(SendToNode(host="10.0.1.10", port=25520, data=b"hello-through-tunnel"))
        await asyncio.sleep(0.1)
        assert received == [b"hello-through-tunnel"]
    finally:
        await sys.shutdown()
        server.close()
        await server.wait_closed()


async def test_remote_transport_sender_uses_advertised_address() -> None:
    """MessageEnvelope.sender uses advertised address, not bind address."""
    captured_data: list[bytes] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _read_raw_frame(reader)
        writer.write(_make_server_handshake_frame("127.0.0.1", actual_port))
        await writer.drain()
        frame = await _read_raw_frame(reader)
        captured_data.append(frame[1:])

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    actual_port = server.sockets[0].getsockname()[1]

    sys = ActorSystem(name="test-sys")
    await sys.__aenter__()

    handler = _PlaceholderHandler()
    ref, _ = await _spawn_tcp(sys, handler, client_only=True)
    local: LocalTransport = sys._local_transport  # pyright: ignore[reportPrivateUsage]

    remote = RemoteTransport(
        local=local,
        tcp=ref,
        serializer=JsonSerializer(TypeRegistry()),
        local_host="127.0.0.1",
        local_port=5000,
        system_name="test-sys",
        advertised_host="bastion.example.com",
        advertised_port=9999,
    )
    handler.delegate = remote

    try:
        addr = ActorAddress(
            system="remote-sys",
            path="/target",
            host="127.0.0.1",
            port=actual_port,
        )
        remote.deliver(addr, Ping(value=42))
        await asyncio.sleep(0.1)

        assert len(captured_data) == 1
        envelope = MessageEnvelope.from_bytes(captured_data[0])
        assert "bastion.example.com" in envelope.sender
        assert "9999" in envelope.sender
        assert "5000" not in envelope.sender
    finally:
        await sys.shutdown()
        server.close()
        await server.wait_closed()


async def test_tcp_transport_tls(tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]) -> None:
    server_ctx, client_ctx = tls_contexts

    handler_a = _PlaceholderHandler()
    handler_b = _PlaceholderHandler()

    sys_a = ActorSystem(name="test-a")
    sys_b = ActorSystem(name="test-b")
    await sys_a.__aenter__()
    await sys_b.__aenter__()

    ref_a, port_a = await _spawn_tcp(sys_a, handler_a, server_ssl=server_ctx, client_ssl=client_ctx)
    _, port_b = await _spawn_tcp(sys_b, handler_b, server_ssl=server_ctx, client_ssl=client_ctx)

    try:
        envelope = MessageEnvelope(
            target=f"casty://sys@127.0.0.1:{port_b}/actor",
            sender=f"casty://sys@127.0.0.1:{port_a}/sender",
            payload=b"hello-tls",
            type_hint="str",
        )
        ref_a.tell(SendToNode(host="127.0.0.1", port=port_b, data=envelope.to_bytes()))
        await asyncio.sleep(0.2)
        assert len(handler_b.received) == 1
        restored = MessageEnvelope.from_bytes(handler_b.received[0])
        assert restored.payload == b"hello-tls"
    finally:
        await sys_a.shutdown()
        await sys_b.shutdown()


async def test_tcp_bidirectional_communication() -> None:
    """A sends to B, B sends to A — verify bidirectional delivery."""
    handler_a = _PlaceholderHandler()
    handler_b = _PlaceholderHandler()

    sys_a = ActorSystem(name="test-a")
    sys_b = ActorSystem(name="test-b")
    await sys_a.__aenter__()
    await sys_b.__aenter__()

    ref_a, port_a = await _spawn_tcp(sys_a, handler_a)
    ref_b, port_b = await _spawn_tcp(sys_b, handler_b)

    try:
        ref_a.tell(SendToNode(host="127.0.0.1", port=port_b, data=b"hello-from-a"))
        await asyncio.sleep(0.2)
        assert len(handler_b.received) == 1
        assert handler_b.received[0] == b"hello-from-a"

        ref_b.tell(SendToNode(host="127.0.0.1", port=port_a, data=b"hello-from-b"))
        await asyncio.sleep(0.2)
        assert len(handler_a.received) == 1
        assert handler_a.received[0] == b"hello-from-b"
    finally:
        await sys_a.shutdown()
        await sys_b.shutdown()


async def test_tcp_simultaneous_connect_race() -> None:
    """Both sides send concurrently — race resolves, then both directions work."""
    handler_a = _PlaceholderHandler()
    handler_b = _PlaceholderHandler()

    sys_a = ActorSystem(name="test-a")
    sys_b = ActorSystem(name="test-b")
    await sys_a.__aenter__()
    await sys_b.__aenter__()

    ref_a, port_a = await _spawn_tcp(sys_a, handler_a)
    ref_b, port_b = await _spawn_tcp(sys_b, handler_b)

    try:
        ref_a.tell(SendToNode(host="127.0.0.1", port=port_b, data=b"race-a"))
        ref_b.tell(SendToNode(host="127.0.0.1", port=port_a, data=b"race-b"))
        await asyncio.sleep(0.3)

        ref_a.tell(SendToNode(host="127.0.0.1", port=port_b, data=b"from-a"))
        ref_b.tell(SendToNode(host="127.0.0.1", port=port_a, data=b"from-b"))
        await asyncio.sleep(0.2)

        assert b"from-a" in handler_b.received
        assert b"from-b" in handler_a.received
    finally:
        await sys_a.shutdown()
        await sys_b.shutdown()


async def test_tcp_hostname_ip_alias_reuses_peer() -> None:
    """Sending via hostname then via IP both deliver successfully."""
    handler_a = _PlaceholderHandler()
    handler_b = _PlaceholderHandler()

    sys_a = ActorSystem(name="test-a")
    sys_b = ActorSystem(name="test-b")
    await sys_a.__aenter__()
    await sys_b.__aenter__()

    ref_a, _ = await _spawn_tcp(sys_a, handler_a)
    _, port_b = await _spawn_tcp(sys_b, handler_b)

    try:
        ref_a.tell(SendToNode(host="localhost", port=port_b, data=b"via-hostname"))
        await asyncio.sleep(0.2)
        assert len(handler_b.received) == 1

        ref_a.tell(SendToNode(host="127.0.0.1", port=port_b, data=b"via-ip"))
        await asyncio.sleep(0.2)
        assert len(handler_b.received) == 2
    finally:
        await sys_a.shutdown()
        await sys_b.shutdown()
