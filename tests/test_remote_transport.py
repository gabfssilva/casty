from __future__ import annotations

import asyncio
import ssl
import struct
from dataclasses import dataclass

import pytest
import trustme

from casty.address import ActorAddress
from casty.ref import ActorRef
from casty.remote_transport import FRAME_HANDSHAKE, FRAME_MESSAGE, MessageEnvelope, RemoteTransport, TcpTransport
from casty.serialization import JsonSerializer, TypeRegistry
from casty.transport import LocalTransport
from casty.shard_coordinator_actor import GetShardLocation, ShardLocation
from casty.cluster_state import NodeAddress


@dataclass(frozen=True)
class Ping:
    value: int


@dataclass(frozen=True)
class Pong:
    value: int


async def _start_remote_pair() -> tuple[RemoteTransport, RemoteTransport, LocalTransport, LocalTransport, int, int]:
    """Create and start a pair of RemoteTransports, returning actual ports."""
    registry_a = TypeRegistry()
    registry_b = TypeRegistry()

    local_a = LocalTransport()
    local_b = LocalTransport()

    tcp_a = TcpTransport(host="127.0.0.1", port=0)
    tcp_b = TcpTransport(host="127.0.0.1", port=0)

    serializer_a = JsonSerializer(registry_a)
    serializer_b = JsonSerializer(registry_b)

    # Use port=0 placeholder; we'll update _local_port after start
    remote_a = RemoteTransport(
        local=local_a, tcp=tcp_a, serializer=serializer_a,
        local_host="127.0.0.1", local_port=0, system_name="sys-a",
    )
    remote_b = RemoteTransport(
        local=local_b, tcp=tcp_b, serializer=serializer_b,
        local_host="127.0.0.1", local_port=0, system_name="sys-b",
    )

    await remote_a.start()
    await remote_b.start()

    port_a = tcp_a.port
    port_b = tcp_b.port

    # Update local_port and self_address after start so routing works correctly
    remote_a._local_port = port_a
    remote_b._local_port = port_b
    tcp_a.set_self_address("127.0.0.1", port_a)
    tcp_b.set_self_address("127.0.0.1", port_b)

    return remote_a, remote_b, local_a, local_b, port_a, port_b


# --- Original tests ---


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
    received: list[bytes] = []

    class Handler:
        async def on_message(self, data: bytes) -> None:
            received.append(data)

    transport_a = TcpTransport(host="127.0.0.1", port=0)
    transport_b = TcpTransport(host="127.0.0.1", port=0)

    await transport_a.start(Handler())
    await transport_b.start(Handler())

    try:
        envelope = MessageEnvelope(
            target=f"casty://sys@127.0.0.1:{transport_b.port}/actor",
            sender=f"casty://sys@127.0.0.1:{transport_a.port}/sender",
            payload=b"hello",
            type_hint="str",
        )
        await transport_a.send("127.0.0.1", transport_b.port, envelope.to_bytes())
        await asyncio.sleep(0.2)
        assert len(received) == 1
        restored = MessageEnvelope.from_bytes(received[0])
        assert restored.payload == b"hello"
    finally:
        await transport_a.stop()
        await transport_b.stop()


async def test_tcp_transport_connection_reuse() -> None:
    received: list[bytes] = []

    class Handler:
        async def on_message(self, data: bytes) -> None:
            received.append(data)

    transport_a = TcpTransport(host="127.0.0.1", port=0)
    transport_b = TcpTransport(host="127.0.0.1", port=0)

    await transport_a.start(Handler())
    await transport_b.start(Handler())

    try:
        for i in range(5):
            envelope = MessageEnvelope(
                target=f"casty://sys@127.0.0.1:{transport_b.port}/actor",
                sender=f"casty://sys@127.0.0.1:{transport_a.port}/sender",
                payload=f"msg-{i}".encode(),
                type_hint="str",
            )
            await transport_a.send("127.0.0.1", transport_b.port, envelope.to_bytes())
        await asyncio.sleep(0.3)
        assert len(received) == 5
    finally:
        await transport_a.stop()
        await transport_b.stop()


# --- Phase 2: RemoteTransport tests ---


async def test_remote_transport_local_delivery() -> None:
    """Local delivery goes through LocalTransport."""
    registry = TypeRegistry()

    local = LocalTransport()
    tcp = TcpTransport(host="127.0.0.1", port=0)
    serializer = JsonSerializer(registry)

    remote = RemoteTransport(
        local=local, tcp=tcp, serializer=serializer,
        local_host="127.0.0.1", local_port=25520, system_name="test",
    )

    received: list[object] = []
    local.register("/test-actor", lambda msg: received.append(msg))

    addr = ActorAddress(system="test", path="/test-actor", host="127.0.0.1", port=25520)
    remote.deliver(addr, Ping(value=42))

    assert len(received) == 1
    assert isinstance(received[0], Ping)
    assert received[0].value == 42


async def test_remote_transport_tcp_delivery() -> None:
    """Two RemoteTransports: message from A arrives at B's local handler."""
    remote_a, remote_b, local_a, local_b, port_a, port_b = await _start_remote_pair()

    try:
        received: list[object] = []
        local_b.register("/target-actor", lambda msg: received.append(msg))

        target_addr = ActorAddress(
            system="sys-b", path="/target-actor", host="127.0.0.1", port=port_b
        )
        remote_a.deliver(target_addr, Ping(value=99))

        await asyncio.sleep(0.3)

        assert len(received) == 1
        assert isinstance(received[0], Ping)
        assert received[0].value == 99
    finally:
        await remote_a.stop()
        await remote_b.stop()


async def test_remote_transport_actor_ref_roundtrip() -> None:
    """Send message containing ActorRef, verify reply comes back via TCP."""
    remote_a, remote_b, local_a, local_b, port_a, port_b = await _start_remote_pair()

    try:
        # Set up reply handler on A
        received_on_a: list[object] = []
        local_a.register("/reply", lambda msg: received_on_a.append(msg))

        # Create a reply ref that points to A
        reply_addr = ActorAddress(
            system="sys-a", path="/reply", host="127.0.0.1", port=port_a
        )
        reply_ref = ActorRef[ShardLocation](address=reply_addr, _transport=remote_a)

        # Set up coordinator handler on B that replies
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

        local_b.register("/coordinator", handle_on_b)

        # Send GetShardLocation from A to B's coordinator
        coord_addr = ActorAddress(
            system="sys-b", path="/coordinator", host="127.0.0.1", port=port_b
        )
        remote_a.deliver(coord_addr, GetShardLocation(shard_id=5, reply_to=reply_ref))

        await asyncio.sleep(0.5)

        # B should have received the request
        assert len(received_on_b) == 1
        assert isinstance(received_on_b[0], GetShardLocation)
        assert received_on_b[0].shard_id == 5

        # A should have received the reply
        assert len(received_on_a) == 1
        assert isinstance(received_on_a[0], ShardLocation)
        assert received_on_a[0].shard_id == 5
    finally:
        await remote_a.stop()
        await remote_b.stop()


async def test_tcp_transport_connection_retry() -> None:
    """Connection retry after drop."""
    received: list[bytes] = []

    class Handler:
        async def on_message(self, data: bytes) -> None:
            received.append(data)

    transport_b = TcpTransport(host="127.0.0.1", port=0)
    await transport_b.start(Handler())
    port_b = transport_b.port

    transport_a = TcpTransport(host="127.0.0.1", port=0)
    await transport_a.start(Handler())

    try:
        envelope = MessageEnvelope(
            target=f"casty://sys@127.0.0.1:{port_b}/actor",
            sender="casty://sys@127.0.0.1:0/sender",
            payload=b"msg1",
            type_hint="str",
        )
        await transport_a.send("127.0.0.1", port_b, envelope.to_bytes())
        await asyncio.sleep(0.1)
        assert len(received) == 1

        # Manually clear peer to simulate stale connection
        peer = transport_a._peers.pop(("127.0.0.1", port_b), None)
        if peer is not None:
            peer.read_task.cancel()
            peer.writer.close()

        envelope2 = MessageEnvelope(
            target=f"casty://sys@127.0.0.1:{port_b}/actor",
            sender="casty://sys@127.0.0.1:0/sender",
            payload=b"msg2",
            type_hint="str",
        )
        await transport_a.send("127.0.0.1", port_b, envelope2.to_bytes())
        await asyncio.sleep(0.2)
        assert len(received) == 2
    finally:
        await transport_a.stop()
        await transport_b.stop()


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
    """TcpTransport with address_map connects to the mapped address, not the logical one."""
    received: list[bytes] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Read client handshake
        handshake = await _read_raw_frame(reader)
        assert handshake[0] == FRAME_HANDSHAKE
        # Send server handshake back
        writer.write(_make_server_handshake_frame("127.0.0.1", actual_port))
        await writer.drain()
        # Read message frame
        frame = await _read_raw_frame(reader)
        assert frame[0] == FRAME_MESSAGE
        received.append(frame[1:])

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    actual_port = server.sockets[0].getsockname()[1]

    tcp = TcpTransport(
        "127.0.0.1", 0,
        address_map={("10.0.1.10", 25520): ("127.0.0.1", actual_port)},
    )

    try:
        await tcp.send("10.0.1.10", 25520, b"hello-through-tunnel")
        await asyncio.sleep(0.1)
        assert received == [b"hello-through-tunnel"]
    finally:
        await tcp.stop()
        server.close()
        await server.wait_closed()


async def test_remote_transport_sender_uses_advertised_address() -> None:
    """MessageEnvelope.sender uses advertised address, not bind address."""
    captured_data: list[bytes] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Read client handshake
        await _read_raw_frame(reader)
        # Send server handshake back
        writer.write(_make_server_handshake_frame("127.0.0.1", actual_port))
        await writer.drain()
        # Read message frame
        frame = await _read_raw_frame(reader)
        captured_data.append(frame[1:])  # strip frame_type byte

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    actual_port = server.sockets[0].getsockname()[1]

    local = LocalTransport()
    registry = TypeRegistry()
    tcp = TcpTransport("127.0.0.1", 0)
    serializer = JsonSerializer(registry)

    remote = RemoteTransport(
        local=local,
        tcp=tcp,
        serializer=serializer,
        local_host="127.0.0.1",
        local_port=5000,
        system_name="test-sys",
        advertised_host="bastion.example.com",
        advertised_port=9999,
    )

    await remote.start()
    try:
        addr = ActorAddress(
            system="remote-sys", path="/target",
            host="127.0.0.1", port=actual_port,
        )
        await remote._send_remote(addr, Ping(value=42))
        await asyncio.sleep(0.1)

        assert len(captured_data) == 1
        envelope = MessageEnvelope.from_bytes(captured_data[0])
        assert "bastion.example.com" in envelope.sender
        assert "9999" in envelope.sender
        assert "5000" not in envelope.sender
    finally:
        await remote.stop()
        server.close()
        await server.wait_closed()


async def test_tcp_transport_tls(tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]) -> None:
    server_ctx, client_ctx = tls_contexts

    received: list[bytes] = []

    class Handler:
        async def on_message(self, data: bytes) -> None:
            received.append(data)

    transport_a = TcpTransport(host="127.0.0.1", port=0, server_ssl=server_ctx, client_ssl=client_ctx)
    transport_b = TcpTransport(host="127.0.0.1", port=0, server_ssl=server_ctx, client_ssl=client_ctx)

    await transport_a.start(Handler())
    await transport_b.start(Handler())

    try:
        envelope = MessageEnvelope(
            target=f"casty://sys@127.0.0.1:{transport_b.port}/actor",
            sender=f"casty://sys@127.0.0.1:{transport_a.port}/sender",
            payload=b"hello-tls",
            type_hint="str",
        )
        await transport_a.send("127.0.0.1", transport_b.port, envelope.to_bytes())
        await asyncio.sleep(0.2)
        assert len(received) == 1
        restored = MessageEnvelope.from_bytes(received[0])
        assert restored.payload == b"hello-tls"
    finally:
        await transport_a.stop()
        await transport_b.stop()


# --- Bidirectional connection tests ---


async def test_tcp_bidirectional_communication() -> None:
    """A sends to B, B sends to A — verify single peer each direction."""
    received_on_a: list[bytes] = []
    received_on_b: list[bytes] = []

    class HandlerA:
        async def on_message(self, data: bytes) -> None:
            received_on_a.append(data)

    class HandlerB:
        async def on_message(self, data: bytes) -> None:
            received_on_b.append(data)

    transport_a = TcpTransport(host="127.0.0.1", port=0)
    transport_b = TcpTransport(host="127.0.0.1", port=0)

    await transport_a.start(HandlerA())
    await transport_b.start(HandlerB())

    transport_a.set_self_address("127.0.0.1", transport_a.port)
    transport_b.set_self_address("127.0.0.1", transport_b.port)

    try:
        # A sends to B
        await transport_a.send("127.0.0.1", transport_b.port, b"hello-from-a")
        await asyncio.sleep(0.2)
        assert len(received_on_b) == 1
        assert received_on_b[0] == b"hello-from-a"

        # B sends to A — should reuse the inbound connection from A
        await transport_b.send("127.0.0.1", transport_a.port, b"hello-from-b")
        await asyncio.sleep(0.2)
        assert len(received_on_a) == 1
        assert received_on_a[0] == b"hello-from-b"

        # Verify 1 peer on each side
        assert len(transport_a._peers) == 1  # pyright: ignore[reportPrivateUsage]
        assert len(transport_b._peers) == 1  # pyright: ignore[reportPrivateUsage]
    finally:
        await transport_a.stop()
        await transport_b.stop()


async def test_tcp_simultaneous_connect_race() -> None:
    """Both sides send concurrently — race resolves to 1 peer each, then both directions work."""
    received_on_a: list[bytes] = []
    received_on_b: list[bytes] = []

    class HandlerA:
        async def on_message(self, data: bytes) -> None:
            received_on_a.append(data)

    class HandlerB:
        async def on_message(self, data: bytes) -> None:
            received_on_b.append(data)

    transport_a = TcpTransport(host="127.0.0.1", port=0)
    transport_b = TcpTransport(host="127.0.0.1", port=0)

    await transport_a.start(HandlerA())
    await transport_b.start(HandlerB())

    transport_a.set_self_address("127.0.0.1", transport_a.port)
    transport_b.set_self_address("127.0.0.1", transport_b.port)

    try:
        # Both send simultaneously to trigger a race
        await asyncio.gather(
            transport_a.send("127.0.0.1", transport_b.port, b"race-a"),
            transport_b.send("127.0.0.1", transport_a.port, b"race-b"),
        )
        await asyncio.sleep(0.3)

        # Race dedup: exactly 1 peer on each side
        assert len(transport_a._peers) == 1  # pyright: ignore[reportPrivateUsage]
        assert len(transport_b._peers) == 1  # pyright: ignore[reportPrivateUsage]

        # After race resolution, bidirectional communication works
        await transport_a.send("127.0.0.1", transport_b.port, b"from-a")
        await transport_b.send("127.0.0.1", transport_a.port, b"from-b")
        await asyncio.sleep(0.2)

        assert b"from-a" in received_on_b
        assert b"from-b" in received_on_a

        # Still exactly 1 peer each
        assert len(transport_a._peers) == 1  # pyright: ignore[reportPrivateUsage]
        assert len(transport_b._peers) == 1  # pyright: ignore[reportPrivateUsage]
    finally:
        await transport_a.stop()
        await transport_b.stop()


async def test_tcp_hostname_ip_alias_reuses_peer() -> None:
    """Sending via hostname then via IP reuses the same connection (alias)."""
    received: list[bytes] = []

    class Handler:
        async def on_message(self, data: bytes) -> None:
            received.append(data)

    transport_a = TcpTransport(host="127.0.0.1", port=0)
    transport_b = TcpTransport(host="127.0.0.1", port=0)

    await transport_a.start(Handler())
    await transport_b.start(Handler())

    transport_a.set_self_address("127.0.0.1", transport_a.port)
    transport_b.set_self_address("127.0.0.1", transport_b.port)

    try:
        # First send via "localhost" — connects and learns canonical (127.0.0.1, port_b)
        await transport_a.send("localhost", transport_b.port, b"via-hostname")
        await asyncio.sleep(0.2)
        assert len(received) == 1

        # Second send via "127.0.0.1" — should reuse via alias, no new connection
        await transport_a.send("127.0.0.1", transport_b.port, b"via-ip")
        await asyncio.sleep(0.2)
        assert len(received) == 2

        # Only 1 peer on A (canonical address), with an alias for the hostname
        assert len(transport_a._peers) == 1  # pyright: ignore[reportPrivateUsage]
        alias = transport_a._peer_aliases  # pyright: ignore[reportPrivateUsage]
        assert ("localhost", transport_b.port) in alias
        assert alias[("localhost", transport_b.port)] == ("127.0.0.1", transport_b.port)
    finally:
        await transport_a.stop()
        await transport_b.stop()
