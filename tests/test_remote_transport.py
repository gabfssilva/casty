from __future__ import annotations

import asyncio
from dataclasses import dataclass

from casty.address import ActorAddress
from casty.ref import ActorRef
from casty.remote_transport import MessageEnvelope, RemoteTransport, TcpTransport
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

    # Update local_port after start so routing works correctly
    remote_a._local_port = port_a
    remote_b._local_port = port_b

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

        # Manually clear connection to simulate stale connection
        transport_a._connections.pop(("127.0.0.1", port_b), None)

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
