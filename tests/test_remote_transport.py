from __future__ import annotations

import asyncio

from casty.remote_transport import MessageEnvelope, TcpTransport


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
