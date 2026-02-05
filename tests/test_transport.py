from __future__ import annotations

from casty.address import ActorAddress
from casty.transport import LocalTransport, MessageTransport


async def test_local_transport_implements_protocol() -> None:
    assert isinstance(LocalTransport(), MessageTransport)


async def test_local_transport_delivers_to_registered_cell() -> None:
    transport = LocalTransport()
    received: list[object] = []
    transport.register("/user/greeter", lambda msg: received.append(msg))

    addr = ActorAddress(system="test", path="/user/greeter")
    transport.deliver(addr, "hello")

    assert received == ["hello"]


async def test_local_transport_unregistered_path_is_noop() -> None:
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/user/unknown")
    # Should not raise
    transport.deliver(addr, "hello")


async def test_local_transport_unregister() -> None:
    transport = LocalTransport()
    received: list[object] = []
    transport.register("/user/greeter", lambda msg: received.append(msg))

    addr = ActorAddress(system="test", path="/user/greeter")
    transport.deliver(addr, "before")
    assert received == ["before"]

    transport.unregister("/user/greeter")
    transport.deliver(addr, "after")
    # Nothing new delivered after unregister
    assert received == ["before"]
