from __future__ import annotations

from casty.core.address import ActorAddress
from casty.core.transport import LocalTransport, MessageTransport


async def test_local_transport_implements_protocol() -> None:
    assert isinstance(LocalTransport(), MessageTransport)


async def test_local_transport_delivers_to_registered_cell() -> None:
    transport = LocalTransport()
    received: list[object] = []
    transport.register("/user/greeter", lambda msg: received.append(msg))

    addr = ActorAddress(system="test", path="/user/greeter")
    transport.deliver(addr, "hello")

    assert received == ["hello"]


async def test_local_transport_unregistered_path_buffers() -> None:
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/user/unknown")
    transport.deliver(addr, "hello")

    assert transport._pending["/user/unknown"] == ["hello"]


async def test_local_transport_unregister() -> None:
    transport = LocalTransport()
    received: list[object] = []
    transport.register("/user/greeter", lambda msg: received.append(msg))

    addr = ActorAddress(system="test", path="/user/greeter")
    transport.deliver(addr, "before")
    assert received == ["before"]

    transport.unregister("/user/greeter")
    transport.deliver(addr, "after")
    assert received == ["before"]
    assert "/user/greeter" not in transport._pending


async def test_local_transport_pending_flushed_on_register() -> None:
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/actor")
    transport.deliver(addr, "msg-1")
    transport.deliver(addr, "msg-2")

    received: list[object] = []
    transport.register("/actor", lambda msg: received.append(msg))

    assert received == ["msg-1", "msg-2"]
    assert "/actor" not in transport._pending


async def test_local_transport_pending_respects_max_buffer() -> None:
    transport = LocalTransport(max_pending_per_path=2)
    addr = ActorAddress(system="test", path="/actor")
    transport.deliver(addr, "a")
    transport.deliver(addr, "b")
    transport.deliver(addr, "c")

    assert transport._pending["/actor"] == ["a", "b"]

    received: list[object] = []
    transport.register("/actor", lambda msg: received.append(msg))
    assert received == ["a", "b"]
