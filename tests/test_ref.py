from __future__ import annotations

from casty.address import ActorAddress
from casty.transport import LocalTransport
from casty.ref import ActorRef


async def test_tell_delivers_via_transport() -> None:
    received: list[str] = []
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/actor")
    transport.register("/actor", received.append)
    ref: ActorRef[str] = ActorRef(address=addr, _transport=transport)
    ref.tell("hello")
    assert received == ["hello"]


async def test_tell_preserves_message_order() -> None:
    received: list[int] = []
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/actor")
    transport.register("/actor", received.append)
    ref: ActorRef[int] = ActorRef(address=addr, _transport=transport)
    ref.tell(1)
    ref.tell(2)
    ref.tell(3)
    assert received == [1, 2, 3]


async def test_ref_is_frozen() -> None:
    import pytest

    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/actor")
    ref: ActorRef[str] = ActorRef(address=addr, _transport=transport)
    with pytest.raises(AttributeError):
        ref.address = addr  # type: ignore[misc]
