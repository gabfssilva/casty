from __future__ import annotations

from casty.core.address import ActorAddress
from casty.core.messages import Terminated
from casty.ref import ActorRef
from casty.remote.ref import RemoteActorRef
from casty.core.transport import LocalTransport


def _dummy_ref() -> ActorRef[str]:
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/dummy")
    return RemoteActorRef(address=addr, _transport=transport)


async def test_terminated_holds_ref() -> None:
    ref = _dummy_ref()
    t = Terminated(ref=ref)
    assert t.ref is ref


async def test_terminated_is_frozen() -> None:
    import pytest

    ref = _dummy_ref()
    t = Terminated(ref=ref)
    with pytest.raises(AttributeError):
        t.ref = ref  # type: ignore[misc]
