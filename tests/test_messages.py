from __future__ import annotations

from casty.messages import Terminated
from casty.ref import ActorRef


async def test_terminated_holds_ref() -> None:
    ref: ActorRef[str] = ActorRef(_send=lambda m: None)
    t = Terminated(ref=ref)
    assert t.ref is ref


async def test_terminated_is_frozen() -> None:
    import pytest

    ref: ActorRef[str] = ActorRef(_send=lambda m: None)
    t = Terminated(ref=ref)
    with pytest.raises(AttributeError):
        t.ref = ref  # type: ignore[misc]
