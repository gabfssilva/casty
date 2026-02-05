from __future__ import annotations

from casty.ref import ActorRef


async def test_tell_calls_internal_send() -> None:
    sent: list[str] = []
    ref: ActorRef[str] = ActorRef(_send=sent.append)
    ref.tell("hello")
    assert sent == ["hello"]


async def test_tell_preserves_message_order() -> None:
    sent: list[int] = []
    ref: ActorRef[int] = ActorRef(_send=sent.append)
    ref.tell(1)
    ref.tell(2)
    ref.tell(3)
    assert sent == [1, 2, 3]


async def test_ref_is_frozen() -> None:
    import pytest

    ref: ActorRef[str] = ActorRef(_send=lambda m: None)
    with pytest.raises(AttributeError):
        ref._send = lambda m: None  # type: ignore[misc]
