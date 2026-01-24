from __future__ import annotations

import pytest
import asyncio
from types import SimpleNamespace
from casty.mailbox import ActorMailbox, Stop
from casty.envelope import Envelope


@pytest.mark.asyncio
async def test_actor_mailbox_basic_iteration():
    mailbox: ActorMailbox[str] = ActorMailbox()

    await mailbox.put(Envelope(payload="hello"))
    await mailbox.put(Envelope(payload="world"))
    await mailbox.put(Envelope(payload=Stop()))

    messages = []
    async for msg, ctx in mailbox:
        messages.append(msg)

    assert messages == ["hello", "world"]


@pytest.mark.asyncio
async def test_actor_mailbox_with_state():
    state = SimpleNamespace(count=0)
    mailbox: ActorMailbox[str] = ActorMailbox(state=state)

    await mailbox.put(Envelope(payload="msg"))
    await mailbox.put(Envelope(payload=Stop()))

    async for msg, ctx in mailbox:
        state.count = 42

    assert state.count == 42


@pytest.mark.asyncio
async def test_actor_mailbox_with_filter():
    state = SimpleNamespace(count=0)
    calls: list[str] = []

    async def tracking_filter(s, inner):
        async for msg, ctx in inner:
            calls.append(f"before:{msg}")
            yield msg, ctx
            calls.append(f"after:{msg}")

    mailbox: ActorMailbox[str] = ActorMailbox(
        state=state,
        filters=[tracking_filter],
    )

    await mailbox.put(Envelope(payload="a"))
    await mailbox.put(Envelope(payload="b"))
    await mailbox.put(Envelope(payload=Stop()))

    async for msg, ctx in mailbox:
        pass

    assert calls == ["before:a", "after:a", "before:b", "after:b"]


@pytest.mark.asyncio
async def test_actor_mailbox_multiple_filters():
    calls: list[str] = []

    async def filter_1(s, inner):
        async for msg, ctx in inner:
            calls.append(f"f1-before:{msg}")
            yield msg, ctx
            calls.append(f"f1-after:{msg}")

    async def filter_2(s, inner):
        async for msg, ctx in inner:
            calls.append(f"f2-before:{msg}")
            yield msg, ctx
            calls.append(f"f2-after:{msg}")

    mailbox: ActorMailbox[str] = ActorMailbox(filters=[filter_1, filter_2])

    await mailbox.put(Envelope(payload="x"))
    await mailbox.put(Envelope(payload=Stop()))

    async for msg, ctx in mailbox:
        calls.append(f"process:{msg}")

    assert calls == ["f1-before:x", "f2-before:x", "process:x", "f2-after:x", "f1-after:x"]


@pytest.mark.asyncio
async def test_actor_mailbox_context_fields():
    from casty.ref import LocalActorRef

    mailbox: ActorMailbox[str] = ActorMailbox(
        self_id="test-actor",
        node_id="node-1",
        is_leader=False,
    )

    sender_mailbox: ActorMailbox[str] = ActorMailbox()
    sender_ref = LocalActorRef[str](actor_id="other-actor", _deliver=sender_mailbox.put)

    await mailbox.put(Envelope(payload="msg", sender=sender_ref))
    await mailbox.put(Envelope(payload=Stop()))

    async for msg, ctx in mailbox:
        assert ctx.self_id == "test-actor"
        assert ctx.sender is sender_ref
        assert ctx.node_id == "node-1"
        assert ctx.is_leader is False


@pytest.mark.asyncio
async def test_actor_mailbox_set_is_leader():
    mailbox: ActorMailbox[str] = ActorMailbox(is_leader=True)

    assert mailbox._is_leader is True
    mailbox.set_is_leader(False)
    assert mailbox._is_leader is False


@pytest.mark.asyncio
async def test_actor_mailbox_state_property():
    state = SimpleNamespace(value=100)
    mailbox: ActorMailbox[str] = ActorMailbox(state=state)

    assert mailbox.state is state
    assert mailbox.state.value == 100


@pytest.mark.asyncio
async def test_actor_mailbox_no_state():
    mailbox: ActorMailbox[str] = ActorMailbox()
    assert mailbox.state is None


@pytest.mark.asyncio
async def test_actor_mailbox_set_self_ref():
    mailbox: ActorMailbox[str] = ActorMailbox()
    mock_ref = object()
    mailbox.set_self_ref(mock_ref)
    assert mailbox._self_ref is mock_ref


