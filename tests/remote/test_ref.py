import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from dataclasses import dataclass

from casty.remote.ref import RemoteRef, SendDeliver, SendAsk
from casty.remote.serializer import MsgPackSerializer


@dataclass
class SampleMsg:
    value: int


@pytest.mark.asyncio
async def test_remote_ref_send():
    session = AsyncMock()
    serializer = MsgPackSerializer()

    ref = RemoteRef(
        actor_id="remote/counter",
        name="counter",
        _session=session,
        _serializer=serializer,
    )

    await ref.send({"value": 42})

    session.send.assert_called_once()
    call_args = session.send.call_args[0][0]
    assert isinstance(call_args, SendDeliver)
    assert call_args.target == "counter"


@pytest.mark.asyncio
async def test_remote_ref_ask():
    session = AsyncMock()
    serializer = MsgPackSerializer()
    session.ask = AsyncMock(return_value=serializer.encode({"result": 100}))

    ref = RemoteRef(
        actor_id="remote/counter",
        name="counter",
        _session=session,
        _serializer=serializer,
    )

    result = await ref.ask({"get": True})

    assert result == {"result": 100}
    session.ask.assert_called_once()


def test_remote_ref_has_actor_id():
    session = MagicMock()
    serializer = MsgPackSerializer()

    ref = RemoteRef(
        actor_id="remote/counter",
        name="counter",
        _session=session,
        _serializer=serializer,
    )

    assert ref.actor_id == "remote/counter"


@pytest.mark.asyncio
async def test_remote_ref_send_with_sender():
    session = AsyncMock()
    serializer = MsgPackSerializer()
    sender = MagicMock()
    sender.actor_id = "local/my-actor"

    ref = RemoteRef(
        actor_id="remote/counter",
        name="counter",
        _session=session,
        _serializer=serializer,
    )

    await ref.send({"value": 1}, sender=sender)

    call_args = session.send.call_args[0][0]
    assert call_args.sender_name == "local/my-actor"
