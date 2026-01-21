import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from dataclasses import dataclass

from casty import ActorSystem, actor, Mailbox
from casty.remote.session import session_actor, SendLookup
from casty.remote.registry import registry_actor
from casty.remote.serializer import MsgPackSerializer
from casty.remote.protocol import RemoteEnvelope
from casty.remote.messages import Expose, Lookup
from casty.remote.ref import SendDeliver, SendAsk
from casty.io.messages import Received, Write


@dataclass
class Increment:
    amount: int


@dataclass
class Get:
    pass


@pytest.mark.asyncio
async def test_session_handles_deliver():
    async with ActorSystem() as system:
        serializer = MsgPackSerializer()

        received_msgs = []

        @actor
        async def target_actor(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                received_msgs.append(msg)

        target = await system.actor(target_actor(), name="target")
        registry = await system.actor(registry_actor(serializer), name="registry")
        await registry.ask(Expose(ref=target, name="target"))

        connection = AsyncMock()
        session = await system.actor(
            session_actor(connection, registry, serializer),
            name="session"
        )

        envelope = RemoteEnvelope(
            type="deliver",
            target="target",
            payload=serializer.encode({"value": 42}),
        )
        await session.send(Received(serializer.encode(envelope.to_dict())))
        await asyncio.sleep(0.05)

        assert len(received_msgs) == 1
        assert received_msgs[0] == {"value": 42}


@pytest.mark.asyncio
async def test_session_send_deliver():
    async with ActorSystem() as system:
        serializer = MsgPackSerializer()
        registry = await system.actor(registry_actor(serializer), name="registry")

        connection = AsyncMock()
        session = await system.actor(
            session_actor(connection, registry, serializer),
            name="session"
        )

        await session.send(SendDeliver(
            target="counter",
            payload=serializer.encode({"inc": 1}),
        ))
        await asyncio.sleep(0.01)

        connection.send.assert_called_once()
        call_args = connection.send.call_args[0][0]
        assert isinstance(call_args, Write)
