import pytest
import asyncio
from unittest.mock import AsyncMock
from dataclasses import dataclass

from casty import ActorSystem, actor, Mailbox
from casty.remote.session import session_actor, SendLookup
from casty.remote.serializer import MsgPackSerializer
from casty.remote.protocol import RemoteEnvelope
from casty.remote.ref import SendDeliver
from casty.io.messages import Received, Write
from casty.remote.remote import _SessionConnected, _SessionDisconnected, _LocalLookup


@actor
async def mock_remote_actor(*, mailbox: Mailbox):
    """Mock remote actor that handles session registration and local lookups."""
    exposed: dict[str, any] = {}

    async for msg, ctx in mailbox:
        match msg:
            case _SessionConnected(_, _):
                pass
            case _SessionDisconnected(_):
                pass
            case _LocalLookup(name):
                await ctx.reply(exposed.get(name))
            case ("expose", name, ref):
                exposed[name] = ref


@pytest.mark.asyncio
async def test_session_send_deliver():
    """Test that session sends deliver envelope over connection."""
    async with ActorSystem() as system:
        serializer = MsgPackSerializer()
        mock_remote = await system.actor(mock_remote_actor(), name="mock-remote")

        connection = AsyncMock()
        session = await system.actor(
            session_actor(connection, mock_remote, serializer, "test-peer"),
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


@pytest.mark.asyncio
async def test_session_handles_deliver():
    """Test that session delivers incoming messages to exposed actors."""
    async with ActorSystem() as system:
        serializer = MsgPackSerializer()

        received_msgs = []

        @actor
        async def target_actor(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                received_msgs.append(msg)

        target = await system.actor(target_actor(), name="target")

        # Create mock remote that knows about the target
        mock_remote = await system.actor(mock_remote_actor(), name="mock-remote")
        await mock_remote.send(("expose", "target", target))

        connection = AsyncMock()
        session = await system.actor(
            session_actor(connection, mock_remote, serializer, "test-peer"),
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
async def test_session_handles_lookup():
    """Test that session responds to lookup requests."""
    async with ActorSystem() as system:
        serializer = MsgPackSerializer()

        @actor
        async def target_actor(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                pass

        target = await system.actor(target_actor(), name="target")

        mock_remote = await system.actor(mock_remote_actor(), name="mock-remote")
        await mock_remote.send(("expose", "target", target))

        connection = AsyncMock()
        session = await system.actor(
            session_actor(connection, mock_remote, serializer, "test-peer"),
            name="session"
        )

        envelope = RemoteEnvelope(
            type="lookup",
            name="target",
            correlation_id="test-123",
        )
        await session.send(Received(serializer.encode(envelope.to_dict())))
        await asyncio.sleep(0.05)

        # Should have sent a lookup_result back
        connection.send.assert_called()
        call_args = connection.send.call_args[0][0]
        assert isinstance(call_args, Write)

        # Decode and verify
        response = RemoteEnvelope.from_dict(serializer.decode(call_args.data))
        assert response.type == "lookup_result"
        assert response.correlation_id == "test-123"
        assert response.payload == b"1"  # exists


@pytest.mark.asyncio
async def test_session_handles_lookup_not_found():
    """Test that session responds to lookup for non-existent actor."""
    async with ActorSystem() as system:
        serializer = MsgPackSerializer()

        mock_remote = await system.actor(mock_remote_actor(), name="mock-remote")

        connection = AsyncMock()
        session = await system.actor(
            session_actor(connection, mock_remote, serializer, "test-peer"),
            name="session"
        )

        envelope = RemoteEnvelope(
            type="lookup",
            name="nonexistent",
            correlation_id="test-456",
        )
        await session.send(Received(serializer.encode(envelope.to_dict())))
        await asyncio.sleep(0.05)

        connection.send.assert_called()
        call_args = connection.send.call_args[0][0]

        response = RemoteEnvelope.from_dict(serializer.decode(call_args.data))
        assert response.type == "lookup_result"
        assert response.correlation_id == "test-456"
        assert response.payload == b"0"  # does not exist
