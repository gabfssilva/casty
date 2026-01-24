import asyncio
import pytest
from unittest.mock import Mock, AsyncMock
from dataclasses import dataclass
from casty.cluster.sharded_ref import ShardedActorRef


@dataclass
class SampleMsg:
    value: int


@pytest.fixture
def mock_resolver():
    resolver = Mock()
    resolver.resolve_address = Mock(side_effect=lambda n: f"{n}.local:8080")
    resolver.get_leader_id = Mock(return_value="node-1")
    return resolver


@pytest.fixture
def mock_send():
    return AsyncMock(return_value=None)


@pytest.fixture
def mock_ask():
    return AsyncMock(return_value="ok")


@pytest.mark.asyncio
async def test_send_to_known_leader(mock_resolver, mock_send, mock_ask):
    ref = ShardedActorRef(
        actor_id="test-actor",
        resolver=mock_resolver,
        send_fn=mock_send,
        ask_fn=mock_ask,
    )
    ref.known_leader_id = "node-1"

    await ref.send(SampleMsg(42))

    mock_send.assert_called_once()
    call_args = mock_send.call_args
    assert call_args[0][0] == "node-1.local:8080"


@pytest.mark.asyncio
async def test_send_discovers_leader_from_resolver(mock_resolver, mock_send, mock_ask):
    ref = ShardedActorRef(
        actor_id="test-actor",
        resolver=mock_resolver,
        send_fn=mock_send,
        ask_fn=mock_ask,
    )

    await ref.send(SampleMsg(42))

    mock_resolver.get_leader_id.assert_called_once_with("test-actor")
    assert ref.known_leader_id == "node-1"


@pytest.mark.asyncio
async def test_ask_returns_response(mock_resolver, mock_send, mock_ask):
    mock_ask.return_value = "response-value"

    ref = ShardedActorRef(
        actor_id="test-actor",
        resolver=mock_resolver,
        send_fn=mock_send,
        ask_fn=mock_ask,
    )

    result = await ref.ask(SampleMsg(42))

    assert result == "response-value"


@pytest.mark.asyncio
async def test_operators(mock_resolver, mock_send, mock_ask):
    ref = ShardedActorRef(
        actor_id="test-actor",
        resolver=mock_resolver,
        send_fn=mock_send,
        ask_fn=mock_ask,
    )
    ref.known_leader_id = "node-1"

    await (ref >> SampleMsg(42))
    assert mock_send.call_count == 1

    mock_ask.return_value = 100
    result = await (ref << SampleMsg(10))
    assert result == 100
