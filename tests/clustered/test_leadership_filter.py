import pytest
from unittest.mock import Mock, AsyncMock
from casty.cluster.replication.filter import leadership_filter


@pytest.fixture
def mock_shard_coordinator():
    coord = Mock()
    coord.is_leader = Mock(return_value=True)
    coord.get_leader_id = Mock(return_value="node-2")
    return coord


@pytest.fixture
def mock_context():
    ctx = Mock()
    ctx.reply = AsyncMock()
    return ctx


@pytest.mark.asyncio
async def test_leadership_filter_allows_when_leader(mock_shard_coordinator, mock_context):
    mock_shard_coordinator.is_leader.return_value = True

    filter_fn = leadership_filter(mock_shard_coordinator, "test-actor", 3)

    async def inner_stream(state):
        yield "msg1", mock_context
        yield "msg2", mock_context

    messages = []
    async for msg, ctx in filter_fn(None, inner_stream(None)):
        messages.append(msg)

    assert messages == ["msg1", "msg2"]
    mock_context.reply.assert_not_called()


@pytest.mark.asyncio
async def test_leadership_filter_drops_when_not_leader(mock_shard_coordinator, mock_context):
    mock_shard_coordinator.is_leader.return_value = False

    filter_fn = leadership_filter(mock_shard_coordinator, "test-actor", 3)

    async def inner_stream(state):
        yield "msg1", mock_context

    messages = []
    async for msg, ctx in filter_fn(None, inner_stream(None)):
        messages.append(msg)

    assert messages == []


@pytest.mark.asyncio
async def test_leadership_filter_checks_each_message(mock_shard_coordinator, mock_context):
    call_count = 0

    def is_leader_side_effect(actor_id, replicas):
        nonlocal call_count
        call_count += 1
        return call_count % 2 == 1

    mock_shard_coordinator.is_leader.side_effect = is_leader_side_effect

    filter_fn = leadership_filter(mock_shard_coordinator, "test-actor", 3)

    async def inner_stream(state):
        yield "msg1", mock_context
        yield "msg2", mock_context
        yield "msg3", mock_context

    messages = []
    async for msg, ctx in filter_fn(None, inner_stream(None)):
        messages.append(msg)

    assert messages == ["msg1", "msg3"]
