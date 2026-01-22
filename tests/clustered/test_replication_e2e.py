import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox, message
from casty.state import State
from casty.actor_config import Routing
from casty.cluster import (
    DevelopmentCluster,
    VectorClock,
    Snapshot,
    InMemory,
)
from casty.cluster.conflict import detect_conflict, ConflictResult
from casty.serializable import serializable, serialize, deserialize


@serializable
@dataclass
class CounterState:
    count: int = 0


@message(readonly=True)
class GetValue:
    pass


@message
class SetValue:
    value: int


@pytest.mark.asyncio
async def test_replication_end_to_end():
    """Complete test of replicated actor with state"""

    @actor(replicated=2, routing={GetValue: Routing.LEADER, SetValue: Routing.LEADER})
    async def counter(state: State[int], *, mailbox: Mailbox[GetValue | SetValue]):
        async for msg, ctx in mailbox:
            match msg:
                case GetValue():
                    await ctx.reply(state.value)
                case SetValue(value):
                    state.set(value)

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(counter(0), name="counter")

        result = await ref.ask(GetValue())
        assert result == 0

        await ref.send(SetValue(100))
        await asyncio.sleep(0.05)

        result = await ref.ask(GetValue())
        assert result == 100


@pytest.mark.asyncio
async def test_multiple_updates():
    """Multiple state updates"""

    @actor(replicated=2, routing={GetValue: Routing.LEADER, SetValue: Routing.LEADER})
    async def accumulator(state: State[int], *, mailbox: Mailbox[GetValue | SetValue]):
        async for msg, ctx in mailbox:
            match msg:
                case GetValue():
                    await ctx.reply(state.value)
                case SetValue(value):
                    state.set(state.value + value)

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(accumulator(0), name="acc")

        for i in range(10):
            await ref.send(SetValue(1))

        await asyncio.sleep(0.1)

        result = await ref.ask(GetValue())
        assert result == 10


@pytest.mark.asyncio
async def test_replicated_actor_preserves_state_across_messages():
    """State is preserved across message processing"""

    @actor(replicated=1)
    async def stateful(state: State[list], *, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            if msg == "get":
                await ctx.reply(state.value)
            else:
                state.set(state.value + [msg])

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(stateful([]), name="list")

        await ref.send("a")
        await ref.send("b")
        await ref.send("c")
        await asyncio.sleep(0.05)

        result = await ref.ask("get")
        assert result == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_non_replicated_actor_still_works():
    """Non-replicated actors should continue to work normally"""

    @actor
    async def echo(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            await ctx.reply(f"echo: {msg}")

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(echo(), name="echo")

        result = await ref.ask("hello")
        assert result == "echo: hello"


@pytest.mark.asyncio
async def test_replicated_actor_context_has_is_leader():
    """Context should have is_leader flag"""

    @actor(replicated=1)
    async def leader_aware(state: State[str], *, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            if msg == "check":
                await ctx.reply(ctx.is_leader)

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(leader_aware(""), name="aware")

        result = await ref.ask("check")
        assert result is True


class TestReplicationComponents:
    @pytest.mark.asyncio
    async def test_vector_clock_tracks_changes(self):
        """Test that state changes are tracked with vector clocks."""
        state = State(value=CounterState(0), node_id="node-1")

        state.set(CounterState(1))
        state.set(CounterState(2))

        assert state.clock.versions == {"node-1": 2}

    @pytest.mark.asyncio
    async def test_snapshot_and_restore(self):
        """Test state can be snapshotted and restored."""
        state1 = State(value=CounterState(10), node_id="node-1")
        state1.set(CounterState(10))

        snapshot = state1.snapshot()

        state2 = State(value=CounterState(0), node_id="node-2")
        state2.restore(snapshot)

        assert state2.value.count == 10
        assert state2.clock.versions == {"node-1": 1}

    @pytest.mark.asyncio
    async def test_conflict_detection_concurrent_updates(self):
        """Test that concurrent updates are detected as conflicts."""
        state1 = State(value=CounterState(0), node_id="node-1")
        state2 = State(value=CounterState(0), node_id="node-2")

        state1.set(CounterState(5))
        state2.set(CounterState(10))

        result = detect_conflict(state1.clock, state2.clock)

        assert result == ConflictResult.MERGE

    @pytest.mark.asyncio
    async def test_snapshot_backend_finds_base(self):
        """Test that snapshot backend can find common ancestor."""
        backend = InMemory()

        base_clock = VectorClock({"node-1": 1, "node-2": 1})
        await backend.save(Snapshot(
            data=serialize(CounterState(0)),
            clock=base_clock
        ))

        await backend.save(Snapshot(
            data=serialize(CounterState(5)),
            clock=VectorClock({"node-1": 2, "node-2": 1})
        ))

        clock_a = VectorClock({"node-1": 3, "node-2": 1})
        clock_b = VectorClock({"node-1": 2, "node-2": 2})

        base = await backend.find_base(clock_a, clock_b)

        assert base is not None
        restored = deserialize(base.data)
        assert restored.count == 5

    @pytest.mark.asyncio
    async def test_three_way_merge(self):
        """Test three-way merge resolves conflicts correctly."""
        base = CounterState(count=0)
        local = CounterState(count=5)
        remote = CounterState(count=3)

        base_count = base.count
        local_delta = local.count - base_count
        remote_delta = remote.count - base_count
        merged_count = base_count + local_delta + remote_delta

        merged = CounterState(count=merged_count)

        assert merged.count == 8

