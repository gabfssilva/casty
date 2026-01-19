"""Tests for core actor primitives."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, ActorSystem, Context

from .conftest import (
    Counter,
    Decrement,
    EchoActor,
    ForwardTo,
    ForwarderActor,
    GetValue,
    Increment,
    Ping,
    Reset,
    SetValue,
)


class TestActorSystem:
    """Tests for ActorSystem basic operations."""

    @pytest.mark.asyncio
    async def test_actor_creation(self, system: ActorSystem):
        """Test creating an actor."""
        counter = await system.actor(Counter, name="test-counter")
        assert counter is not None
        assert counter.id is not None

    @pytest.mark.asyncio
    async def test_actor_with_name(self, system: ActorSystem):
        """Test creating an actor with a name."""
        counter = await system.actor(Counter, name="my-counter")
        assert "my-counter" in counter.id.name
        assert "my-counter" in str(counter.id)

    @pytest.mark.asyncio
    async def test_actor_with_kwargs(self, system: ActorSystem):
        """Test creating an actor with constructor arguments."""
        counter = await system.actor(Counter, name="counter-with-initial", initial=42)

        result = await counter.ask(GetValue())
        assert result == 42

    @pytest.mark.asyncio
    async def test_actor_lifecycle_start(self, system: ActorSystem):
        """Test that on_start is called."""
        counter = await system.actor(Counter, name="lifecycle-counter")

        # Give the actor a moment to start
        await asyncio.sleep(0.05)

        # We can't directly check started flag, but we can verify it responds
        result = await counter.ask(GetValue())
        assert result == 0


class TestActorSend:
    """Tests for fire-and-forget send pattern."""

    @pytest.mark.asyncio
    async def test_send_single_message(self, system: ActorSystem):
        """Test sending a single message."""
        counter = await system.actor(Counter, name="send-single-counter")

        await counter.send(Increment(5))
        await asyncio.sleep(0.05)  # Allow message to be processed

        result = await counter.ask(GetValue())
        assert result == 5

    @pytest.mark.asyncio
    async def test_send_multiple_messages(self, system: ActorSystem):
        """Test sending multiple messages in sequence."""
        counter = await system.actor(Counter, name="send-multiple-counter")

        await counter.send(Increment(1))
        await counter.send(Increment(2))
        await counter.send(Increment(3))
        await asyncio.sleep(0.05)

        result = await counter.ask(GetValue())
        assert result == 6

    @pytest.mark.asyncio
    async def test_send_different_message_types(self, system: ActorSystem):
        """Test sending different message types."""
        counter = await system.actor(Counter, name="send-different-types-counter", initial=10)

        await counter.send(Increment(5))
        await counter.send(Decrement(3))
        await asyncio.sleep(0.05)

        result = await counter.ask(GetValue())
        assert result == 12  # 10 + 5 - 3

    @pytest.mark.asyncio
    async def test_send_operator(self, system: ActorSystem):
        """Test >> operator for send."""
        counter = await system.actor(Counter, name="send-operator-counter")

        await (counter >> Increment(10))
        await asyncio.sleep(0.05)

        result = await counter.ask(GetValue())
        assert result == 10


class TestActorAsk:
    """Tests for request-response ask pattern."""

    @pytest.mark.asyncio
    async def test_ask_simple(self, system: ActorSystem):
        """Test simple ask pattern."""
        counter = await system.actor(Counter, name="ask-simple-counter", initial=42)

        result = await counter.ask(GetValue())
        assert result == 42

    @pytest.mark.asyncio
    async def test_ask_with_timeout(self, system: ActorSystem):
        """Test ask with custom timeout."""
        counter = await system.actor(Counter, name="ask-timeout-counter", initial=100)

        result = await counter.ask(GetValue(), timeout=1.0)
        assert result == 100

    @pytest.mark.asyncio
    async def test_ask_timeout_raises(self, system: ActorSystem):
        """Test that ask raises on timeout."""

        class SlowActor(Actor[Ping]):
            async def receive(self, msg: Ping, ctx: Context) -> None:
                await asyncio.sleep(10)  # Very slow
                await ctx.reply("done")

        slow = await system.actor(SlowActor, name="slow-actor")

        with pytest.raises(asyncio.TimeoutError):
            await slow.ask(Ping(), timeout=0.1)

    @pytest.mark.asyncio
    async def test_ask_operator(self, system: ActorSystem):
        """Test << operator for ask."""
        counter = await system.actor(Counter, name="ask-operator-counter", initial=77)

        result = await (counter << GetValue())
        assert result == 77

    @pytest.mark.asyncio
    async def test_echo_actor(self, system: ActorSystem):
        """Test actor that echoes messages."""
        echo = await system.actor(EchoActor, name="echo-actor")

        result = await echo.ask("hello")
        assert result == "hello"

        result = await echo.ask({"key": "value"})
        assert result == {"key": "value"}


class TestActorContext:
    """Tests for actor Context functionality."""

    @pytest.mark.asyncio
    async def test_context_self_ref(self, system: ActorSystem):
        """Test that context provides self reference."""

        @dataclass
        class GetSelfId:
            pass

        class SelfAwareActor(Actor[GetSelfId]):
            async def receive(self, msg: GetSelfId, ctx: Context) -> None:
                await ctx.reply(ctx.self_ref.id)

        actor = await system.actor(SelfAwareActor, name="self-aware")

        result = await actor.ask(GetSelfId())
        assert result == actor.id
        assert "self-aware" in result.name

    @pytest.mark.asyncio
    async def test_context_actor_child(self, system: ActorSystem):
        """Test creating child actors from context."""

        @dataclass
        class SpawnAndCount:
            pass

        class ParentActor(Actor[SpawnAndCount | GetValue]):
            def __init__(self):
                self.children_spawned = 0

            async def receive(
                self, msg: SpawnAndCount | GetValue, ctx: Context
            ) -> None:
                match msg:
                    case SpawnAndCount():
                        await ctx.actor(Counter, name=f"child-{self.children_spawned}")
                        self.children_spawned += 1
                        await ctx.reply(self.children_spawned)
                    case GetValue():
                        await ctx.reply(len(ctx.children))

        parent = await system.actor(ParentActor, name="parent-actor")

        # Spawn some children
        result = await parent.ask(SpawnAndCount())
        assert result == 1

        result = await parent.ask(SpawnAndCount())
        assert result == 2

        # Check children count
        children_count = await parent.ask(GetValue())
        assert children_count == 2

    @pytest.mark.asyncio
    async def test_context_reply_multiple_times(self, system: ActorSystem):
        """Test that only first reply is sent."""

        @dataclass
        class MultiReply:
            pass

        class MultiReplyActor(Actor[MultiReply]):
            async def receive(self, msg: MultiReply, ctx: Context) -> None:
                await ctx.reply("first")
                await ctx.reply("second")  # Should be ignored
                await ctx.reply("third")  # Should be ignored

        actor = await system.actor(MultiReplyActor, name="multi-reply-actor")

        result = await actor.ask(MultiReply())
        assert result == "first"


class TestActorCommunication:
    """Tests for inter-actor communication."""

    @pytest.mark.asyncio
    async def test_actor_to_actor_send(self, system: ActorSystem):
        """Test one actor sending to another."""
        counter = await system.actor(Counter, name="comm-counter")
        forwarder = await system.actor(ForwarderActor, name="comm-forwarder")

        # Forward an increment to the counter
        await forwarder.send(ForwardTo(counter, Increment(7)))
        await asyncio.sleep(0.05)

        result = await counter.ask(GetValue())
        assert result == 7

    @pytest.mark.asyncio
    async def test_message_ordering(self, system: ActorSystem):
        """Test that messages are processed in order."""
        counter = await system.actor(Counter, name="ordering-counter")

        # Send messages that depend on order
        await counter.send(SetValue(100))
        await counter.send(Increment(1))
        await counter.send(Increment(1))
        await counter.send(Decrement(50))
        await asyncio.sleep(0.05)

        result = await counter.ask(GetValue())
        assert result == 52  # 100 + 1 + 1 - 50


class TestActorShutdown:
    """Tests for graceful shutdown."""

    @pytest.mark.asyncio
    async def test_shutdown_stops_actors(self):
        """Test that shutdown stops all actors."""
        from casty.system import LocalSystem
        system = LocalSystem()

        counter1 = await system.actor(Counter, name="shutdown-counter-1")
        counter2 = await system.actor(Counter, name="shutdown-counter-2")

        await counter1.send(Increment(1))
        await counter2.send(Increment(2))
        await asyncio.sleep(0.05)

        await system.shutdown()

        # After shutdown, actors should be stopped
        assert len(system._actors) == 0

    @pytest.mark.asyncio
    async def test_context_manager_shutdown(self):
        """Test that context manager performs shutdown."""
        from casty.system import LocalSystem
        async with LocalSystem() as system:
            counter = await system.actor(Counter, name="context-manager-counter")
            await counter.send(Increment(5))
            await asyncio.sleep(0.05)
            result = await counter.ask(GetValue())
            assert result == 5

        # After exiting context, system should be shut down
        assert len(system._actors) == 0


class TestActorEquality:
    """Tests for ActorRef equality and hashing."""

    @pytest.mark.asyncio
    async def test_same_ref_equals(self, system: ActorSystem):
        """Test that same ref equals itself."""
        counter = await system.actor(Counter, name="equality-counter")
        assert counter == counter

    @pytest.mark.asyncio
    async def test_different_refs_not_equal(self, system: ActorSystem):
        """Test that different refs are not equal."""
        counter1 = await system.actor(Counter, name="equality-counter-1")
        counter2 = await system.actor(Counter, name="equality-counter-2")
        assert counter1 != counter2

    @pytest.mark.asyncio
    async def test_ref_hashable(self, system: ActorSystem):
        """Test that refs can be used in sets/dicts."""
        counter1 = await system.actor(Counter, name="hashable-counter-1")
        counter2 = await system.actor(Counter, name="hashable-counter-2")

        actors = {counter1, counter2}
        assert len(actors) == 2
        assert counter1 in actors
        assert counter2 in actors

        actor_map = {counter1: "first", counter2: "second"}
        assert actor_map[counter1] == "first"
        assert actor_map[counter2] == "second"
