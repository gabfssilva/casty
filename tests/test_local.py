"""Functional tests for local ActorSystem."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, ActorSystem, Context

from conftest import Counter, Decrement, GetValue, Increment, Reset


class TestSpawn:
    """Tests for actor spawning."""

    async def test_spawn_creates_actor(self) -> None:
        async with ActorSystem() as system:
            ref = await system.spawn(Counter)

            assert ref is not None
            assert ref.id is not None

    async def test_spawn_with_name(self) -> None:
        async with ActorSystem() as system:
            ref = await system.spawn(Counter, name="my-counter")

            assert ref.id.name == "my-counter"

    async def test_spawn_with_initial_value(self) -> None:
        async with ActorSystem() as system:
            ref = await system.spawn(Counter, initial=100)

            value = await ref.ask(GetValue())
            assert value == 100


class TestSend:
    """Tests for fire-and-forget messaging."""

    async def test_send_increment(self) -> None:
        async with ActorSystem() as system:
            ref = await system.spawn(Counter)

            await ref.send(Increment(5))
            value = await ref.ask(GetValue())

            assert value == 5

    async def test_send_multiple_messages(self) -> None:
        async with ActorSystem() as system:
            ref = await system.spawn(Counter)

            await ref.send(Increment(10))
            await ref.send(Increment(5))
            await ref.send(Decrement(3))

            value = await ref.ask(GetValue())
            assert value == 12  # 10 + 5 - 3


class TestAsk:
    """Tests for request-response pattern."""

    async def test_ask_get_value(self) -> None:
        async with ActorSystem() as system:
            ref = await system.spawn(Counter, initial=42)

            value = await ref.ask(GetValue())

            assert value == 42

    async def test_ask_reset(self) -> None:
        async with ActorSystem() as system:
            ref = await system.spawn(Counter, initial=100)

            result = await ref.ask(Reset())

            assert result == 0

    async def test_ask_timeout(self) -> None:
        @dataclass
        class SlowMessage:
            pass

        class SlowActor(Actor[SlowMessage]):
            async def receive(self, msg: SlowMessage, ctx: Context) -> None:
                await asyncio.sleep(5)  # Slow processing
                ctx.reply("done")

        async with ActorSystem() as system:
            ref = await system.spawn(SlowActor)

            with pytest.raises(asyncio.TimeoutError):
                await ref.ask(SlowMessage(), timeout=0.1)


class TestShutdown:
    """Tests for graceful shutdown."""

    async def test_shutdown_stops_actors(self) -> None:
        system = ActorSystem()
        await system.spawn(Counter)
        await system.spawn(Counter)

        await system.shutdown()

        assert len(system._actors) == 0

    async def test_shutdown_from_actor(self) -> None:
        @dataclass
        class ShutdownRequest:
            pass

        class ShutdownActor(Actor[ShutdownRequest]):
            async def receive(self, msg: ShutdownRequest, ctx: Context) -> None:
                await ctx.system.shutdown()

        system = ActorSystem()
        ref = await system.spawn(ShutdownActor)

        await ref.send(ShutdownRequest())
        await asyncio.sleep(0.2)

        assert system._running is False

    async def test_context_manager_calls_shutdown(self) -> None:
        """Verify that exiting context manager calls shutdown."""
        system = ActorSystem()
        async with system:
            await system.spawn(Counter)
            await system.spawn(Counter)
            assert system._running is True

        assert system._running is False
        assert len(system._actors) == 0


class TestLifecycleHooks:
    """Tests for actor lifecycle hooks."""

    async def test_on_start_called(self) -> None:
        started = []

        @dataclass
        class Ping:
            pass

        class StartActor(Actor[Ping]):
            async def on_start(self) -> None:
                started.append(True)

            async def receive(self, msg: Ping, ctx: Context) -> None:
                pass

        async with ActorSystem() as system:
            await system.spawn(StartActor)
            await asyncio.sleep(0.05)
            assert len(started) == 1

    async def test_on_stop_called(self) -> None:
        stopped = []

        @dataclass
        class Ping:
            pass

        class StopActor(Actor[Ping]):
            async def on_stop(self) -> None:
                stopped.append(True)

            async def receive(self, msg: Ping, ctx: Context) -> None:
                pass

        async with ActorSystem() as system:
            await system.spawn(StopActor)

        await asyncio.sleep(0.1)
        assert len(stopped) == 1

    async def test_on_error_continue(self) -> None:
        @dataclass
        class BadMessage:
            pass

        @dataclass
        class GoodMessage:
            pass

        class ErrorActor(Actor[BadMessage | GoodMessage]):
            def __init__(self) -> None:
                self.processed = 0

            def on_error(self, exc: Exception) -> bool:
                return True  # Continue after error

            async def receive(self, msg: BadMessage | GoodMessage, ctx: Context) -> None:
                if isinstance(msg, BadMessage):
                    raise ValueError("Bad!")
                self.processed += 1
                ctx.reply(self.processed)

        async with ActorSystem() as system:
            ref = await system.spawn(ErrorActor)

            await ref.send(BadMessage())  # Should not kill actor
            result = await ref.ask(GoodMessage())

            assert result == 1
