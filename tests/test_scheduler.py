"""Tests for Scheduler actor."""

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty import Actor, ActorSystem, Context
from casty.scheduler import Schedule, Scheduler


@dataclass
class Tick:
    """Custom timeout message."""
    pass


@dataclass
class GetCount:
    """Request number of ticks received."""
    pass


@dataclass
class GetReceived:
    """Request if any tick was received."""
    pass


class TickCollector(Actor[Any]):
    """Actor that collects tick notifications."""

    def __init__(self):
        self.tick_count = 0
        self.received_tick = False

    async def receive(self, msg: Any, ctx: Context) -> None:
        match msg:
            case Tick():
                self.tick_count += 1
                self.received_tick = True
            case GetCount():
                await ctx.reply(self.tick_count)
            case GetReceived():
                await ctx.reply(self.received_tick)


class TestSchedulerBasic:
    """Tests for basic Scheduler functionality."""

    @pytest.mark.asyncio
    async def test_schedule_timeout(self, system: ActorSystem):
        """Test that a scheduled message is delivered."""
        scheduler = await system.spawn(Scheduler)
        collector = await system.spawn(TickCollector)

        await scheduler.send(Schedule(timeout=0, listener=collector, message=Tick()))
        await asyncio.sleep(0.1)

        received = await collector.ask(GetReceived())
        assert received is True

    @pytest.mark.asyncio
    async def test_schedule_timeout_with_delay(self, system: ActorSystem):
        """Test that scheduled message respects the delay."""
        scheduler = await system.spawn(Scheduler)
        collector = await system.spawn(TickCollector)

        await scheduler.send(Schedule(timeout=0.2, listener=collector, message=Tick()))

        # Should not have received yet
        await asyncio.sleep(0.05)
        received = await collector.ask(GetReceived())
        assert received is False

        # Should receive after delay
        await asyncio.sleep(0.25)
        received = await collector.ask(GetReceived())
        assert received is True

    @pytest.mark.asyncio
    async def test_multiple_schedules(self, system: ActorSystem):
        """Test multiple schedules to same listener."""
        scheduler = await system.spawn(Scheduler)
        collector = await system.spawn(TickCollector)

        await scheduler.send(Schedule(timeout=0, listener=collector, message=Tick()))
        await scheduler.send(Schedule(timeout=0, listener=collector, message=Tick()))
        await scheduler.send(Schedule(timeout=0, listener=collector, message=Tick()))
        await asyncio.sleep(0.15)

        count = await collector.ask(GetCount())
        assert count == 3

    @pytest.mark.asyncio
    async def test_multiple_listeners(self, system: ActorSystem):
        """Test scheduling to multiple different listeners."""
        scheduler = await system.spawn(Scheduler)
        collector1 = await system.spawn(TickCollector)
        collector2 = await system.spawn(TickCollector)
        collector3 = await system.spawn(TickCollector)

        await scheduler.send(Schedule(timeout=0, listener=collector1, message=Tick()))
        await scheduler.send(Schedule(timeout=0, listener=collector2, message=Tick()))
        await scheduler.send(Schedule(timeout=0, listener=collector3, message=Tick()))
        await asyncio.sleep(0.15)

        count1 = await collector1.ask(GetCount())
        count2 = await collector2.ask(GetCount())
        count3 = await collector3.ask(GetCount())

        assert count1 == 1
        assert count2 == 1
        assert count3 == 1


class TestSchedulerConcurrency:
    """Tests for concurrent scheduling."""

    @pytest.mark.asyncio
    async def test_concurrent_short_timeouts(self, system: ActorSystem):
        """Test many concurrent short timeouts."""
        scheduler = await system.spawn(Scheduler)
        collector = await system.spawn(TickCollector)

        # Schedule many timeouts concurrently
        for _ in range(10):
            await scheduler.send(Schedule(timeout=0.01, listener=collector, message=Tick()))

        # Wait for all to complete
        await asyncio.sleep(0.2)

        count = await collector.ask(GetCount())
        assert count == 10

    @pytest.mark.asyncio
    async def test_concurrent_varied_timeouts(self, system: ActorSystem):
        """Test concurrent timeouts with varied delays."""
        scheduler = await system.spawn(Scheduler)
        collector = await system.spawn(TickCollector)

        # Schedule with different delays
        await scheduler.send(Schedule(timeout=0.1, listener=collector, message=Tick()))
        await scheduler.send(Schedule(timeout=0.05, listener=collector, message=Tick()))
        await scheduler.send(Schedule(timeout=0.15, listener=collector, message=Tick()))

        # Wait for all to complete
        await asyncio.sleep(0.25)

        count = await collector.ask(GetCount())
        assert count == 3


class TestSchedulerStaggeredTimeouts:
    """Tests for staggered timeout delivery."""

    @pytest.mark.asyncio
    async def test_staggered_delivery(self, system: ActorSystem):
        """Test that timeouts are delivered at correct times."""
        scheduler = await system.spawn(Scheduler)
        collector = await system.spawn(TickCollector)

        await scheduler.send(Schedule(timeout=0.05, listener=collector, message=Tick()))
        await scheduler.send(Schedule(timeout=0.15, listener=collector, message=Tick()))
        await scheduler.send(Schedule(timeout=0.25, listener=collector, message=Tick()))

        # After first timeout
        await asyncio.sleep(0.08)
        count = await collector.ask(GetCount())
        assert count == 1

        # After second timeout
        await asyncio.sleep(0.1)
        count = await collector.ask(GetCount())
        assert count == 2

        # After third timeout
        await asyncio.sleep(0.15)
        count = await collector.ask(GetCount())
        assert count == 3


class TestSchedulerCustomMessages:
    """Tests for custom message types."""

    @pytest.mark.asyncio
    async def test_custom_message_delivered(self, system: ActorSystem):
        """Test that custom messages are delivered correctly."""

        @dataclass
        class CustomAlert:
            code: int
            text: str

        @dataclass
        class GetAlerts:
            pass

        class AlertCollector(Actor[Any]):
            def __init__(self):
                self.alerts: list[CustomAlert] = []

            async def receive(self, msg: Any, ctx: Context) -> None:
                match msg:
                    case CustomAlert():
                        self.alerts.append(msg)
                    case GetAlerts():
                        await ctx.reply(self.alerts)

        scheduler = await system.spawn(Scheduler)
        collector = await system.spawn(AlertCollector)

        await scheduler.send(Schedule(
            timeout=0,
            listener=collector,
            message=CustomAlert(code=42, text="hello")
        ))
        await asyncio.sleep(0.1)

        alerts = await collector.ask(GetAlerts())
        assert len(alerts) == 1
        assert alerts[0].code == 42
        assert alerts[0].text == "hello"

    @pytest.mark.asyncio
    async def test_different_message_types(self, system: ActorSystem):
        """Test scheduling different message types to same listener."""

        @dataclass
        class MsgA:
            pass

        @dataclass
        class MsgB:
            pass

        @dataclass
        class GetMessages:
            pass

        class MultiCollector(Actor[Any]):
            def __init__(self):
                self.a_count = 0
                self.b_count = 0

            async def receive(self, msg: Any, ctx: Context) -> None:
                match msg:
                    case MsgA():
                        self.a_count += 1
                    case MsgB():
                        self.b_count += 1
                    case GetMessages():
                        await ctx.reply((self.a_count, self.b_count))

        scheduler = await system.spawn(Scheduler)
        collector = await system.spawn(MultiCollector)

        await scheduler.send(Schedule(timeout=0, listener=collector, message=MsgA()))
        await scheduler.send(Schedule(timeout=0, listener=collector, message=MsgB()))
        await scheduler.send(Schedule(timeout=0, listener=collector, message=MsgA()))
        await asyncio.sleep(0.15)

        a_count, b_count = await collector.ask(GetMessages())
        assert a_count == 2
        assert b_count == 1


class TestSchedulerOperatorSyntax:
    """Tests for >> operator syntax."""

    @pytest.mark.asyncio
    async def test_schedule_with_operator(self, system: ActorSystem):
        """Test scheduling using >> operator."""
        scheduler = await system.spawn(Scheduler)
        collector = await system.spawn(TickCollector)

        await (scheduler >> Schedule(timeout=0, listener=collector, message=Tick()))
        await asyncio.sleep(0.1)

        received = await collector.ask(GetReceived())
        assert received is True


class TestActorRefSchedule:
    """Tests for ActorRef.schedule() convenience method."""

    @pytest.mark.asyncio
    async def test_schedule_to_self(self, system: ActorSystem):
        """Test scheduling a message to self (default listener)."""
        collector = await system.spawn(TickCollector)

        # Schedule to self (no listener specified)
        await collector.schedule(timeout=0.05, message=Tick())

        # Should not have received yet
        received = await collector.ask(GetReceived())
        assert received is False

        # Wait for timeout
        await asyncio.sleep(0.1)

        received = await collector.ask(GetReceived())
        assert received is True

    @pytest.mark.asyncio
    async def test_schedule_to_other_listener(self, system: ActorSystem):
        """Test scheduling a message to another actor."""
        source = await system.spawn(TickCollector)
        target = await system.spawn(TickCollector)

        # Schedule from source but deliver to target
        await source.schedule(timeout=0.05, message=Tick(), listener=target)
        await asyncio.sleep(0.1)

        # Source should NOT have received
        source_received = await source.ask(GetReceived())
        assert source_received is False

        # Target SHOULD have received
        target_received = await target.ask(GetReceived())
        assert target_received is True

    @pytest.mark.asyncio
    async def test_schedule_multiple_to_self(self, system: ActorSystem):
        """Test scheduling multiple messages to self."""
        collector = await system.spawn(TickCollector)

        await collector.schedule(timeout=0.01, message=Tick())
        await collector.schedule(timeout=0.02, message=Tick())
        await collector.schedule(timeout=0.03, message=Tick())
        await asyncio.sleep(0.15)

        count = await collector.ask(GetCount())
        assert count == 3

    @pytest.mark.asyncio
    async def test_schedule_custom_message_to_self(self, system: ActorSystem):
        """Test scheduling custom message type to self."""

        @dataclass
        class Reminder:
            text: str

        @dataclass
        class GetReminders:
            pass

        class ReminderActor(Actor[Any]):
            def __init__(self):
                self.reminders: list[str] = []

            async def receive(self, msg: Any, ctx: Context) -> None:
                match msg:
                    case Reminder(text):
                        self.reminders.append(text)
                    case GetReminders():
                        await ctx.reply(self.reminders)

        actor = await system.spawn(ReminderActor)

        await actor.schedule(timeout=0, message=Reminder("wake up"))
        await actor.schedule(timeout=0, message=Reminder("meeting"))
        await asyncio.sleep(0.1)

        reminders = await actor.ask(GetReminders())
        assert len(reminders) == 2
        assert "wake up" in reminders
        assert "meeting" in reminders
