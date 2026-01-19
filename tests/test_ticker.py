"""Tests for Ticker actor and ctx.tick() API."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, ActorSystem, Context
from casty.ticker import Ticker, Subscribe, Unsubscribe


@dataclass
class Ping:
    """Simple tick message."""

    pass


@dataclass
class GetCount:
    """Request number of pings received."""

    pass


@dataclass
class GetPings:
    """Request all received pings."""

    pass


class PingCollector(Actor[Ping | GetCount | GetPings]):
    """Actor that collects ping notifications."""

    def __init__(self):
        self.count = 0

    async def receive(self, msg: Ping | GetCount | GetPings, ctx: Context) -> None:
        match msg:
            case Ping():
                self.count += 1
            case GetCount():
                await ctx.reply(self.count)
            case GetPings():
                await ctx.reply(self.count)


class TestTickerDirect:
    """Tests for direct Ticker usage."""

    @pytest.mark.asyncio
    async def test_subscribe_receives_messages(self, system: ActorSystem):
        """Test that a subscribed listener receives messages."""
        ticker = await system.actor(Ticker, name="ticker")
        collector = await system.actor(PingCollector, name="ping-collector")

        sub_id = await ticker.ask(Subscribe(collector, Ping(), interval=0.05))
        await asyncio.sleep(0.2)

        count = await collector.ask(GetCount())
        assert count >= 3
        assert isinstance(sub_id, str)

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_messages(self, system: ActorSystem):
        """Test that unsubscribing stops message delivery."""
        ticker = await system.actor(Ticker, name="ticker")
        collector = await system.actor(PingCollector, name="ping-collector")

        sub_id = await ticker.ask(Subscribe(collector, Ping(), interval=0.05))
        await asyncio.sleep(0.15)

        count_before = await collector.ask(GetCount())
        assert count_before >= 2

        await ticker.send(Unsubscribe(sub_id))
        await asyncio.sleep(0.15)

        count_after = await collector.ask(GetCount())
        assert count_after == count_before


class TestTickerMultipleListeners:
    """Tests for multiple listeners with different messages."""

    @pytest.mark.asyncio
    async def test_multiple_listeners_different_intervals(self, system: ActorSystem):
        """Test multiple listeners with different intervals."""
        ticker = await system.actor(Ticker, name="ticker")
        fast_collector = await system.actor(PingCollector, name="fast-ping-collector")
        slow_collector = await system.actor(PingCollector, name="slow-ping-collector")

        await ticker.ask(Subscribe(fast_collector, Ping(), interval=0.05))
        await ticker.ask(Subscribe(slow_collector, Ping(), interval=0.15))
        await asyncio.sleep(0.35)

        fast_count = await fast_collector.ask(GetCount())
        slow_count = await slow_collector.ask(GetCount())

        assert fast_count >= 5
        assert slow_count >= 2
        assert fast_count > slow_count

    @pytest.mark.asyncio
    async def test_unsubscribe_one_keeps_other(self, system: ActorSystem):
        """Test that unsubscribing one listener doesn't affect others."""
        ticker = await system.actor(Ticker, name="ticker")
        collector1 = await system.actor(PingCollector, name="ping-collector-1")
        collector2 = await system.actor(PingCollector, name="ping-collector-2")

        sub_id1 = await ticker.ask(Subscribe(collector1, Ping(), interval=0.05))
        await ticker.ask(Subscribe(collector2, Ping(), interval=0.05))
        await asyncio.sleep(0.1)

        await ticker.send(Unsubscribe(sub_id1))
        count1_before = await collector1.ask(GetCount())

        await asyncio.sleep(0.15)

        count1_after = await collector1.ask(GetCount())
        count2 = await collector2.ask(GetCount())

        assert count1_after == count1_before
        assert count2 > count1_after


class TestContextTick:
    """Tests for ctx.tick() API."""

    @pytest.mark.asyncio
    async def test_ctx_tick_receives_messages(self, system: ActorSystem):
        """Test ctx.tick() sends messages to self."""

        @dataclass
        class Start:
            pass

        class TickingActor(Actor[Start | Ping | GetCount]):
            def __init__(self):
                self.count = 0
                self.tick_id: str | None = None

            async def receive(self, msg: Start | Ping | GetCount, ctx: Context) -> None:
                match msg:
                    case Start():
                        self.tick_id = await ctx.tick(Ping(), interval=0.05)
                    case Ping():
                        self.count += 1
                    case GetCount():
                        await ctx.reply(self.count)

        actor = await system.actor(TickingActor, name="ticking-actor")
        await actor.send(Start())
        await asyncio.sleep(0.2)

        count = await actor.ask(GetCount())
        assert count >= 3

    @pytest.mark.asyncio
    async def test_ctx_cancel_tick_stops_messages(self, system: ActorSystem):
        """Test ctx.cancel_tick() stops message delivery."""

        @dataclass
        class Start:
            pass

        @dataclass
        class Stop:
            pass

        class TickingActor(Actor[Start | Stop | Ping | GetCount]):
            def __init__(self):
                self.count = 0
                self.tick_id: str | None = None

            async def receive(self, msg: Start | Stop | Ping | GetCount, ctx: Context) -> None:
                match msg:
                    case Start():
                        self.tick_id = await ctx.tick(Ping(), interval=0.05)
                    case Stop():
                        if self.tick_id:
                            await ctx.cancel_tick(self.tick_id)
                            self.tick_id = None
                    case Ping():
                        self.count += 1
                    case GetCount():
                        await ctx.reply(self.count)

        actor = await system.actor(TickingActor, name="ticking-actor")
        await actor.send(Start())
        await asyncio.sleep(0.15)

        count_before = await actor.ask(GetCount())
        assert count_before >= 2

        await actor.send(Stop())
        await asyncio.sleep(0.15)

        count_after = await actor.ask(GetCount())
        assert count_after == count_before


class TestTickerCustomMessages:
    """Tests for custom message types."""

    @pytest.mark.asyncio
    async def test_different_message_types(self, system: ActorSystem):
        """Test ticking with different message types."""

        @dataclass
        class Heartbeat:
            pass

        @dataclass
        class GetHeartbeats:
            pass

        class HeartbeatCollector(Actor[Heartbeat | GetHeartbeats]):
            def __init__(self):
                self.count = 0

            async def receive(self, msg: Heartbeat | GetHeartbeats, ctx: Context) -> None:
                match msg:
                    case Heartbeat():
                        self.count += 1
                    case GetHeartbeats():
                        await ctx.reply(self.count)

        ticker = await system.actor(Ticker, name="ticker")
        collector = await system.actor(HeartbeatCollector, name="heartbeat-collector")

        await ticker.ask(Subscribe(collector, Heartbeat(), interval=0.05))
        await asyncio.sleep(0.2)

        count = await collector.ask(GetHeartbeats())
        assert count >= 3
