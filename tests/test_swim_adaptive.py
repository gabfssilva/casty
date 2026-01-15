"""Tests for AdaptiveTimerActor (EWMA period adaptation)."""

import pytest
import asyncio

from casty import ActorSystem, Actor, Context
from casty.swim.adaptive import AdaptiveTimerActor
from casty.swim.config import SwimConfig
from casty.swim.messages import RecordProbeResult, GetCurrentPeriod, PeriodAdjusted
from tests.utils import retry_until


@pytest.fixture
def config():
    """Test config with known values."""
    return SwimConfig(
        protocol_period=1.0,
        min_protocol_period=0.1,
        max_protocol_period=10.0,
        adaptation_alpha=0.3,  # Higher alpha for faster response
        bind_address=("127.0.0.1", 0),
    )


@pytest.fixture
async def system():
    """Create and cleanup an ActorSystem."""
    async with ActorSystem() as sys:
        yield sys


class TestAdaptiveTimerActor:
    """Tests for AdaptiveTimerActor."""

    @pytest.mark.asyncio
    async def test_initial_state(self, system, config):
        """Test initial actor state."""
        received = []

        class MockCoordinator(Actor[PeriodAdjusted]):
            async def receive(self, msg, ctx: Context):
                received.append(msg)

        coordinator = await system.spawn(MockCoordinator)

        adaptive = await system.spawn(
            AdaptiveTimerActor,
            config=config,
            coordinator=coordinator,
        )

        # Query current period
        period = await adaptive.ask(GetCurrentPeriod())
        assert period == config.protocol_period

    @pytest.mark.asyncio
    async def test_period_increases_on_failures(self, system, config):
        """Test period increases when success rate is low."""
        received = []

        class MockCoordinator(Actor[PeriodAdjusted]):
            async def receive(self, msg, ctx: Context):
                received.append(msg)

        coordinator = await system.spawn(MockCoordinator)

        adaptive = await system.spawn(
            AdaptiveTimerActor,
            config=config,
            coordinator=coordinator,
        )

        initial_period = config.protocol_period

        # Record enough failures to trigger adjustment
        for _ in range(15):
            await adaptive.send(RecordProbeResult(success=False, latency_ms=0.0))

        # Wait for period adjustment
        await retry_until(
            lambda: any(isinstance(m, PeriodAdjusted) for m in received),
            timeout=2.0,
            interval=0.05,
            message="PeriodAdjusted not received after failures",
        )

        # Check if coordinator received period adjustment
        # Period should have increased due to failures
        period_msgs = [m for m in received if isinstance(m, PeriodAdjusted)]
        assert len(period_msgs) >= 1
        assert period_msgs[-1].new_period > initial_period

    @pytest.mark.asyncio
    async def test_period_decreases_on_success(self, system):
        """Test period decreases when success rate is high."""
        received = []

        class MockCoordinator(Actor[PeriodAdjusted]):
            async def receive(self, msg, ctx: Context):
                received.append(msg)

        coordinator = await system.spawn(MockCoordinator)

        # Start with a longer period
        config = SwimConfig(
            protocol_period=5.0,
            min_protocol_period=0.1,
            max_protocol_period=10.0,
            adaptation_alpha=0.5,  # Fast response
            bind_address=("127.0.0.1", 0),
        )

        adaptive = await system.spawn(
            AdaptiveTimerActor,
            config=config,
            coordinator=coordinator,
        )

        # Record enough successes to trigger adjustment
        for _ in range(15):
            await adaptive.send(RecordProbeResult(success=True, latency_ms=10.0))

        # Wait for period adjustment
        await retry_until(
            lambda: any(isinstance(m, PeriodAdjusted) for m in received),
            timeout=2.0,
            interval=0.05,
            message="PeriodAdjusted not received after successes",
        )

        # Check if coordinator received period adjustment
        # Period should have decreased due to successes
        period_msgs = [m for m in received if isinstance(m, PeriodAdjusted)]
        assert len(period_msgs) >= 1
        assert period_msgs[-1].new_period < 5.0

    @pytest.mark.asyncio
    async def test_coordinator_receives_adjustment(self, system):
        """Test that coordinator receives period adjustment messages."""
        received = []

        class MockCoordinator(Actor[PeriodAdjusted]):
            async def receive(self, msg, ctx: Context):
                received.append(msg)

        coordinator = await system.spawn(MockCoordinator)

        config = SwimConfig(
            protocol_period=1.0,
            min_protocol_period=0.1,
            max_protocol_period=10.0,
            adaptation_alpha=0.8,  # Very fast response for testing
            bind_address=("127.0.0.1", 0),
        )

        adaptive = await system.spawn(
            AdaptiveTimerActor,
            config=config,
            coordinator=coordinator,
        )

        # Record all failures to ensure period increases
        for _ in range(20):
            await adaptive.send(RecordProbeResult(success=False, latency_ms=0.0))

        # Wait for period adjustment
        await retry_until(
            lambda: any(isinstance(m, PeriodAdjusted) for m in received),
            timeout=2.0,
            interval=0.05,
            message="PeriodAdjusted not received by coordinator",
        )

        # Coordinator should have received at least one PeriodAdjusted message
        period_msgs = [m for m in received if isinstance(m, PeriodAdjusted)]
        assert len(period_msgs) >= 1

    @pytest.mark.asyncio
    async def test_get_current_period(self, system, config):
        """Test querying current period."""
        class MockCoordinator(Actor[PeriodAdjusted]):
            async def receive(self, msg, ctx: Context):
                pass

        coordinator = await system.spawn(MockCoordinator)

        adaptive = await system.spawn(
            AdaptiveTimerActor,
            config=config,
            coordinator=coordinator,
        )

        # Query should return initial period
        period = await adaptive.ask(GetCurrentPeriod())
        assert period == config.protocol_period

        # After some failures, period should be different
        for _ in range(15):
            await adaptive.send(RecordProbeResult(success=False, latency_ms=0.0))

        # Wait for internal state to update
        await asyncio.sleep(0.1)

        new_period = await adaptive.ask(GetCurrentPeriod())
        # Period should have changed (increased due to failures)
        assert new_period >= config.protocol_period
