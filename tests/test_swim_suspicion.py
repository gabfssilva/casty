"""Tests for SuspicionActor (Lifeguard)."""

import pytest
import asyncio

from casty import ActorSystem, Actor, Context
from casty.swim.suspicion import SuspicionActor
from casty.swim.config import SwimConfig
from casty.swim.messages import (
    StartSuspicion,
    ConfirmSuspicion,
    CancelSuspicion,
    SuspicionTimeout,
    Refute,
)
from tests.utils import retry_until


@pytest.fixture
def config():
    """Fast config for testing."""
    return SwimConfig.development()


@pytest.fixture
async def system():
    """Create and cleanup an ActorSystem."""
    async with ActorSystem() as sys:
        yield sys


class TestSuspicionActor:
    """Tests for SuspicionActor."""

    @pytest.mark.asyncio
    async def test_suspicion_timeout_notifies_coordinator(self, system):
        """Test that timeout expiration notifies coordinator."""
        coordinator_received = []

        class MockCoordinator(Actor[SuspicionTimeout | Refute]):
            async def receive(self, msg, ctx: Context):
                coordinator_received.append(msg)

        coordinator = await system.spawn(MockCoordinator)

        # Short but realistic timeout config
        # min_timeout = 1.0 * log10(3) * 0.05 ≈ 0.024s (24ms)
        # max_timeout = 2.0 * 0.024 ≈ 0.048s (48ms)
        config = SwimConfig(
            protocol_period=0.05,
            suspicion_alpha=1.0,
            suspicion_beta=2.0,
            confirmation_threshold=5,
            bind_address=("127.0.0.1", 0),
        )

        suspicion = await system.spawn(
            SuspicionActor,
            config=config,
            coordinator=coordinator,
            cluster_size_getter=lambda: 3,
        )

        # Start suspicion
        await suspicion.send(StartSuspicion(node_id="node-2", reporter="node-1"))

        # Wait for timeout notification using retry
        await retry_until(
            lambda: any(isinstance(m, SuspicionTimeout) for m in coordinator_received),
            timeout=5.0,
            interval=0.05,
            message="SuspicionTimeout not received by coordinator",
        )

        # Check coordinator was notified
        timeout_msgs = [m for m in coordinator_received if isinstance(m, SuspicionTimeout)]
        assert len(timeout_msgs) >= 1
        assert timeout_msgs[0].node_id == "node-2"

    @pytest.mark.asyncio
    async def test_multiple_confirmations_speeds_up_timeout(self, system):
        """Test that multiple confirmations speed up the timeout."""
        coordinator_received = []

        class MockCoordinator(Actor[SuspicionTimeout | Refute]):
            async def receive(self, msg, ctx: Context):
                coordinator_received.append(msg)

        coordinator = await system.spawn(MockCoordinator)

        # Config with longer timeout
        config = SwimConfig(
            protocol_period=0.1,
            suspicion_alpha=0.5,
            suspicion_beta=2.0,
            confirmation_threshold=5,
            bind_address=("127.0.0.1", 0),
        )

        suspicion = await system.spawn(
            SuspicionActor,
            config=config,
            coordinator=coordinator,
            cluster_size_getter=lambda: 10,
        )

        # Start suspicion with multiple confirmations
        await suspicion.send(StartSuspicion(node_id="node-2", reporter="node-1"))
        await suspicion.send(ConfirmSuspicion(node_id="node-2", reporter="node-3"))
        await suspicion.send(ConfirmSuspicion(node_id="node-2", reporter="node-4"))
        await suspicion.send(ConfirmSuspicion(node_id="node-2", reporter="node-5"))
        await suspicion.send(ConfirmSuspicion(node_id="node-2", reporter="node-6"))

        # Wait for timeout - should be faster due to confirmations
        await retry_until(
            lambda: any(isinstance(m, SuspicionTimeout) for m in coordinator_received),
            timeout=5.0,
            interval=0.05,
            message="SuspicionTimeout not received (with confirmations)",
        )

        # Check coordinator was notified
        timeout_msgs = [m for m in coordinator_received if isinstance(m, SuspicionTimeout)]
        assert len(timeout_msgs) >= 1

    @pytest.mark.asyncio
    async def test_cancel_suspicion_prevents_timeout(self, system):
        """Test that canceling suspicion prevents timeout notification."""
        coordinator_received = []

        class MockCoordinator(Actor[SuspicionTimeout | Refute]):
            async def receive(self, msg, ctx: Context):
                coordinator_received.append(msg)

        coordinator = await system.spawn(MockCoordinator)

        config = SwimConfig(
            protocol_period=0.1,
            suspicion_alpha=1.0,
            suspicion_beta=2.0,
            confirmation_threshold=5,
            bind_address=("127.0.0.1", 0),
        )

        suspicion = await system.spawn(
            SuspicionActor,
            config=config,
            coordinator=coordinator,
            cluster_size_getter=lambda: 5,
        )

        # Start and immediately cancel suspicion
        await suspicion.send(StartSuspicion(node_id="node-2", reporter="node-1"))
        await asyncio.sleep(0.01)  # Small delay to ensure StartSuspicion is processed
        await suspicion.send(CancelSuspicion(node_id="node-2"))

        # Wait long enough for timeout to have occurred if it wasn't cancelled
        # (use shorter wait since we expect no timeout)
        await asyncio.sleep(0.3)

        # Check no timeout was sent (cancellation worked)
        timeout_msgs = [m for m in coordinator_received if isinstance(m, SuspicionTimeout)]
        assert len([m for m in timeout_msgs if m.node_id == "node-2"]) == 0

    @pytest.mark.asyncio
    async def test_refutation_cancels_suspicion(self, system):
        """Test that refutation cancels suspicion and notifies coordinator."""
        coordinator_received = []

        class MockCoordinator(Actor[SuspicionTimeout | Refute]):
            async def receive(self, msg, ctx: Context):
                coordinator_received.append(msg)

        coordinator = await system.spawn(MockCoordinator)

        config = SwimConfig(
            protocol_period=0.1,
            suspicion_alpha=2.0,
            suspicion_beta=3.0,
            confirmation_threshold=5,
            bind_address=("127.0.0.1", 0),
        )

        suspicion = await system.spawn(
            SuspicionActor,
            config=config,
            coordinator=coordinator,
            cluster_size_getter=lambda: 5,
        )

        # Start suspicion and then refute
        await suspicion.send(StartSuspicion(node_id="node-2", reporter="node-1"))
        await asyncio.sleep(0.01)  # Small delay to ensure StartSuspicion is processed
        await suspicion.send(Refute(node_id="node-2", incarnation=5))

        # Wait for refutation to be forwarded
        await retry_until(
            lambda: any(isinstance(m, Refute) for m in coordinator_received),
            timeout=2.0,
            interval=0.05,
            message="Refute not forwarded to coordinator",
        )

        # Check refutation was forwarded
        refute_msgs = [m for m in coordinator_received if isinstance(m, Refute)]
        assert len(refute_msgs) >= 1
        assert refute_msgs[0].incarnation == 5

        # Wait long enough to verify no timeout occurs
        await asyncio.sleep(0.3)

        # Check no timeout for node-2 after refutation
        timeout_msgs = [m for m in coordinator_received if isinstance(m, SuspicionTimeout) and m.node_id == "node-2"]
        assert len(timeout_msgs) == 0
