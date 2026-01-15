"""AdaptiveTimerActor - Protocol period adaptation using EWMA.

Implements dynamic protocol period adjustment based on network conditions.
Uses Exponentially Weighted Moving Average (EWMA) to track probe success
rate and adjusts the protocol period accordingly.

Key features:
- EWMA-based success rate tracking
- Dynamic period adjustment with bounds
- Separate latency tracking for optimization
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import Actor, Context

from .config import SwimConfig
from .messages import RecordProbeResult, PeriodAdjusted, GetCurrentPeriod

if TYPE_CHECKING:
    from casty import LocalRef


# Internal messages
@dataclass(frozen=True)
class _AdjustPeriod:
    """Internal: Trigger period adjustment."""

    pass


# Adaptive timer message type
AdaptiveMessage = RecordProbeResult | GetCurrentPeriod | _AdjustPeriod


class AdaptiveTimerActor(Actor[AdaptiveMessage]):
    """Actor managing adaptive protocol period.

    Tracks probe success rate using EWMA and adjusts the protocol
    period to optimize failure detection latency vs. network overhead.

    Adjustment rules:
    - Success rate > 95%: Decrease period (probe more frequently)
    - Success rate < 80%: Increase period (probe less frequently)
    - Otherwise: Maintain current period

    The period is bounded by config.min_protocol_period and
    config.max_protocol_period to prevent runaway adaptation.
    """

    # Thresholds for adjustment
    HIGH_SUCCESS_THRESHOLD = 0.95
    LOW_SUCCESS_THRESHOLD = 0.80

    # Adjustment multipliers
    DECREASE_MULTIPLIER = 0.9
    INCREASE_MULTIPLIER = 1.2

    # Minimum samples before adjusting
    MIN_SAMPLES = 10

    def __init__(
        self,
        config: SwimConfig,
        coordinator: LocalRef[Any],
    ) -> None:
        self.config = config
        self.coordinator = coordinator

        # Current state
        self.current_period = config.protocol_period

        # EWMA tracking
        self.alpha = config.adaptation_alpha
        self.success_rate = 1.0  # Start optimistic
        self.avg_latency_ms = 0.0

        # Sample counting
        self.sample_count = 0
        self.adjustment_count = 0

    async def receive(self, msg: AdaptiveMessage, ctx: Context) -> None:
        """Handle adaptive timer messages."""
        match msg:
            case RecordProbeResult(success, latency_ms):
                await self._record_result(success, latency_ms, ctx)

            case GetCurrentPeriod():
                ctx.reply(self.current_period)

            case _AdjustPeriod():
                await self._adjust_period(ctx)

    async def _record_result(
        self, success: bool, latency_ms: float, ctx: Context
    ) -> None:
        """Record probe result and update EWMA."""
        self.sample_count += 1

        # Update success rate EWMA
        success_value = 1.0 if success else 0.0
        self.success_rate = (
            self.alpha * success_value + (1 - self.alpha) * self.success_rate
        )

        # Update latency EWMA (only for successful probes)
        if success and latency_ms > 0:
            if self.avg_latency_ms == 0:
                self.avg_latency_ms = latency_ms
            else:
                self.avg_latency_ms = (
                    self.alpha * latency_ms + (1 - self.alpha) * self.avg_latency_ms
                )

        # Trigger adjustment after enough samples
        if self.sample_count >= self.MIN_SAMPLES:
            await ctx.self_ref.send(_AdjustPeriod())
            self.sample_count = 0

    async def _adjust_period(self, ctx: Context) -> None:
        """Adjust protocol period based on current metrics."""
        old_period = self.current_period
        new_period = old_period

        if self.success_rate > self.HIGH_SUCCESS_THRESHOLD:
            # Network is healthy - decrease period (probe more often)
            new_period = old_period * self.DECREASE_MULTIPLIER
        elif self.success_rate < self.LOW_SUCCESS_THRESHOLD:
            # Network is stressed - increase period (probe less often)
            new_period = old_period * self.INCREASE_MULTIPLIER
        # else: maintain current period

        # Apply bounds
        new_period = max(self.config.min_protocol_period, new_period)
        new_period = min(self.config.max_protocol_period, new_period)

        # Only notify if changed
        if new_period != old_period:
            self.current_period = new_period
            self.adjustment_count += 1

            await self.coordinator.send(PeriodAdjusted(new_period))

    # Getters for testing and monitoring

    def get_success_rate(self) -> float:
        """Get current success rate EWMA."""
        return self.success_rate

    def get_avg_latency_ms(self) -> float:
        """Get current average latency EWMA."""
        return self.avg_latency_ms

    def get_current_period(self) -> float:
        """Get current protocol period."""
        return self.current_period

    def get_adjustment_count(self) -> int:
        """Get number of period adjustments made."""
        return self.adjustment_count
