"""SuspicionActor - Lifeguard adaptive suspicion timeout.

Implements the Lifeguard paper's Local Health Aware Suspicion mechanism.
The suspicion timeout decreases logarithmically as more independent nodes
confirm the suspicion, reducing false positives while maintaining fast
failure detection.

Key features:
- Adaptive timeout based on confirmation count
- Logarithmic decay formula for timeout calculation
- Per-member suspicion tracking
- Buddy notification support (Nack)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import Actor, Context

from .config import SwimConfig
from .state import (
    SuspicionState,
    calculate_min_suspicion_timeout,
    calculate_max_suspicion_timeout,
)
from .messages import (
    StartSuspicion,
    ConfirmSuspicion,
    CancelSuspicion,
    SuspicionTimeout,
    Refute,
)

if TYPE_CHECKING:
    from casty import LocalRef


# Internal messages
@dataclass(frozen=True)
class _CheckTimeout:
    """Internal: Check if suspicion timeout expired."""

    node_id: str


# Suspicion message type
SuspicionMessage = (
    StartSuspicion
    | ConfirmSuspicion
    | CancelSuspicion
    | Refute
    | _CheckTimeout
)


class SuspicionActor(Actor[SuspicionMessage]):
    """Actor managing Lifeguard suspicion timeouts.

    For each suspected member, tracks:
    - When suspicion started
    - How many independent confirmations received
    - Adaptive timeout based on confirmations

    The timeout formula:
        fraction = log(n + 1) / log(k + 1)
        timeout = max - fraction * (max - min)

    Where:
    - n = number of confirmations
    - k = confirmation threshold (typically 5)
    - min = α × log₁₀(cluster_size) × protocol_period
    - max = β × min
    """

    def __init__(
        self,
        config: SwimConfig,
        coordinator: LocalRef[Any],
        cluster_size_getter: Any = None,  # Callable[[], int]
    ) -> None:
        self.config = config
        self.coordinator = coordinator
        self.cluster_size_getter = cluster_size_getter or (lambda: 10)

        # Active suspicions: node_id -> SuspicionState
        self.suspicions: dict[str, SuspicionState] = {}

        # Timeout task IDs: node_id -> task_id
        self.timeout_task_ids: dict[str, str] = {}

    async def on_stop(self) -> None:
        """Cancel all pending timeout tasks."""
        for task_id in self.timeout_task_ids.values():
            await self._ctx.cancel_schedule(task_id)
        self.timeout_task_ids.clear()

    async def receive(self, msg: SuspicionMessage, ctx: Context) -> None:
        """Handle suspicion messages."""
        match msg:
            case StartSuspicion(node_id, reporter):
                await self._start_suspicion(node_id, reporter, ctx)

            case ConfirmSuspicion(node_id, reporter):
                await self._confirm_suspicion(node_id, reporter, ctx)

            case CancelSuspicion(node_id):
                await self._cancel_suspicion(node_id)

            case Refute(node_id, incarnation):
                await self._handle_refutation(node_id, incarnation, ctx)

            case _CheckTimeout(node_id):
                await self._check_timeout(node_id, ctx)

    async def _start_suspicion(
        self, node_id: str, reporter: str, ctx: Context
    ) -> None:
        """Start suspicion for a member."""
        if node_id in self.suspicions:
            # Already suspected - treat as confirmation
            await self._confirm_suspicion(node_id, reporter, ctx)
            return

        # Calculate timeout parameters
        cluster_size = self.cluster_size_getter()
        min_timeout = calculate_min_suspicion_timeout(
            cluster_size,
            self.config.protocol_period,
            self.config.suspicion_alpha,
        )
        max_timeout = calculate_max_suspicion_timeout(
            min_timeout,
            self.config.suspicion_beta,
        )

        # Create suspicion state
        state = SuspicionState(
            node_id=node_id,
            first_reporter=reporter,
            min_timeout=min_timeout,
            max_timeout=max_timeout,
            confirmation_threshold=self.config.confirmation_threshold,
        )
        state.add_confirmation(reporter)
        self.suspicions[node_id] = state

        # Schedule timeout check
        await self._schedule_timeout(node_id, state.calculate_timeout(), ctx)

    async def _confirm_suspicion(
        self, node_id: str, reporter: str, ctx: Context
    ) -> None:
        """Add independent confirmation for suspicion."""
        state = self.suspicions.get(node_id)
        if state is None:
            return

        # Add confirmation
        if state.add_confirmation(reporter):
            # New confirmation - recalculate and reschedule timeout
            new_timeout = state.remaining_timeout()
            await self._reschedule_timeout(node_id, new_timeout, ctx)

    async def _cancel_suspicion(self, node_id: str) -> None:
        """Cancel suspicion for a member (refutation received)."""
        # Remove suspicion state
        self.suspicions.pop(node_id, None)

        # Cancel timeout task
        task_id = self.timeout_task_ids.pop(node_id, None)
        if task_id:
            await self._ctx.cancel_schedule(task_id)

    async def _handle_refutation(
        self, node_id: str, incarnation: int, ctx: Context
    ) -> None:
        """Handle refutation from suspected member."""
        state = self.suspicions.get(node_id)
        if state is None:
            return

        # Refutation is valid - cancel suspicion
        await self._cancel_suspicion(node_id)

        # Notify coordinator
        await self.coordinator.send(Refute(node_id, incarnation))

    async def _check_timeout(self, node_id: str, ctx: Context) -> None:
        """Check if suspicion timeout has expired."""
        state = self.suspicions.get(node_id)
        if state is None:
            return

        if state.is_expired():
            # Timeout expired - declare member dead
            self.suspicions.pop(node_id, None)
            self.timeout_task_ids.pop(node_id, None)

            # Notify coordinator
            await self.coordinator.send(SuspicionTimeout(node_id))

    async def _schedule_timeout(
        self, node_id: str, delay: float, ctx: Context
    ) -> None:
        """Schedule a timeout check."""
        # Cancel existing task if any
        existing_task_id = self.timeout_task_ids.get(node_id)
        if existing_task_id:
            await ctx.cancel_schedule(existing_task_id)

        # Schedule timeout
        task_id = await ctx.schedule(delay, _CheckTimeout(node_id))
        self.timeout_task_ids[node_id] = task_id

    async def _reschedule_timeout(
        self, node_id: str, new_delay: float, ctx: Context
    ) -> None:
        """Reschedule timeout with new delay (after confirmation)."""
        if new_delay <= 0:
            # Timeout already expired
            await ctx.self_ref.send(_CheckTimeout(node_id))
        else:
            await self._schedule_timeout(node_id, new_delay, ctx)

    def get_suspicion_state(self, node_id: str) -> SuspicionState | None:
        """Get current suspicion state for a member (for testing)."""
        return self.suspicions.get(node_id)

    def get_active_suspicions(self) -> list[str]:
        """Get list of currently suspected members (for testing)."""
        return list(self.suspicions.keys())
