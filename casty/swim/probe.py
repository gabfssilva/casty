"""ProbeActor - Ephemeral actor for executing probes.

Each ProbeActor is spawned to handle a single probe sequence:
1. Send direct Ping to target
2. Wait for Ack with timeout
3. If timeout, send PingReq to K intermediaries
4. Collect indirect Acks
5. Report result to coordinator

The actor terminates after completing the probe sequence.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import Actor, Context

from .messages import (
    Ping,
    Ack,
    PingReq,
    IndirectAck,
    ProbeResult,
    IncomingMessage,
    GossipEntry,
)
from .config import SwimConfig

if TYPE_CHECKING:
    from casty import LocalRef


# Internal messages
@dataclass(frozen=True)
class _DirectTimeout:
    """Direct probe timeout expired."""

    pass


@dataclass(frozen=True)
class _IndirectTimeout:
    """Indirect probe timeout expired."""

    pass


@dataclass(frozen=True)
class _StartProbe:
    """Start the probe sequence."""

    pass


# Message type for ProbeActor
ProbeMessage = (
    _StartProbe
    | _DirectTimeout
    | _IndirectTimeout
    | IncomingMessage
)


class ProbeActor(Actor[ProbeMessage]):
    """Ephemeral actor that executes a single probe sequence.

    Lifecycle:
    1. Spawned by coordinator for each probe round
    2. Sends direct Ping to target
    3. Waits for Ack (or timeout → indirect probing)
    4. Reports result to coordinator
    5. Stops itself after completion
    """

    def __init__(
        self,
        target_node_id: str,
        target_address: tuple[str, int],
        sequence: int,
        config: SwimConfig,
        coordinator: LocalRef[Any],
        send_wire_message: Any,  # Callable to send wire messages
        intermediaries: list[tuple[str, tuple[str, int]]] | None = None,
        my_node_id: str = "",
        my_incarnation: int = 0,
    ) -> None:
        self.target_node_id = target_node_id
        self.target_address = target_address
        self.sequence = sequence
        self.config = config
        self.coordinator = coordinator
        self.send_wire_message = send_wire_message
        self.intermediaries = intermediaries or []
        self.my_node_id = my_node_id
        self.my_incarnation = my_incarnation

        self.start_time = 0.0
        self.direct_timeout_task: asyncio.Task | None = None
        self.indirect_timeout_task: asyncio.Task | None = None
        self.pending_indirect_acks = 0
        self.received_ack = False
        self.target_incarnation: int | None = None

    async def on_start(self) -> None:
        """Start the probe sequence."""
        self.start_time = time.time()
        await self._send_direct_ping()

    async def on_stop(self) -> None:
        """Cleanup timeouts."""
        if self.direct_timeout_task:
            self.direct_timeout_task.cancel()
        if self.indirect_timeout_task:
            self.indirect_timeout_task.cancel()

    async def receive(self, msg: ProbeMessage, ctx: Context) -> None:
        """Handle messages during direct probe phase."""
        match msg:
            case _StartProbe():
                await self._send_direct_ping()

            case IncomingMessage(from_node, _, message):
                await self._handle_incoming(from_node, message, ctx)

            case _DirectTimeout():
                await self._handle_direct_timeout(ctx)

            case _IndirectTimeout():
                await self._handle_indirect_timeout(ctx)

    async def _indirect_phase_behavior(self, msg: ProbeMessage, ctx: Context) -> None:
        """Handle messages during indirect probe phase."""
        match msg:
            case IncomingMessage(from_node, _, message):
                await self._handle_incoming_indirect(from_node, message, ctx)

            case _IndirectTimeout():
                await self._handle_indirect_timeout(ctx)

            case _:
                pass  # Ignore other messages

    async def _send_direct_ping(self) -> None:
        """Send direct Ping to target."""
        ping = Ping(
            source=self.my_node_id,
            source_incarnation=self.my_incarnation,
            sequence=self.sequence,
            piggyback=(),  # Piggyback will be added by coordinator
        )

        # Send ping via TransportMux (using node_id)
        await self.send_wire_message(self.target_node_id, ping)

        # Start timeout
        self.direct_timeout_task = asyncio.create_task(
            self._timeout_after(self.config.ping_timeout, _DirectTimeout())
        )

    async def _timeout_after(self, delay: float, msg: Any) -> None:
        """Wait and send timeout message to self."""
        await asyncio.sleep(delay)
        await self._ctx.self_ref.send(msg)

    async def _handle_incoming(
        self, from_node: str, message: Any, ctx: Context
    ) -> None:
        """Handle incoming message during direct phase."""
        match message:
            case Ack(source, source_incarnation, seq, _):
                if (
                    source == self.target_node_id
                    and seq == self.sequence
                    and not self.received_ack
                ):
                    self.received_ack = True
                    self.target_incarnation = source_incarnation
                    await self._report_success(ctx)

            case IndirectAck(_, target, seq, target_incarnation):
                # Shouldn't receive indirect ack in direct phase
                if target == self.target_node_id and seq == self.sequence:
                    self.received_ack = True
                    self.target_incarnation = target_incarnation
                    await self._report_success(ctx)

            case _:
                pass

    async def _handle_incoming_indirect(
        self, from_node: str, message: Any, ctx: Context
    ) -> None:
        """Handle incoming message during indirect phase."""
        match message:
            case Ack(source, source_incarnation, seq, _):
                # Direct ack during indirect phase (late arrival)
                if (
                    source == self.target_node_id
                    and seq == self.sequence
                    and not self.received_ack
                ):
                    self.received_ack = True
                    self.target_incarnation = source_incarnation
                    await self._report_success(ctx)

            case IndirectAck(_, target, seq, target_incarnation):
                if (
                    target == self.target_node_id
                    and seq == self.sequence
                    and not self.received_ack
                ):
                    self.received_ack = True
                    self.target_incarnation = target_incarnation
                    await self._report_success(ctx)

            case _:
                pass

    async def _handle_direct_timeout(self, ctx: Context) -> None:
        """Direct probe timed out - try indirect probing."""
        if self.received_ack:
            return

        # Cancel direct timeout task
        if self.direct_timeout_task:
            self.direct_timeout_task.cancel()
            self.direct_timeout_task = None

        # If no intermediaries, fail immediately
        if not self.intermediaries:
            await self._report_failure(ctx)
            return

        # Switch to indirect phase
        ctx.become(self._indirect_phase_behavior)

        # Send PingReq to intermediaries (using node_id)
        self.pending_indirect_acks = len(self.intermediaries)
        for intermediary_id, intermediary_addr in self.intermediaries:
            ping_req = PingReq(
                source=self.my_node_id,
                target=self.target_node_id,
                sequence=self.sequence,
            )
            await self.send_wire_message(intermediary_id, ping_req)

        # Start indirect timeout
        self.indirect_timeout_task = asyncio.create_task(
            self._timeout_after(self.config.ping_req_timeout, _IndirectTimeout())
        )

    async def _handle_indirect_timeout(self, ctx: Context) -> None:
        """Indirect probe timed out - report failure."""
        if not self.received_ack:
            await self._report_failure(ctx)

    async def _report_success(self, ctx: Context) -> None:
        """Report successful probe to coordinator."""
        latency_ms = (time.time() - self.start_time) * 1000

        await self.coordinator.send(
            ProbeResult(
                target=self.target_node_id,
                success=True,
                incarnation=self.target_incarnation,
                latency_ms=latency_ms,
            )
        )

        # Stop self
        await self._stop(ctx)

    async def _report_failure(self, ctx: Context) -> None:
        """Report failed probe to coordinator."""
        latency_ms = (time.time() - self.start_time) * 1000

        await self.coordinator.send(
            ProbeResult(
                target=self.target_node_id,
                success=False,
                incarnation=None,
                latency_ms=latency_ms,
            )
        )

        # Stop self
        await self._stop(ctx)

    async def _stop(self, ctx: Context) -> None:
        """Stop this ephemeral actor."""
        # Cancel any pending timeouts
        if self.direct_timeout_task:
            self.direct_timeout_task.cancel()
        if self.indirect_timeout_task:
            self.indirect_timeout_task.cancel()

        # Request parent to stop us
        if ctx.parent:
            await ctx.parent.send(_ProbeCompleted(self.target_node_id, self.sequence))


@dataclass(frozen=True)
class _ProbeCompleted:
    """Internal message to notify coordinator that probe is done."""

    target: str
    sequence: int
