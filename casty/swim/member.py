"""MemberActor - State machine for tracking cluster member state.

Each MemberActor represents a single known member in the cluster
and manages its state transitions: alive → suspected → dead.

The actor uses the become() pattern to implement the state machine,
with each state having its own message handler.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import Actor, Context

from .messages import (
    UpdateMemberState,
    GetMemberState,
    MemberStateChanged,
    MemberJoined,
    MemberLeft,
    MemberSuspected,
    MemberFailed,
    MemberAlive,
    Refute,
    StartSuspicion,
    CancelSuspicion,
)
from .state import MemberState, MemberInfo

if TYPE_CHECKING:
    from casty import LocalRef


# Internal messages for state transitions
@dataclass(frozen=True)
class _MarkSuspected:
    """Internal: Mark member as suspected."""

    reporter: str


@dataclass(frozen=True)
class _MarkDead:
    """Internal: Mark member as dead."""

    pass


@dataclass(frozen=True)
class _MarkAlive:
    """Internal: Mark member as alive (refutation)."""

    incarnation: int


# Message type for MemberActor
MemberMessage = (
    UpdateMemberState
    | GetMemberState
    | _MarkSuspected
    | _MarkDead
    | _MarkAlive
)


class MemberActor(Actor[MemberMessage]):
    """Actor managing state for a single cluster member.

    Implements a state machine with three states:
    - alive: Member is healthy
    - suspected: Member may have failed
    - dead: Member has been declared failed

    State transitions:
    - alive → suspected: After probe failure
    - suspected → alive: On refutation with higher incarnation
    - suspected → dead: On suspicion timeout
    - dead → alive: On join with higher incarnation
    """

    def __init__(
        self,
        node_id: str,
        address: tuple[str, int],
        incarnation: int = 0,
        coordinator: LocalRef[Any] | None = None,
        suspicion_actor: LocalRef[Any] | None = None,
    ) -> None:
        self.info = MemberInfo(
            node_id=node_id,
            address=address,
            state=MemberState.ALIVE,
            incarnation=incarnation,
        )
        self.coordinator = coordinator
        self.suspicion_actor = suspicion_actor

    async def receive(self, msg: MemberMessage, ctx: Context) -> None:
        """Handle messages in ALIVE state."""
        match msg:
            case UpdateMemberState():
                await self._handle_update(msg, ctx)

            case GetMemberState():
                ctx.reply(self.info)

            case _MarkSuspected(reporter):
                await self._transition_to_suspected(reporter, ctx)

            case _MarkDead():
                # Already dead case handled
                pass

            case _MarkAlive(incarnation):
                # Already alive, update incarnation if higher
                if incarnation > self.info.incarnation:
                    self.info.incarnation = incarnation
                    self.info.update_last_seen()

    async def _suspected_behavior(self, msg: MemberMessage, ctx: Context) -> None:
        """Handle messages in SUSPECTED state."""
        match msg:
            case UpdateMemberState():
                await self._handle_update(msg, ctx)

            case GetMemberState():
                ctx.reply(self.info)

            case _MarkSuspected():
                # Already suspected, ignore
                pass

            case _MarkDead():
                await self._transition_to_dead(ctx)

            case _MarkAlive(incarnation):
                if incarnation > self.info.incarnation:
                    await self._transition_to_alive(incarnation, ctx)

    async def _dead_behavior(self, msg: MemberMessage, ctx: Context) -> None:
        """Handle messages in DEAD state."""
        match msg:
            case UpdateMemberState():
                # Can be resurrected with higher incarnation
                await self._handle_update(msg, ctx)

            case GetMemberState():
                ctx.reply(self.info)

            case _MarkSuspected():
                # Already dead, ignore
                pass

            case _MarkDead():
                # Already dead, ignore
                pass

            case _MarkAlive(incarnation):
                if incarnation > self.info.incarnation:
                    await self._transition_to_alive(incarnation, ctx)

    async def _handle_update(self, msg: UpdateMemberState, ctx: Context) -> None:
        """Handle state update from gossip."""
        # Only process updates for our node
        if msg.node_id != self.info.node_id:
            return

        # Incarnation check: higher incarnation always wins
        if msg.incarnation < self.info.incarnation:
            return

        if msg.incarnation > self.info.incarnation:
            self.info.incarnation = msg.incarnation
            self.info.update_last_seen()

            if msg.address:
                self.info.address = msg.address

        # Process state change based on gossip type
        match msg.state:
            case "alive":
                if self.info.state != MemberState.ALIVE:
                    await self._transition_to_alive(msg.incarnation, ctx)

            case "suspected":
                if self.info.state == MemberState.ALIVE:
                    await self._transition_to_suspected("gossip", ctx)

            case "dead":
                if self.info.state != MemberState.DEAD:
                    await self._transition_to_dead(ctx)

    async def _transition_to_suspected(self, reporter: str, ctx: Context) -> None:
        """Transition from ALIVE to SUSPECTED."""
        old_state = self.info.state
        self.info.state = MemberState.SUSPECTED

        ctx.become(self._suspected_behavior)

        # Notify coordinator
        if self.coordinator:
            await self.coordinator.send(
                MemberStateChanged(
                    node_id=self.info.node_id,
                    old_state=old_state,
                    new_state=MemberState.SUSPECTED,
                    incarnation=self.info.incarnation,
                )
            )
            await self.coordinator.send(MemberSuspected(node_id=self.info.node_id))

        # Start Lifeguard suspicion timer
        if self.suspicion_actor:
            await self.suspicion_actor.send(
                StartSuspicion(node_id=self.info.node_id, reporter=reporter)
            )

    async def _transition_to_dead(self, ctx: Context) -> None:
        """Transition to DEAD state."""
        old_state = self.info.state
        self.info.state = MemberState.DEAD

        ctx.become(self._dead_behavior)

        # Notify coordinator
        if self.coordinator:
            await self.coordinator.send(
                MemberStateChanged(
                    node_id=self.info.node_id,
                    old_state=old_state,
                    new_state=MemberState.DEAD,
                    incarnation=self.info.incarnation,
                )
            )
            await self.coordinator.send(MemberFailed(node_id=self.info.node_id))

    async def _transition_to_alive(self, incarnation: int, ctx: Context) -> None:
        """Transition to ALIVE state (refutation or resurrection)."""
        old_state = self.info.state
        self.info.state = MemberState.ALIVE
        self.info.incarnation = incarnation
        self.info.update_last_seen()

        # Reset to default behavior
        ctx.unbecome()

        # Cancel suspicion if active
        if self.suspicion_actor and old_state == MemberState.SUSPECTED:
            await self.suspicion_actor.send(CancelSuspicion(node_id=self.info.node_id))

        # Notify coordinator
        if self.coordinator:
            await self.coordinator.send(
                MemberStateChanged(
                    node_id=self.info.node_id,
                    old_state=old_state,
                    new_state=MemberState.ALIVE,
                    incarnation=self.info.incarnation,
                )
            )
            if old_state == MemberState.SUSPECTED:
                await self.coordinator.send(MemberAlive(node_id=self.info.node_id))
            elif old_state == MemberState.DEAD:
                await self.coordinator.send(
                    MemberJoined(node_id=self.info.node_id, address=self.info.address)
                )

    # Public methods to trigger state transitions

    async def mark_suspected(self, reporter: str) -> None:
        """Mark this member as suspected (called by coordinator)."""
        await self._ctx.self_ref.send(_MarkSuspected(reporter))

    async def mark_dead(self) -> None:
        """Mark this member as dead (called by suspicion actor)."""
        await self._ctx.self_ref.send(_MarkDead())

    async def mark_alive(self, incarnation: int) -> None:
        """Mark this member as alive (refutation)."""
        await self._ctx.self_ref.send(_MarkAlive(incarnation))
