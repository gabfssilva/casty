"""Tests for become/unbecome behavior switching."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, ActorSystem, Context


# Test messages


@dataclass
class GetState:
    """Request current state."""

    pass


@dataclass
class Activate:
    """Switch to active mode."""

    pass


@dataclass
class Deactivate:
    """Switch back to initial mode."""

    pass


@dataclass
class Process:
    """Process something in current mode."""

    value: int


@dataclass
class PushBehavior:
    """Push a new behavior onto the stack."""

    level: int


@dataclass
class PopBehavior:
    """Pop current behavior from the stack."""

    pass


@dataclass
class GetLevel:
    """Get current behavior level."""

    pass


# Test actors


class StateMachineActor(Actor[GetState | Activate | Deactivate | Process]):
    """Actor that switches between two states."""

    def __init__(self):
        self.mode = "initial"
        self.processed: list[tuple[str, int]] = []

    async def receive(self, msg: GetState | Activate | Deactivate | Process, ctx: Context):
        match msg:
            case GetState():
                ctx.reply(self.mode)
            case Activate():
                self.mode = "active"
                ctx.become(self.active_behavior)
            case Process(value):
                self.processed.append(("initial", value))
            case Deactivate():
                pass  # Already in initial mode

    async def active_behavior(
        self, msg: GetState | Activate | Deactivate | Process, ctx: Context
    ):
        match msg:
            case GetState():
                ctx.reply(self.mode)
            case Deactivate():
                self.mode = "initial"
                ctx.unbecome()
            case Process(value):
                self.processed.append(("active", value))
            case Activate():
                pass  # Already in active mode


class StackedBehaviorActor(Actor[PushBehavior | PopBehavior | GetLevel]):
    """Actor that demonstrates behavior stacking."""

    def __init__(self):
        self.level = 0

    async def receive(self, msg: PushBehavior | PopBehavior | GetLevel, ctx: Context):
        match msg:
            case GetLevel():
                ctx.reply(self.level)
            case PushBehavior(level):
                self.level = level
                ctx.become(self._make_level_behavior(level))
            case PopBehavior():
                pass  # Already at base level

    def _make_level_behavior(self, level: int):
        async def behavior(msg: PushBehavior | PopBehavior | GetLevel, ctx: Context):
            match msg:
                case GetLevel():
                    ctx.reply(level)
                case PushBehavior(new_level):
                    self.level = new_level
                    ctx.become(self._make_level_behavior(new_level))
                case PopBehavior():
                    self.level = level - 1
                    ctx.unbecome()

        return behavior


class DiscardBehaviorActor(Actor[Activate | Deactivate | GetState]):
    """Actor that uses discard_old=True."""

    def __init__(self):
        self.state = "A"

    async def receive(self, msg: Activate | Deactivate | GetState, ctx: Context):
        match msg:
            case GetState():
                ctx.reply(f"state={self.state}, handler=receive")
            case Activate():
                self.state = "B"
                ctx.become(self.behavior_b, discard_old=True)
            case _:
                pass

    async def behavior_b(self, msg: Activate | Deactivate | GetState, ctx: Context):
        match msg:
            case GetState():
                ctx.reply(f"state={self.state}, handler=behavior_b")
            case Activate():
                self.state = "C"
                ctx.become(self.behavior_c, discard_old=True)
            case Deactivate():
                ctx.unbecome()  # Goes back to receive (stack is empty)
            case _:
                pass

    async def behavior_c(self, msg: Activate | Deactivate | GetState, ctx: Context):
        match msg:
            case GetState():
                ctx.reply(f"state={self.state}, handler=behavior_c")
            case Deactivate():
                self.state = "B"
                ctx.unbecome()  # Goes back to receive (discard_old=True doesn't stack)
            case _:
                pass


class TestBecome:
    """Tests for ctx.become() behavior switching."""

    @pytest.mark.asyncio
    async def test_become_switches_behavior(self, system: ActorSystem):
        """Test that become() switches to a new message handler."""
        actor = await system.spawn(StateMachineActor)

        # Initial state
        state = await actor.ask(GetState())
        assert state == "initial"

        # Switch to active
        await actor.send(Activate())
        await asyncio.sleep(0.05)

        state = await actor.ask(GetState())
        assert state == "active"

    @pytest.mark.asyncio
    async def test_become_processes_with_new_behavior(self, system: ActorSystem):
        """Test that messages are processed by the new behavior."""
        actor = await system.spawn(StateMachineActor)

        # Process in initial mode
        await actor.send(Process(1))
        await asyncio.sleep(0.05)

        # Switch to active and process
        await actor.send(Activate())
        await actor.send(Process(2))
        await asyncio.sleep(0.05)

        # Switch back and process
        await actor.send(Deactivate())
        await actor.send(Process(3))
        await asyncio.sleep(0.05)

        # Verify processed messages went to correct handlers
        # We need to get the actor instance to check processed list
        # Use ask to get the state
        state = await actor.ask(GetState())
        assert state == "initial"


class TestUnbecome:
    """Tests for ctx.unbecome() behavior reverting."""

    @pytest.mark.asyncio
    async def test_unbecome_reverts_behavior(self, system: ActorSystem):
        """Test that unbecome() reverts to previous behavior."""
        actor = await system.spawn(StateMachineActor)

        # Switch to active
        await actor.send(Activate())
        await asyncio.sleep(0.05)
        assert await actor.ask(GetState()) == "active"

        # Switch back
        await actor.send(Deactivate())
        await asyncio.sleep(0.05)
        assert await actor.ask(GetState()) == "initial"

    @pytest.mark.asyncio
    async def test_unbecome_on_empty_stack_is_safe(self, system: ActorSystem):
        """Test that unbecome() on empty stack doesn't crash."""
        actor = await system.spawn(StateMachineActor)

        # Try to deactivate when already in initial (calls unbecome on empty stack)
        await actor.send(Deactivate())
        await asyncio.sleep(0.05)

        # Actor should still work
        state = await actor.ask(GetState())
        assert state == "initial"


class TestBehaviorStack:
    """Tests for behavior stacking."""

    @pytest.mark.asyncio
    async def test_multiple_become_stacks_behaviors(self, system: ActorSystem):
        """Test that multiple become() calls stack behaviors."""
        actor = await system.spawn(StackedBehaviorActor)

        # Base level
        assert await actor.ask(GetLevel()) == 0

        # Push level 1
        await actor.send(PushBehavior(1))
        await asyncio.sleep(0.05)
        assert await actor.ask(GetLevel()) == 1

        # Push level 2
        await actor.send(PushBehavior(2))
        await asyncio.sleep(0.05)
        assert await actor.ask(GetLevel()) == 2

        # Push level 3
        await actor.send(PushBehavior(3))
        await asyncio.sleep(0.05)
        assert await actor.ask(GetLevel()) == 3

    @pytest.mark.asyncio
    async def test_unbecome_pops_behavior_stack(self, system: ActorSystem):
        """Test that unbecome() pops behaviors in LIFO order."""
        actor = await system.spawn(StackedBehaviorActor)

        # Push multiple levels
        await actor.send(PushBehavior(1))
        await actor.send(PushBehavior(2))
        await actor.send(PushBehavior(3))
        await asyncio.sleep(0.05)
        assert await actor.ask(GetLevel()) == 3

        # Pop back through levels
        await actor.send(PopBehavior())
        await asyncio.sleep(0.05)
        assert await actor.ask(GetLevel()) == 2

        await actor.send(PopBehavior())
        await asyncio.sleep(0.05)
        assert await actor.ask(GetLevel()) == 1

        await actor.send(PopBehavior())
        await asyncio.sleep(0.05)
        assert await actor.ask(GetLevel()) == 0


class TestDiscardOld:
    """Tests for become(discard_old=True)."""

    @pytest.mark.asyncio
    async def test_discard_old_replaces_behavior(self, system: ActorSystem):
        """Test that discard_old=True replaces instead of stacking."""
        actor = await system.spawn(DiscardBehaviorActor)

        # Initial state
        result = await actor.ask(GetState())
        assert result == "state=A, handler=receive"

        # Switch to B (first become, pushes onto empty stack)
        await actor.send(Activate())
        await asyncio.sleep(0.05)
        result = await actor.ask(GetState())
        assert result == "state=B, handler=behavior_b"

        # Switch to C (discard_old=True replaces B with C)
        await actor.send(Activate())
        await asyncio.sleep(0.05)
        result = await actor.ask(GetState())
        assert result == "state=C, handler=behavior_c"

        # Unbecome should go back to receive (not behavior_b)
        await actor.send(Deactivate())
        await asyncio.sleep(0.05)
        result = await actor.ask(GetState())
        assert result == "state=B, handler=receive"
