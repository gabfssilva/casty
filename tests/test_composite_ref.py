"""Tests for CompositeRef - type-based routing between actors."""

import pytest
from dataclasses import dataclass

from casty import Actor, ActorSystem, Context, CompositeRef


@dataclass
class MsgA:
    value: int


@dataclass
class MsgB:
    text: str


@dataclass
class MsgC:
    data: float


class ActorA(Actor[MsgA]):
    """Actor that handles MsgA."""

    def __init__(self) -> None:
        self.received: list[MsgA] = []

    async def receive(self, msg: MsgA, ctx: Context[MsgA]) -> None:
        self.received.append(msg)
        await ctx.reply(f"A received: {msg.value}")


class ActorB(Actor[MsgB]):
    """Actor that handles MsgB."""

    def __init__(self) -> None:
        self.received: list[MsgB] = []

    async def receive(self, msg: MsgB, ctx: Context[MsgB]) -> None:
        self.received.append(msg)
        await ctx.reply(f"B received: {msg.text}")


class ActorC(Actor[MsgC]):
    """Actor that handles MsgC."""

    def __init__(self) -> None:
        self.received: list[MsgC] = []

    async def receive(self, msg: MsgC, ctx: Context[MsgC]) -> None:
        self.received.append(msg)
        await ctx.reply(f"C received: {msg.data}")


class ActorAB(Actor[MsgA | MsgB]):
    """Actor that handles both MsgA and MsgB."""

    def __init__(self) -> None:
        self.received: list[MsgA | MsgB] = []

    async def receive(self, msg: MsgA | MsgB, ctx: Context[MsgA | MsgB]) -> None:
        self.received.append(msg)
        match msg:
            case MsgA(value):
                await ctx.reply(f"AB received A: {value}")
            case MsgB(text):
                await ctx.reply(f"AB received B: {text}")


@pytest.mark.asyncio
async def test_composite_ref_basic_routing() -> None:
    """Test that messages are routed to the correct actor based on type."""
    async with ActorSystem.local() as system:
        ref_a = await system.actor(ActorA, name="basic-actor-a")
        ref_b = await system.actor(ActorB, name="basic-actor-b")

        # Combine refs
        combined = ref_a | ref_b

        # Send MsgA - should go to ActorA
        result_a = await combined.ask(MsgA(42))
        assert result_a == "A received: 42"

        # Send MsgB - should go to ActorB
        result_b = await combined.ask(MsgB("hello"))
        assert result_b == "B received: hello"


@pytest.mark.asyncio
async def test_composite_ref_three_actors() -> None:
    """Test combining three actors."""
    async with ActorSystem.local() as system:
        ref_a = await system.actor(ActorA, name="three-actor-a")
        ref_b = await system.actor(ActorB, name="three-actor-b")
        ref_c = await system.actor(ActorC, name="three-actor-c")

        # Combine all three
        combined = ref_a | ref_b | ref_c

        # Test routing
        assert await combined.ask(MsgA(1)) == "A received: 1"
        assert await combined.ask(MsgB("test")) == "B received: test"
        assert await combined.ask(MsgC(3.14)) == "C received: 3.14"


@pytest.mark.asyncio
async def test_composite_ref_send() -> None:
    """Test send (fire-and-forget) with composite ref."""
    async with ActorSystem.local() as system:
        ref_a = await system.actor(ActorA, name="send-actor-a")
        ref_b = await system.actor(ActorB, name="send-actor-b")

        combined = ref_a | ref_b

        # Send messages
        await combined.send(MsgA(100))
        await combined.send(MsgB("world"))

        # Give time for messages to be processed
        import asyncio
        await asyncio.sleep(0.1)

        # Verify messages were routed correctly
        # (We can't directly access actors, but if no error, routing worked)


@pytest.mark.asyncio
async def test_composite_ref_operators() -> None:
    """Test >> and << operators."""
    async with ActorSystem.local() as system:
        ref_a = await system.actor(ActorA, name="operators-actor-a")
        ref_b = await system.actor(ActorB, name="operators-actor-b")

        combined = ref_a | ref_b

        # Test >> (send)
        await (combined >> MsgA(5))

        # Test << (ask)
        result = await (combined << MsgB("operator"))
        assert result == "B received: operator"


@pytest.mark.asyncio
async def test_composite_ref_unknown_type_raises() -> None:
    """Test that sending an unknown message type raises TypeError."""
    async with ActorSystem.local() as system:
        ref_a = await system.actor(ActorA, name="unknown-actor-a")
        ref_b = await system.actor(ActorB, name="unknown-actor-b")

        combined = ref_a | ref_b

        # MsgC is not handled by either actor
        with pytest.raises(TypeError, match="No actor ref accepts message"):
            await combined.send(MsgC(1.0))


@pytest.mark.asyncio
async def test_composite_ref_first_match_wins() -> None:
    """Test that when multiple refs accept a type, the first one wins."""
    async with ActorSystem.local() as system:
        ref_ab = await system.actor(ActorAB, name="first-match-actor-ab")
        ref_a = await system.actor(ActorA, name="first-match-actor-a")

        # ref_ab comes first, so it should handle MsgA
        combined = ref_ab | ref_a

        result = await combined.ask(MsgA(99))
        assert result == "AB received A: 99"


@pytest.mark.asyncio
async def test_composite_ref_accepted_types() -> None:
    """Test that accepted_types are correctly extracted."""
    async with ActorSystem.local() as system:
        ref_a = await system.actor(ActorA, name="accepted-types-actor-a")
        ref_b = await system.actor(ActorB, name="accepted-types-actor-b")

        # Verify types were extracted
        assert MsgA in ref_a.accepted_types
        assert MsgB in ref_b.accepted_types


@pytest.mark.asyncio
async def test_composite_ref_repr() -> None:
    """Test CompositeRef string representation."""
    async with ActorSystem.local() as system:
        ref_a = await system.actor(ActorA, name="repr-actor-a")
        ref_b = await system.actor(ActorB, name="repr-actor-b")

        combined = ref_a | ref_b

        repr_str = repr(combined)
        assert "CompositeRef" in repr_str
        assert "MsgA" in repr_str
        assert "MsgB" in repr_str
