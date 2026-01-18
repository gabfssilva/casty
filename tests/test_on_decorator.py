"""Tests for the @on decorator for message dispatch."""

import pytest
from dataclasses import dataclass
from casty import Actor, ActorSystem, Context, on


@dataclass
class Increment:
    amount: int


@dataclass
class Decrement:
    amount: int


@dataclass
class GetCount:
    pass


@dataclass
class Reset:
    pass


class SimpleCounter(Actor[Increment | GetCount]):
    """Simple actor using @on decorator."""

    def __init__(self):
        self.count = 0

    @on(Increment)
    async def handle_increment(self, msg: Increment, ctx: Context):
        self.count += msg.amount

    @on(GetCount)
    async def handle_query(self, msg: GetCount, ctx: Context):
        await ctx.reply(self.count)


class MultiHandlerCounter(Actor[Increment | Decrement | GetCount | Reset]):
    """Actor with multiple handlers."""

    def __init__(self):
        self.count = 0

    @on(Increment)
    async def handle_increment(self, msg: Increment, ctx: Context):
        self.count += msg.amount

    @on(Decrement)
    async def handle_decrement(self, msg: Decrement, ctx: Context):
        self.count -= msg.amount

    @on(GetCount)
    async def handle_query(self, msg: GetCount, ctx: Context):
        await ctx.reply(self.count)

    @on(Reset)
    async def handle_reset(self, msg: Reset, ctx: Context):
        self.count = 0


class MixinCounter(Actor[Increment | Decrement | GetCount]):
    """Actor that combines handlers via mixins."""

    def __init__(self):
        self.count = 0


class IncrementMixin:
    """Mixin providing increment handler."""

    @on(Increment)
    async def handle_increment(self, msg: Increment, ctx: Context):
        self.count += msg.amount


class DecrementMixin:
    """Mixin providing decrement handler."""

    @on(Decrement)
    async def handle_decrement(self, msg: Decrement, ctx: Context):
        self.count -= msg.amount


class QueryMixin:
    """Mixin providing query handler."""

    @on(GetCount)
    async def handle_query(self, msg: GetCount, ctx: Context):
        await ctx.reply(self.count)


class ComposedCounter(MixinCounter, IncrementMixin, DecrementMixin, QueryMixin):
    """Actor composed of multiple mixins."""

    pass


class OverrideReceiveCounter(Actor[Increment | GetCount]):
    """Actor that overrides receive instead of using @on."""

    def __init__(self):
        self.count = 0

    async def receive(self, msg: Increment | GetCount, ctx: Context):
        match msg:
            case Increment(amount):
                self.count += amount
            case GetCount():
                await ctx.reply(self.count)


class TestOnDecorator:
    """Tests for @on decorator functionality."""

    @pytest.mark.asyncio
    async def test_simple_handlers(self):
        """Test simple message handlers with @on."""
        async with ActorSystem.local() as system:
            counter = await system.spawn(SimpleCounter)

            # Test send
            await counter.send(Increment(5))
            await counter.send(Increment(3))

            # Test ask
            result = await counter.ask(GetCount())
            assert result == 8

    @pytest.mark.asyncio
    async def test_multiple_handlers(self):
        """Test multiple handlers in same actor."""
        async with ActorSystem.local() as system:
            counter = await system.spawn(MultiHandlerCounter)

            await counter.send(Increment(10))
            await counter.send(Decrement(3))
            await counter.send(Increment(2))

            result = await counter.ask(GetCount())
            assert result == 9

            await counter.send(Reset())
            result = await counter.ask(GetCount())
            assert result == 0

    @pytest.mark.asyncio
    async def test_mixin_composition(self):
        """Test handler composition via mixins."""
        async with ActorSystem.local() as system:
            counter = await system.spawn(ComposedCounter)

            await counter.send(Increment(5))
            await counter.send(Decrement(2))
            await counter.send(Increment(8))

            result = await counter.ask(GetCount())
            assert result == 11

    @pytest.mark.asyncio
    async def test_override_receive_fallback(self):
        """Test that overriding receive still works when no @on decorators."""
        async with ActorSystem.local() as system:
            counter = await system.spawn(OverrideReceiveCounter)

            await counter.send(Increment(7))
            result = await counter.ask(GetCount())
            assert result == 7



class TestHandlerRegistry:
    """Tests for handler registry collection."""

    def test_handlers_collected_from_class(self):
        """Test that handlers are collected during class initialization."""
        assert Increment in SimpleCounter._handlers
        assert GetCount in SimpleCounter._handlers
        assert len(SimpleCounter._handlers) == 2

    def test_handlers_collected_from_multiple_bases(self):
        """Test handler collection from multiple inheritance."""
        assert Increment in ComposedCounter._handlers
        assert Decrement in ComposedCounter._handlers
        assert GetCount in ComposedCounter._handlers
        assert len(ComposedCounter._handlers) == 3

    def test_no_handlers_when_receive_overridden(self):
        """Test that override receive actors don't collect handlers."""
        # Even though they might have @on decorators, they shouldn't use them
        # if receive is overridden
        assert len(OverrideReceiveCounter._handlers) == 0


class TestMultipleActorInstances:
    """Test that handlers work correctly with multiple actor instances."""

    @pytest.mark.asyncio
    async def test_independent_actor_states(self):
        """Test that multiple instances maintain independent state."""
        async with ActorSystem.local() as system:
            counter1 = await system.spawn(SimpleCounter)
            counter2 = await system.spawn(SimpleCounter)

            await counter1.send(Increment(10))
            await counter2.send(Increment(5))

            result1 = await counter1.ask(GetCount())
            result2 = await counter2.ask(GetCount())

            assert result1 == 10
            assert result2 == 5
