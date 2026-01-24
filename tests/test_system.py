import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox


@dataclass
class Increment:
    amount: int


@dataclass
class Get:
    pass


@actor
async def counter(initial: int, *, mailbox: Mailbox[Increment | Get]):
    count = initial
    async for msg, ctx in mailbox:
        match msg:
            case Increment(amount):
                count += amount
            case Get():
                await ctx.reply(count)


@pytest.mark.asyncio
async def test_system_actor_creation():
    from casty.system import ActorSystem

    async with ActorSystem() as system:
        ref = await system.actor(counter(0), name="c1")
        assert ref is not None
        assert ref.actor_id == "c1"


@pytest.mark.asyncio
async def test_system_send_and_ask():
    from casty.system import ActorSystem

    async with ActorSystem() as system:
        ref = await system.actor(counter(0), name="c1")

        await ref.send(Increment(5))
        await ref.send(Increment(3))

        result = await ref.ask(Get())
        assert result == 8


@pytest.mark.asyncio
async def test_system_get_or_create():
    from casty.system import ActorSystem

    async with ActorSystem() as system:
        ref1 = await system.actor(counter(0), name="c1")
        ref2 = await system.actor(counter(100), name="c1")  # same name

        # Should return same ref, not create new
        assert ref1.actor_id == ref2.actor_id

        # Value should still be 0 (first creation)
        result = await ref1.ask(Get())
        assert result == 0


@pytest.mark.asyncio
async def test_actor_with_explicit_state():
    from casty import ActorSystem, actor, Mailbox
    from casty.state import State

    @dataclass
    class GetState:
        pass

    @actor
    async def state_counter(state: State[int], *, mailbox: Mailbox[GetState]):
        async for msg, ctx in mailbox:
            match msg:
                case GetState():
                    await ctx.reply(state.value)

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(state_counter(State(42)), name="counter")
            result = await ref.ask(GetState())
            assert result == 42
