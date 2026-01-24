from __future__ import annotations

import asyncio

import pytest
from dataclasses import dataclass
from casty import actor, Mailbox, ActorSystem
from casty.cluster.development import debug_filter


@dataclass
class Get:
    pass


@dataclass
class Increment:
    amount: int


@pytest.mark.asyncio
async def test_actor_with_state():
    async with asyncio.timeout(5):
        @actor
        async def counter(count: int, *, mailbox: Mailbox[Get | Increment]):
            async for msg, ctx in mailbox:
                match msg:
                    case Get():
                        await ctx.reply(count)
                    case Increment(amount):
                        count += amount

        async with ActorSystem() as system:
            ref = await system.actor(counter(0), name="test")

            result = await ref.ask(Get())
            assert result == 0

            await ref.send(Increment(5))
            result = await ref.ask(Get())
            assert result == 5


@pytest.mark.asyncio
async def test_actor_state_with_dataclass():
    async with asyncio.timeout(5):
        @dataclass
        class CounterState:
            count: int
            name: str

        @dataclass
        class GetState:
            pass

        @dataclass
        class SetCount:
            value: int

        @actor
        async def stateful(state: CounterState, *, mailbox: Mailbox[GetState | SetCount]):
            async for msg, ctx in mailbox:
                match msg:
                    case GetState():
                        await ctx.reply(state)
                    case SetCount(value):
                        state = CounterState(count=value, name=state.name)

        async with ActorSystem(filters=[debug_filter("main")]) as system:
            ref = await system.actor(stateful(CounterState(count=0, name="test")), name="s")

            result = await ref.ask(GetState())
            assert result.count == 0
            assert result.name == "test"

            await ref.send(SetCount(10))
            result = await ref.ask(GetState())
            assert result.count == 10


@pytest.mark.asyncio
async def test_actor_without_state_still_works():
    async with asyncio.timeout(5):
        @dataclass
        class Ping:
            pass

        @actor
        async def echo(*, mailbox: Mailbox[Ping]):
            async for msg, ctx in mailbox:
                match msg:
                    case Ping():
                        await ctx.reply("pong")

        async with ActorSystem() as system:
            ref = await system.actor(echo(), name="echo")
            result = await ref.ask(Ping())
            assert result == "pong"


@pytest.mark.asyncio
async def test_multiple_state_params():
    async with asyncio.timeout(5):
        @dataclass
        class GetBoth:
            pass

        @dataclass
        class IncrementCount:
            pass

        @dataclass
        class SetName:
            name: str

        @actor
        async def multi_state(count: int, name: str, *, mailbox: Mailbox[GetBoth | IncrementCount | SetName]):
            async for msg, ctx in mailbox:
                match msg:
                    case GetBoth():
                        await ctx.reply((count, name))
                    case IncrementCount():
                        count += 1
                    case SetName(n):
                        name = n

        async with ActorSystem() as system:
            ref = await system.actor(multi_state(0, "initial"), name="multi")

            result = await ref.ask(GetBoth())
            assert result == (0, "initial")

            await ref.send(IncrementCount())
            await ref.send(SetName("updated"))

            result = await ref.ask(GetBoth())
            assert result == (1, "updated")
