from __future__ import annotations

import asyncio

import pytest
from dataclasses import dataclass
from casty import actor, Mailbox, ActorSystem
from casty.cluster.development import debug_filter
from casty.state import State


@dataclass
class Get:
    pass


@dataclass
class Increment:
    amount: int


@pytest.mark.asyncio
async def test_actor_with_state_injection():
    async with asyncio.timeout(5):
        @actor
        async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
            async for msg, ctx in mailbox:
                match msg:
                    case Get():
                        await ctx.reply(state.value)
                    case Increment(amount):
                        state.set(state.value + amount)

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
        async def stateful(state: State[CounterState], *, mailbox: Mailbox[GetState | SetCount]):
            async for msg, ctx in mailbox:
                match msg:
                    case GetState():
                        await ctx.reply(state.value)
                    case SetCount(value):
                        state.set(CounterState(count=value, name=state.value.name))

        async with ActorSystem(filters=[debug_filter("main")]) as system:
            ref = await system.actor(stateful(CounterState(count=0, name="test")), name="s")

            result = await ref.ask(GetState())
            assert result.count == 0
            assert result.name == "test"

            await ref.send(SetCount(10))
            result = await ref.ask(GetState())
            assert result.count == 10


@pytest.mark.asyncio
async def test_actor_state_with_keyword_arg():
    async with asyncio.timeout(5):
        @actor
        async def counter(state: State[int], *, mailbox: Mailbox[Get | Increment]):
            async for msg, ctx in mailbox:
                match msg:
                    case Get():
                        await ctx.reply(state.value)
                    case Increment(amount):
                        state.set(state.value + amount)

        async with ActorSystem() as system:
            ref = await system.actor(counter(state=100), name="kwarg")

            result = await ref.ask(Get())
            assert result == 100


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
async def test_state_version_increments():
    async with asyncio.timeout(5):
        @dataclass
        class GetVersion:
            pass

        @actor
        async def versioned(state: State[int], *, mailbox: Mailbox[GetVersion | Increment]):
            async for msg, ctx in mailbox:
                match msg:
                    case GetVersion():
                        await ctx.reply(state.version)
                    case Increment(amount):
                        state.set(state.value + amount)

        async with ActorSystem() as system:
            ref = await system.actor(versioned(0), name="v")

            version = await ref.ask(GetVersion())
            assert version == 0

            await ref.send(Increment(1))
            version = await ref.ask(GetVersion())
            assert version == 1

            await ref.send(Increment(1))
            await ref.send(Increment(1))
            version = await ref.ask(GetVersion())
            assert version == 3
