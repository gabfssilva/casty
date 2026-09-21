import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from operator import methodcaller
from typing import Literal, assert_never
from uuid import UUID

import pytest

from casty import ActorFailed, ActorSystem, Backoff, Context, MailboxFull, Ref, actor
from tests.app import Account, Balance, Deposit, Order, Paid, Pay, Pending, account
from tests.support import eventually


@dataclass(frozen=True)
class Change:
    reply_to: Ref[int]
    how: Literal["add", "double", "break", "read"]


@dataclass(frozen=True)
class Tally:
    balance: int = 0
    starts: int = 0


@dataclass(frozen=True)
class Starts:
    reply_to: Ref[int]


@dataclass(frozen=True)
class Explode:
    reply_to: Ref[int]


@dataclass(frozen=True)
class Report:
    balance: int
    handled: int


@dataclass(frozen=True)
class Inspect:
    reply_to: Ref[Report]


@dataclass(frozen=True)
class Offset:
    value: int = 0


@dataclass(frozen=True)
class GetOffset:
    reply_to: Ref[int]


type Tree = int | tuple[Tree, ...]


@dataclass(frozen=True)
class Page[T]:
    items: tuple[T, ...]
    next: str | None


@dataclass(frozen=True)
class Sample:
    nothing: None
    flag: bool
    count: int
    ratio: float
    name: str
    raw: bytes
    level: Literal["low", "high"]
    at: datetime
    id: UUID
    numbers: tuple[int, ...]
    pair: tuple[str, int]
    tags: frozenset[str]
    scores: Mapping[str, int]
    page: Page[int]
    tree: Tree
    either: int | str
    order: Order
    account: Ref[Deposit]


@dataclass(frozen=True)
class Read:
    reply_to: Ref[str]


@dataclass(frozen=True)
class Count:
    reply_to: Ref[tuple[str, int]]


@dataclass(frozen=True)
class Stash:
    sample: Sample | None = None


@dataclass(frozen=True)
class Echo:
    reply_to: Ref[Sample]
    sample: Sample


@dataclass(frozen=True)
class Recall:
    reply_to: Ref[Sample | None]


@dataclass(frozen=True)
class Ignore:
    reply_to: Ref[int]


def describe_actor_system() -> None:
    def when_an_account_goes_idle_between_messages() -> None:
        async def it_continues_from_the_saved_state() -> None:
            @actor(initial=Tally())
            async def tally(ctx: Context[Tally, Deposit | Starts]) -> None:
                await ctx.state.set(replace(ctx.state.value, starts=ctx.state.value.starts + 1))
                async for msg in ctx.inbox:
                    match msg:
                        case Deposit(reply_to, amount):
                            await ctx.state.set(replace(ctx.state.value, balance=ctx.state.value.balance + amount))
                            reply_to.tell(ctx.state.value.balance)
                        case Starts(reply_to):
                            reply_to.tell(ctx.state.value.starts)
                        case _:
                            assert_never(msg)

            async with ActorSystem(idle_after=timedelta(0)) as system:
                ref = system.ref(tally, "t-1")

                balances = [await ref.ask(Deposit, 5) for _ in range(20)]

                assert balances[-1] == 100
                assert await ref.ask(Starts) > 1

    def when_the_body_raises() -> None:
        async def it_fails_the_ask_drops_the_message_and_restarts_from_saved_state() -> None:
            @actor(initial=Account())
            async def fragile(ctx: Context[Account, Deposit | Explode | Inspect]) -> None:
                handled = 0
                async for msg in ctx.inbox:
                    handled += 1
                    match msg:
                        case Deposit(reply_to, amount):
                            await ctx.state.set(Account(ctx.state.value.balance + amount))
                            reply_to.tell(ctx.state.value.balance)
                        case Explode():
                            raise RuntimeError("boom")
                        case Inspect(reply_to):
                            reply_to.tell(Report(ctx.state.value.balance, handled))
                        case _:
                            assert_never(msg)

            backoff = Backoff(first=timedelta(milliseconds=50), factor=2.0)
            async with ActorSystem(backoff=backoff) as system:
                ref = system.ref(fragile, "f-1")
                await ref.ask(Deposit, 100)

                with pytest.raises(ActorFailed) as failed:
                    await ref.ask(Explode)

                assert (failed.value.key, failed.value.error, failed.value.message) == ("f-1", "RuntimeError", "boom")
                assert await ref.ask(Inspect) == Report(100, 1)

                started = asyncio.get_running_loop().time()
                for _ in range(2):
                    with pytest.raises(ActorFailed):
                        await ref.ask(Explode)

                assert await ref.ask(Inspect) == Report(100, 1)
                elapsed = asyncio.get_running_loop().time() - started
                assert elapsed >= (backoff.first * backoff.factor).total_seconds()

    def when_the_body_updates_its_state() -> None:
        async def it_saves_what_the_function_answers_whether_or_not_it_is_awaited() -> None:
            async def doubled(current: int) -> int:
                await asyncio.sleep(0)
                return current * 2

            async def broken(current: int) -> int:
                raise ValueError(f"cannot change {current}")

            @actor(initial=1)
            async def figure(ctx: Context[int, Change]) -> None:
                async for msg in ctx.inbox:
                    match msg.how:
                        case "add":
                            msg.reply_to.tell(await ctx.state.update(lambda current: current + 1))
                        case "double":
                            msg.reply_to.tell(await ctx.state.update(doubled))
                        case "break":
                            msg.reply_to.tell(await ctx.state.update(broken))
                        case "read":
                            msg.reply_to.tell(ctx.state.value)

            async with ActorSystem(backoff=Backoff(first=timedelta(milliseconds=10))) as system:
                ref = system.ref(figure, "f-1")

                assert await ref.ask(Change, "add") == 2
                assert await ref.ask(Change, "double") == 4
                with pytest.raises(ActorFailed, match="cannot change 4"):
                    await ref.ask(Change, "break")
                # What the update answered is what was saved, and the one that raised saved nothing.
                assert await ref.ask(Change, "read") == 4

    def when_a_type_has_no_default_initial() -> None:
        async def it_takes_the_initial_of_the_first_ref_and_refuses_a_ref_without_one() -> None:
            @actor
            async def document(ctx: Context[str, Read]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(ctx.state.value)

            async with ActorSystem() as system:
                # Through `methodcaller`, because the checkers refuse this call, which `tests/typing` covers.
                with pytest.raises(TypeError, match="initial"):
                    methodcaller("ref", document, "d-1")(system)

                assert await system.ref(document, "d-1", initial="first").ask(Read) == "first"
                assert await system.ref(document, "d-1", initial="second").ask(Read) == "first"

        async def it_starts_from_none_when_the_state_can_be_none() -> None:
            @actor
            async def draft(ctx: Context[str | None, Read]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(repr(ctx.state.value))

            async with ActorSystem() as system:
                assert await system.ref(draft, "d-1").ask(Read) == "None"

    def when_the_body_becomes_another_behavior() -> None:
        async def it_switches_at_the_next_read_even_inside_a_task_group() -> None:
            @actor
            async def settled(ctx: Context[Paid, Pay]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(False)

            @actor
            async def checkout(ctx: Context[Pending, Pay]) -> None:
                async def pay() -> None:
                    async for msg in ctx.inbox:
                        await ctx.become(settled, Paid(msg.amount))
                        msg.reply_to.tell(True)

                async with asyncio.TaskGroup() as group:
                    group.create_task(pay())
                    group.create_task(asyncio.Event().wait())

            async with ActorSystem() as system:
                ref = system.ref(checkout, "c-1", initial=Pending())

                assert await ref.ask(Pay, 10) is True
                assert await ref.ask(Pay, 10) is False

        async def it_keeps_the_state_when_none_is_given_however_many_times_it_switches() -> None:
            @actor(initial=0)
            async def even(ctx: Context[int, Count]) -> None:
                async for msg in ctx.inbox:
                    await ctx.state.set(ctx.state.value + 1)
                    await ctx.become(odd)
                    msg.reply_to.tell(("even", ctx.state.value))

            @actor(initial=0)
            async def odd(ctx: Context[int, Count]) -> None:
                async for msg in ctx.inbox:
                    await ctx.state.set(ctx.state.value + 1)
                    await ctx.become(even)
                    msg.reply_to.tell(("odd", ctx.state.value))

            async with ActorSystem() as system:
                ref = system.ref(even, "n")
                # Far more switches than a call stack takes: each one ends an activation instead of nesting in it.
                answers = [await ref.ask(Count) for _ in range(3_000)]

            assert answers == [("odd" if turn % 2 else "even", turn + 1) for turn in range(3_000)]

    def when_the_body_is_proactive() -> None:
        async def it_runs_once_its_ref_is_obtained_and_resumes_from_the_saved_offset() -> None:
            requested: list[int] = []

            async def feed(offset: int) -> AsyncIterator[int]:
                requested.append(offset)
                for record in range(offset, 10):
                    if record == 5 and len(requested) == 1:
                        raise RuntimeError("feed broke")
                    yield record
                await asyncio.Event().wait()

            @actor(initial=Offset())
            async def consumer(ctx: Context[Offset, GetOffset]) -> None:
                async for item in ctx.merge(feed(ctx.state.value.value)):
                    match item:
                        case GetOffset(reply_to):
                            reply_to.tell(ctx.state.value.value)
                        case int():
                            await ctx.state.set(Offset(item + 1))
                        case _:
                            assert_never(item)

            backoff = Backoff(first=timedelta(milliseconds=10))
            async with ActorSystem(backoff=backoff) as system:
                ref = system.ref(consumer, "p-0")

                async def reaches_the_end() -> None:
                    assert await ref.ask(GetOffset) == 10

                await eventually(reaches_the_end)
                assert requested == [0, 5]

    def when_messages_carry_every_supported_type() -> None:
        async def it_delivers_them_unchanged() -> None:
            @actor(initial=Stash())
            async def echo(ctx: Context[Stash, Echo | Recall]) -> None:
                async for msg in ctx.inbox:
                    match msg:
                        case Echo(reply_to, sample):
                            await ctx.state.set(Stash(sample))
                            reply_to.tell(sample)
                        case Recall(reply_to):
                            reply_to.tell(ctx.state.value.sample)
                        case _:
                            assert_never(msg)
                    # One message per run: every answer comes from a body that read the state back from the store.
                    return

            async with ActorSystem() as system:
                acc = system.ref(account, "a-1")
                deposits: Ref[Deposit] = acc
                sample = Sample(
                    nothing=None,
                    flag=True,
                    count=-(2**40),
                    ratio=0.25,
                    name="ação",
                    raw=b"\x00\xff",
                    level="high",
                    at=datetime(2026, 9, 17, 12, 30, 15, 123456, tzinfo=timezone(timedelta(hours=-3))),
                    id=UUID("8f5b8c9e-7a2d-4c1e-9b3f-2d6e1a0c4b7d"),
                    numbers=(1, 2, 3),
                    pair=("a", 1),
                    tags=frozenset({"x", "y"}),
                    scores={"x": 1, "y": 2},
                    page=Page((4, 5), next="p2"),
                    tree=(1, (2, (3,)), ()),
                    either="text",
                    order=Paid(7),
                    account=deposits,
                )
                ref = system.ref(echo, "e-1")

                assert await ref.ask(Echo, sample) == sample
                recalled = await ref.ask(Recall)
                assert recalled is not None
                assert recalled == sample
                assert recalled.at.utcoffset() == sample.at.utcoffset()
                assert await recalled.account.ask(Deposit, 7) == 7

    def when_a_bounded_mailbox_is_full() -> None:
        async def it_fails_asks_beyond_capacity_and_processes_the_rest() -> None:
            received = asyncio.Event()
            release = asyncio.Event()

            @actor(initial=Account(), mailbox=2)
            async def slow(ctx: Context[Account, Balance]) -> None:
                async for msg in ctx.inbox:
                    received.set()
                    await release.wait()
                    msg.reply_to.tell(ctx.state.value.balance)

            async with ActorSystem() as system:
                ref = system.ref(slow, "s-1")

                async def overflow() -> None:
                    with pytest.raises(MailboxFull):
                        await ref.ask(Balance)

                async with asyncio.TaskGroup() as group:
                    answers = [group.create_task(ref.ask(Balance))]
                    await received.wait()
                    answers += [group.create_task(ref.ask(Balance)) for _ in range(2)]
                    await group.create_task(overflow())
                    release.set()

                assert [answer.result() for answer in answers] == [0, 0, 0]

    def when_the_body_never_replies() -> None:
        async def it_raises_timeout_error_after_ask_timeout() -> None:
            @actor(initial=Account())
            async def silent(ctx: Context[Account, Ignore | Balance]) -> None:
                async for msg in ctx.inbox:
                    match msg:
                        case Ignore():
                            pass
                        case Balance(reply_to):
                            reply_to.tell(ctx.state.value.balance)
                        case _:
                            assert_never(msg)

            ask_timeout = timedelta(milliseconds=100)
            async with ActorSystem(ask_timeout=ask_timeout) as system:
                ref = system.ref(silent, "q-1")
                started = asyncio.get_running_loop().time()

                with pytest.raises(TimeoutError):
                    await ref.ask(Ignore)

                assert asyncio.get_running_loop().time() - started >= ask_timeout.total_seconds()
                assert await ref.ask(Balance) == 0
