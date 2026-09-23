import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from operator import methodcaller
from typing import Literal, assert_never
from uuid import UUID

import pytest

from casty import ActorFailed, ActorSystem, Backoff, Context, DefaultedActor, MailboxFull, ReentrancyError, Ref, actor
from casty.collections import counter
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


@dataclass(frozen=True)
class Nap:
    reply_to: Ref[int]


@dataclass(frozen=True)
class Relay:
    reply_to: Ref[str]


@dataclass(frozen=True)
class Note:
    text: str


@dataclass(frozen=True)
class Bounce:
    reply_to: Ref[str]
    hops: int


@dataclass(frozen=True)
class Descend:
    reply_to: Ref[tuple[str, ...]]
    depth: int


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

        async def it_takes_none_given_as_initial_as_a_default() -> None:
            @actor(initial=None)
            async def stateless(ctx: Context[None, Read]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(repr(ctx.state.value))

            assert isinstance(stateless, DefaultedActor)
            assert stateless.initial is None
            async with ActorSystem() as system:
                assert await system.ref(stateless, "s-1").ask(Read) == "None"

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

            # Refusing is the default, so a bounded mailbox that says nothing else behaves as it always has.
            assert slow.on_full == "refuse"
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

    def when_a_bounded_mailbox_waits_for_room() -> None:
        async def it_holds_a_fast_sender_until_there_is_room_and_never_queues_past_the_bound() -> None:
            received = asyncio.Event()
            release = asyncio.Event()

            @actor(initial=Account(), mailbox=2, on_full="wait")
            async def slow(ctx: Context[Account, Deposit]) -> None:
                async for msg in ctx.inbox:
                    received.set()
                    await release.wait()
                    await ctx.state.set(Account(ctx.state.value.balance + msg.amount))
                    msg.reply_to.tell(ctx.state.value.balance)

            async with ActorSystem() as system:
                ref = system.ref(slow, "s-1")
                answers = [asyncio.ensure_future(ref.ask(Deposit, 1)) for _ in range(50)]
                await received.wait()
                await asyncio.sleep(0.05)

                # One message in hand and two queued: the other 47 are still with the sender, and none was refused.
                assert system._queued(slow, "s-1") == 2
                assert not any(answer.done() for answer in answers)

                release.set()
                balances = await asyncio.gather(*answers)

            # Callers held on the node of the key are called back in the order they arrived.
            assert balances == list(range(1, 51))

        async def it_ends_a_cycle_of_full_mailboxes_in_an_error_instead_of_a_hang() -> None:
            @actor(initial=Account(), mailbox=1, on_full="wait")
            async def left(ctx: Context[Account, Balance]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(await ctx.system.ref(right, ctx.key).ask(Balance))

            @actor(initial=Account(), mailbox=1, on_full="wait")
            async def right(ctx: Context[Account, Balance]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(await ctx.system.ref(left, ctx.key).ask(Balance))

            async with ActorSystem(backoff=Backoff(first=timedelta(milliseconds=10))) as system:
                ref = system.ref(left, "k")
                # `left` asks `right` with its own mailbox full of the asks below, and `right` asks back into that full
                # mailbox. That ask names `left` as waiting on it, so it fails before it would wait for room, well
                # within the deadline of the asks.
                async with asyncio.timeout(1):
                    outcomes = await asyncio.gather(*(ref.ask(Balance) for _ in range(3)), return_exceptions=True)

            # `right` raised it, and `left`, whose ask failed, raised in turn.
            assert all(
                isinstance(outcome, ActorFailed) and "ReentrancyError" in outcome.message for outcome in outcomes
            ), outcomes

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

    def when_a_type_sets_its_own_timings() -> None:
        async def it_waits_its_own_ask_timeout_while_a_type_that_sets_none_waits_the_system_one() -> None:
            @actor(initial=Account(), ask_timeout=timedelta(seconds=5))
            async def patient(ctx: Context[Account, Balance]) -> None:
                async for msg in ctx.inbox:
                    await asyncio.sleep(0.3)
                    msg.reply_to.tell(ctx.state.value.balance)

            @actor(initial=Account())
            async def hasty(ctx: Context[Account, Balance]) -> None:
                async for msg in ctx.inbox:
                    await asyncio.sleep(0.3)
                    msg.reply_to.tell(ctx.state.value.balance)

            async with ActorSystem(ask_timeout=timedelta(milliseconds=100)) as system:
                assert await system.ref(patient, "p-1").ask(Balance) == 0

                with pytest.raises(TimeoutError):
                    await system.ref(hasty, "h-1").ask(Balance)

        async def it_lets_go_at_its_own_idle_after_while_a_type_that_sets_none_stays() -> None:
            @actor(initial=Tally(), idle_after=timedelta(0))
            async def restless(ctx: Context[Tally, Starts]) -> None:
                await ctx.state.set(replace(ctx.state.value, starts=ctx.state.value.starts + 1))
                async for msg in ctx.inbox:
                    msg.reply_to.tell(ctx.state.value.starts)

            @actor(initial=Tally())
            async def resident(ctx: Context[Tally, Starts]) -> None:
                await ctx.state.set(replace(ctx.state.value, starts=ctx.state.value.starts + 1))
                async for msg in ctx.inbox:
                    msg.reply_to.tell(ctx.state.value.starts)

            async with ActorSystem() as system:
                resident_ref = system.ref(resident, "r-1")
                restless_ref = system.ref(restless, "r-1")

                assert [await resident_ref.ask(Starts) for _ in range(3)] == [1, 1, 1]
                # Obtaining the ref started the key already, and that life may have idled out before the first ask.
                starts = [await restless_ref.ask(Starts) for _ in range(3)]
                assert starts == list(range(starts[0], starts[0] + 3))

    def when_an_ask_is_cancelled() -> None:
        async def it_cancels_the_body_on_it_and_the_next_ask_is_answered_promptly() -> None:
            napping = asyncio.Event()
            stopped = asyncio.Event()

            @actor(initial=Account())
            async def sleeper(ctx: Context[Account, Nap | Balance]) -> None:
                async for msg in ctx.inbox:
                    match msg:
                        case Nap(reply_to):
                            napping.set()
                            try:
                                await asyncio.sleep(10)
                            except asyncio.CancelledError:
                                stopped.set()
                                raise
                            reply_to.tell(0)
                        case Balance(reply_to):
                            reply_to.tell(ctx.state.value.balance)
                        case _:
                            assert_never(msg)

            async with ActorSystem() as system:
                ref = system.ref(sleeper, "s-1")
                asked = asyncio.ensure_future(ref.ask(Nap))
                await napping.wait()
                asked.cancel()

                # The caller sees its own cancellation, as with any other await.
                with pytest.raises(asyncio.CancelledError):
                    await asked
                async with asyncio.timeout(1):
                    await stopped.wait()
                    assert await ref.ask(Balance) == 0

        async def it_raises_timeout_error_under_asyncio_timeout_and_frees_the_key() -> None:
            @actor(initial=Account())
            async def sleeper(ctx: Context[Account, Nap | Balance]) -> None:
                async for msg in ctx.inbox:
                    match msg:
                        case Nap(reply_to):
                            await asyncio.sleep(10)
                            reply_to.tell(0)
                        case Balance(reply_to):
                            reply_to.tell(ctx.state.value.balance)
                        case _:
                            assert_never(msg)

            async with ActorSystem() as system:
                ref = system.ref(sleeper, "s-1")

                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.05):
                        await ref.ask(Nap)
                async with asyncio.timeout(1):
                    assert await ref.ask(Balance) == 0

        async def it_frees_the_key_when_the_deadline_of_the_ask_passes() -> None:
            @actor(initial=Account(), ask_timeout=timedelta(milliseconds=100))
            async def sleeper(ctx: Context[Account, Nap | Balance]) -> None:
                async for msg in ctx.inbox:
                    match msg:
                        case Nap(reply_to):
                            await asyncio.sleep(10)
                            reply_to.tell(0)
                        case Balance(reply_to):
                            reply_to.tell(ctx.state.value.balance)
                        case _:
                            assert_never(msg)

            async with ActorSystem() as system:
                ref = system.ref(sleeper, "s-1")
                started = asyncio.get_running_loop().time()

                with pytest.raises(TimeoutError):
                    await ref.ask(Nap)
                assert await ref.ask(Balance) == 0
                assert asyncio.get_running_loop().time() - started < 1

        async def it_changes_nothing_once_the_answer_has_arrived() -> None:
            @actor(initial=Account())
            async def ledger(ctx: Context[Account, Deposit | Balance]) -> None:
                async for msg in ctx.inbox:
                    match msg:
                        case Deposit(reply_to, amount):
                            reply_to.tell(ctx.state.value.balance + amount)
                            # Still on the message after answering it.
                            await asyncio.sleep(0.05)
                            await ctx.state.set(Account(ctx.state.value.balance + amount))
                        case Balance(reply_to):
                            reply_to.tell(ctx.state.value.balance)
                        case _:
                            assert_never(msg)

            async with ActorSystem() as system:
                ref = system.ref(ledger, "l-1")
                asked = asyncio.ensure_future(ref.ask(Deposit, 5))
                assert await asked == 5

                assert not asked.cancel()
                # Queued behind the rest of the deposit, which ran to its write.
                assert await ref.ask(Balance) == 5

        async def it_drops_a_cancellation_that_crosses_the_answer_the_body_told() -> None:
            @actor(initial=Account())
            async def ledger(ctx: Context[Account, Deposit | Balance]) -> None:
                system = ctx.system
                assert isinstance(system, ActorSystem)
                async for msg in ctx.inbox:
                    match msg:
                        case Deposit(reply_to, amount):
                            reply_to.tell(ctx.state.value.balance + amount)
                            # The deadline of the caller passed while the answer was on its way, so its cancellation
                            # arrives now, while the body is still on the message.
                            system._cancel(ledger, ctx.key, reply_to)
                            await asyncio.sleep(0.05)
                            await ctx.state.set(Account(ctx.state.value.balance + amount))
                        case Balance(reply_to):
                            reply_to.tell(ctx.state.value.balance)
                        case _:
                            assert_never(msg)

            async with ActorSystem() as system:
                ref = system.ref(ledger, "l-1")
                assert await ref.ask(Deposit, 5) == 5

                # Queued behind the rest of the deposit, which the cancellation did not interrupt.
                async with asyncio.timeout(1):
                    assert await ref.ask(Balance) == 5

        async def it_leaves_the_state_last_confirmed_and_runs_the_body_again_from_it() -> None:
            halfway = asyncio.Event()
            starts: list[int] = []

            @actor(initial=Account())
            async def ledger(ctx: Context[Account, Deposit | Balance]) -> None:
                starts.append(ctx.state.value.balance)
                async for msg in ctx.inbox:
                    match msg:
                        case Deposit(reply_to, amount):
                            await ctx.state.set(Account(ctx.state.value.balance + amount))
                            halfway.set()
                            await asyncio.sleep(10)
                            await ctx.state.set(Account(ctx.state.value.balance + 1_000))
                            reply_to.tell(ctx.state.value.balance)
                        case Balance(reply_to):
                            reply_to.tell(ctx.state.value.balance)
                        case _:
                            assert_never(msg)

            async with ActorSystem() as system:
                ref = system.ref(ledger, "l-1")
                asked = asyncio.ensure_future(ref.ask(Deposit, 5))
                await halfway.wait()
                asked.cancel()

                async with asyncio.timeout(1):
                    assert await ref.ask(Balance) == 5
                # The first write was confirmed before the cancellation and the second never started; the body ran
                # again from the start, on the state the first one left.
                assert starts == [0, 5]

        async def it_drops_a_cancelled_request_still_in_the_mailbox_unread() -> None:
            gates = {amount: asyncio.Event() for amount in (1, 2, 3)}
            taken: list[int] = []

            @actor(initial=Account())
            async def slow(ctx: Context[Account, Deposit]) -> None:
                async for msg in ctx.inbox:
                    taken.append(msg.amount)
                    await gates[msg.amount].wait()
                    msg.reply_to.tell(msg.amount)

            async with ActorSystem() as system:
                ref = system.ref(slow, "s-1")
                first = asyncio.ensure_future(ref.ask(Deposit, 1))

                async def holds_the_first() -> None:
                    assert taken == [1]

                await eventually(holds_the_first)
                second = asyncio.ensure_future(ref.ask(Deposit, 2))
                third = asyncio.ensure_future(ref.ask(Deposit, 3))
                assert system._queued(slow, "s-1") == 2

                second.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await second
                assert system._queued(slow, "s-1") == 1

                for gate in gates.values():
                    gate.set()
                assert (await first, await third) == (1, 3)
            assert taken == [1, 3]

        async def it_lets_go_at_once_of_a_caller_held_for_room() -> None:
            gates = {amount: asyncio.Event() for amount in (1, 2, 3, 4)}
            taken: list[int] = []

            @actor(initial=Account(), mailbox=1, on_full="wait")
            async def slow(ctx: Context[Account, Deposit]) -> None:
                async for msg in ctx.inbox:
                    taken.append(msg.amount)
                    await gates[msg.amount].wait()
                    msg.reply_to.tell(msg.amount)

            async with ActorSystem() as system:
                ref = system.ref(slow, "s-1")
                first = asyncio.ensure_future(ref.ask(Deposit, 1))

                async def holds_the_first() -> None:
                    assert taken == [1]

                await eventually(holds_the_first)
                second = asyncio.ensure_future(ref.ask(Deposit, 2))
                # Both find the mailbox full, and wait for room in this order.
                third = asyncio.ensure_future(ref.ask(Deposit, 3))
                fourth = asyncio.ensure_future(ref.ask(Deposit, 4))
                third.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await third

                gates[1].set()

                # Taking the second makes room for one, and it goes to the fourth: the third is not held any more.
                async def queues_the_fourth() -> None:
                    assert (taken, system._queued(slow, "s-1")) == ([1, 2], 1)

                await eventually(queues_the_fourth)
                gates[2].set()
                gates[4].set()
                assert list(await asyncio.gather(first, second, fourth)) == [1, 2, 4]
            assert taken == [1, 2, 4]

        async def it_passes_on_to_the_ask_the_body_was_awaiting() -> None:
            napping = asyncio.Event()
            stopped = asyncio.Event()

            @actor(initial=Account())
            async def sleeper(ctx: Context[Account, Nap | Balance]) -> None:
                async for msg in ctx.inbox:
                    match msg:
                        case Nap(reply_to):
                            napping.set()
                            try:
                                await asyncio.sleep(10)
                            except asyncio.CancelledError:
                                stopped.set()
                                raise
                            reply_to.tell(0)
                        case Balance(reply_to):
                            reply_to.tell(ctx.state.value.balance)
                        case _:
                            assert_never(msg)

            @actor(initial=Account())
            async def front(ctx: Context[Account, Nap]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(await ctx.system.ref(sleeper, ctx.key).ask(Nap))

            async with ActorSystem() as system:
                asked = asyncio.ensure_future(system.ref(front, "k").ask(Nap))
                await napping.wait()
                asked.cancel()

                async with asyncio.timeout(1):
                    await stopped.wait()
                    assert await system.ref(sleeper, "k").ask(Balance) == 0

    def when_a_type_handles_several_messages_at_once() -> None:
        async def it_overlaps_up_to_its_concurrency_on_one_key_over_a_state_it_only_reads() -> None:
            inside = 0
            most = 0
            full = asyncio.Event()
            release = asyncio.Event()

            @actor(initial=Account(balance=7), concurrency=4)
            async def lookup(ctx: Context[Account, Balance]) -> None:
                nonlocal inside, most
                async for msg in ctx.inbox:
                    inside += 1
                    most = max(most, inside)
                    if inside == 4:
                        full.set()
                    await release.wait()
                    inside -= 1
                    msg.reply_to.tell(ctx.state.value.balance)

            assert lookup.concurrency == 4
            async with ActorSystem() as system:
                ref = system.ref(lookup, "l-1")
                answers = [asyncio.ensure_future(ref.ask(Balance)) for _ in range(10)]
                async with asyncio.timeout(1):
                    await full.wait()
                await asyncio.sleep(0.05)

                # Four messages of one key in hand at once, and the other six queued behind them.
                assert (inside, system._queued(lookup, "l-1")) == (4, 6)
                release.set()
                assert await asyncio.gather(*answers) == [7] * 10
            assert most == 4

        async def it_refuses_every_write_of_the_state() -> None:
            @actor(initial=Account(balance=3), concurrency=2)
            async def reader(ctx: Context[Account, Change]) -> None:
                async for msg in ctx.inbox:
                    match msg.how:
                        case "add":
                            await ctx.state.set(Account(ctx.state.value.balance + 1))
                        case "double":
                            await ctx.state.update(lambda current: Account(current.balance * 2))
                        case "break":
                            await ctx.become(reader)
                        case "read":
                            pass
                    msg.reply_to.tell(ctx.state.value.balance)

            writes: tuple[Literal["add", "double", "break"], ...] = ("add", "double", "break")
            async with ActorSystem(backoff=Backoff(first=timedelta(milliseconds=10))) as system:
                ref = system.ref(reader, "r-1")
                for how in writes:
                    with pytest.raises(ActorFailed, match="read-only"):
                        await ref.ask(Change, how)

                assert await ref.ask(Change, "read") == 3

        async def it_keeps_one_message_at_a_time_by_default() -> None:
            inside = 0
            most = 0

            @actor(initial=Account())
            async def single(ctx: Context[Account, Balance]) -> None:
                nonlocal inside, most
                async for msg in ctx.inbox:
                    inside += 1
                    most = max(most, inside)
                    await asyncio.sleep(0.01)
                    inside -= 1
                    msg.reply_to.tell(ctx.state.value.balance)

            assert single.concurrency == 1
            async with ActorSystem() as system:
                ref = system.ref(single, "s-1")
                assert await asyncio.gather(*(ref.ask(Balance) for _ in range(5))) == [0] * 5
            assert most == 1

        def it_is_refused_for_the_body_of_a_collection() -> None:
            with pytest.raises(ValueError, match="concurrency"):
                actor(initial=0, concurrency=2)(counter.actor.body)

    def when_asks_come_back_to_a_key_that_waits() -> None:
        async def it_raises_reentrancy_error_naming_both_keys_when_two_actors_ask_each_other() -> None:
            @actor(initial=Account())
            async def ping(ctx: Context[Account, Relay]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(await ctx.system.ref(pong, ctx.key).ask(Relay))

            @actor(initial=Account())
            async def pong(ctx: Context[Account, Relay]) -> None:
                async for msg in ctx.inbox:
                    try:
                        msg.reply_to.tell(await ctx.system.ref(ping, ctx.key).ask(Relay))
                    except ReentrancyError as error:
                        msg.reply_to.tell(str(error))

            async with ActorSystem() as system:
                # Well within the deadline of the asks, which is where the cycle used to end.
                async with asyncio.timeout(1):
                    answer = await system.ref(ping, "k").ask(Relay)

            assert f"{ping.name}/k -> {pong.name}/k -> {ping.name}/k" in answer

        async def it_raises_at_once_when_a_body_asks_its_own_key() -> None:
            @actor(initial=Account())
            async def narcissus(ctx: Context[Account, Relay]) -> None:
                async for msg in ctx.inbox:
                    try:
                        msg.reply_to.tell(await ctx.self.ask(Relay))
                    except ReentrancyError as error:
                        msg.reply_to.tell(str(error))

            async with ActorSystem() as system:
                started = asyncio.get_running_loop().time()
                answer = await system.ref(narcissus, "n").ask(Relay)
                assert asyncio.get_running_loop().time() - started < 0.5

            assert f"{narcissus.name}/n -> {narcissus.name}/n" in answer

        async def it_lets_a_long_acyclic_chain_through_and_names_only_its_most_recent_callers() -> None:
            @actor(initial=Account())
            async def hop(ctx: Context[Account, Descend]) -> None:
                system = ctx.system
                assert isinstance(system, ActorSystem)
                async for msg in ctx.inbox:
                    if msg.depth == 0:
                        msg.reply_to.tell(tuple(system._chain()))
                    else:
                        below = system.ref(hop, str(int(ctx.key) + 1))
                        msg.reply_to.tell(await below.ask(Descend, msg.depth - 1))

            async with ActorSystem() as system:
                async with asyncio.timeout(5):
                    chain = await system.ref(hop, "0").ask(Descend, 40)

            # Forty-one keys deep, an ask of the last would name sixteen: the most recent, itself last.
            assert chain == tuple(f"{hop.name}/{key}" for key in range(25, 41))

        async def it_names_only_the_bodies_that_wait_for_the_answer() -> None:
            seen: dict[str, list[str]] = {}

            @actor(initial=Account())
            async def bottom(ctx: Context[Account, Note]) -> None:
                system = ctx.system
                assert isinstance(system, ActorSystem)
                async for _ in ctx.inbox:
                    seen["told"] = system._chain()

            @actor(initial=Account())
            async def middle(ctx: Context[Account, Relay]) -> None:
                system = ctx.system
                assert isinstance(system, ActorSystem)

                async def beside() -> list[str]:
                    return system._chain()

                async for msg in ctx.inbox:
                    seen["waiting"] = system._chain()
                    seen["beside"] = await asyncio.create_task(beside())
                    system.ref(bottom, ctx.key).tell(Note("done"))
                    msg.reply_to.tell("")
                    seen["answered"] = system._chain()

            @actor(initial=Account())
            async def top(ctx: Context[Account, Relay]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(await ctx.system.ref(middle, ctx.key).ask(Relay))

            async with ActorSystem() as system:
                await system.ref(top, "k").ask(Relay)
                outside = system._chain()

                async def told() -> None:
                    assert "told" in seen

                await eventually(told)

            assert outside == []
            assert seen == {
                # `top` waits for `middle`, which holds its own key until it reads again.
                "waiting": [f"{top.name}/k", f"{middle.name}/k"],
                # A task the body starts beside it holds nothing.
                "beside": [],
                # A `tell` keeps nobody waiting.
                "told": [f"{bottom.name}/k"],
                # Answered, the request keeps `top` waiting no more.
                "answered": [f"{middle.name}/k"],
            }

        async def it_lets_a_body_that_has_read_since_be_asked_back() -> None:
            answers: list[str] = []

            @actor(initial=Account())
            async def caller(ctx: Context[Account, Relay]) -> None:
                pending: list[asyncio.Future[str]] = []
                async for msg in ctx.inbox:
                    if not pending:
                        # Asked without waiting: the body reads its next message while the answer is on its way.
                        pending.append(asyncio.ensure_future(ctx.system.ref(callee, ctx.key).ask(Relay)))
                    msg.reply_to.tell("caller")

            @actor(initial=Account())
            async def callee(ctx: Context[Account, Relay]) -> None:
                async for msg in ctx.inbox:
                    answers.append(await ctx.system.ref(caller, ctx.key).ask(Relay))
                    msg.reply_to.tell("callee")

            async with ActorSystem() as system:
                assert await system.ref(caller, "k").ask(Relay) == "caller"

                async def asked_back() -> None:
                    assert answers == ["caller"]

                await eventually(asked_back)

        async def it_takes_a_cycle_into_a_free_run_and_refuses_it_once_every_run_waits() -> None:
            async def bounced(to: Ref[Bounce], hops: int) -> str:
                if hops == 0:
                    return "landed"
                try:
                    return await to.ask(Bounce, hops - 1)
                except ReentrancyError as error:
                    return str(error)

            @actor(initial=Account(), concurrency=2)
            async def there(ctx: Context[Account, Bounce]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(await bounced(ctx.system.ref(back, ctx.key), msg.hops))

            @actor(initial=Account(), concurrency=2)
            async def back(ctx: Context[Account, Bounce]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(await bounced(ctx.system.ref(there, ctx.key), msg.hops))

            async with ActorSystem() as system:
                async with asyncio.timeout(2):
                    # There, back, and there again: the second run of `there` takes it.
                    assert await system.ref(there, "a").ask(Bounce, 2) == "landed"
                    # Twice round, both runs of each key wait down the chain, and the fifth hop has none to read it.
                    refused = await system.ref(there, "b").ask(Bounce, 4)

            assert f"{there.name}/b -> {back.name}/b -> {there.name}/b -> {back.name}/b -> {there.name}/b" in refused
