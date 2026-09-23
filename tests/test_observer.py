import asyncio
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import get_args

import pytest

from casty import (
    ActivationEnded,
    ActivationStarted,
    ActorFailed,
    ActorSystem,
    Context,
    Event,
    MessageDropped,
    Ref,
    Unavailable,
    actor,
)
from tests.app import Append, Deposit, Entries, account, ledger
from tests.cluster import FAST, Harness, Node
from tests.support import eventually

EVERY: frozenset[type[object]] = frozenset(get_args(Event.__value__))
"""Every kind of event the API documents."""
QUICK = replace(FAST, idle_after=timedelta(milliseconds=300))
"""Keys idle out while the test goes on, and a node that died is removed a second after it is buried."""


class Recorder:
    """Every event one node reported, in the order its observer was called."""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def __call__(self, event: Event, /) -> None:
        self.events.append(event)


class Choosy(Recorder):
    """A recorder that says it takes dropped messages and nothing else."""

    def wants(self, kind: type[Event], /) -> bool:
        return kind is MessageDropped


@dataclass(frozen=True)
class Idle:
    pass


@dataclass(frozen=True)
class Crash:
    reply_to: Ref[bool]


@dataclass(frozen=True)
class Nap:
    pass


@actor(initial=Idle())
async def brittle(ctx: Context[Idle, Crash]) -> None:
    async for _ in ctx.inbox:
        raise RuntimeError("the body broke")


@actor(initial=Idle(), mailbox=1)
async def sleepy(ctx: Context[Idle, Nap]) -> None:
    async for _ in ctx.inbox:
        await asyncio.sleep(1)


def describe_observer() -> None:
    def when_a_cluster_crashes_partitions_and_heals() -> None:
        async def it_reports_every_kind_of_event() -> None:
            recorders: defaultdict[int, Recorder] = defaultdict(Recorder)

            async with Harness.start(4, timing=QUICK, observer=recorders.__getitem__) as harness:
                a, b, c, d = harness.nodes
                key = await _key_on(a)
                with pytest.raises(ActorFailed):
                    await a.system.ref(brittle, "b-1").ask(Crash)
                # One message runs or waits, and a mailbox of one holds the next: the third has nowhere to go.
                for _ in range(3):
                    a.system.ref(sleepy, "s-1").tell(Nap())

                harness.partition({a}, {b, c, d})
                with pytest.raises(Unavailable):
                    await a.system.ref(ledger, key).ask(Append, 100)
                harness.heal()

                harness.isolate(d)
                await harness.crash(d)

                async def every_kind_was_seen() -> None:
                    seen = {type(event) for recorder in recorders.values() for event in recorder.events}
                    missing = sorted(kind.__name__ for kind in EVERY - seen)
                    assert not missing, f"never reported: {missing}"

                await eventually(every_kind_was_seen, timedelta(seconds=20))

    def when_a_system_runs_alone() -> None:
        async def it_reports_an_activation_as_it_starts_and_as_it_idles_out() -> None:
            recorder = Recorder()

            async with ActorSystem(observer=recorder, idle_after=timedelta(milliseconds=50)) as system:
                assert await system.ref(account, "a-1").ask(Deposit, 1) == 1

                async def it_started_and_ended() -> None:
                    assert recorder.events == [
                        ActivationStarted(account.name, "a-1"),
                        ActivationEnded(account.name, "a-1"),
                    ]

                await eventually(it_started_and_ended)

    def when_the_observer_raises() -> None:
        async def it_reports_the_exception_to_the_loop_and_the_system_carries_on(
            caplog: pytest.LogCaptureFixture,
        ) -> None:
            def broken(event: Event, /) -> None:
                raise RuntimeError(f"the observer broke on {event}")

            async with ActorSystem(observer=broken) as system:
                assert await system.ref(account, "a-1").ask(Deposit, 1) == 1

                async def the_failure_was_logged() -> None:
                    assert any(
                        record.exc_info is not None and "the observer broke" in str(record.exc_info[1])
                        for record in caplog.records
                    )

                await eventually(the_failure_was_logged)
                assert await system.ref(account, "a-2").ask(Deposit, 2) == 2
                assert await system.ref(account, "a-1").ask(Deposit, 1) == 2

    def when_the_observer_says_which_kinds_it_takes() -> None:
        async def it_is_given_no_event_of_another_kind() -> None:
            choosy = Choosy()

            async with ActorSystem(observer=choosy, idle_after=timedelta(milliseconds=100)) as system:
                # One message runs or waits, and a mailbox of one holds the next: the third has nowhere to go.
                for _ in range(3):
                    system.ref(sleepy, "s-1").tell(Nap())

                async def the_drop_was_given_and_the_key_idled_out() -> None:
                    assert choosy.events
                    assert system.stats().actors[sleepy.name].active == 0

                await eventually(the_drop_was_given_and_the_key_idled_out)

            assert {type(event) for event in choosy.events} == {MessageDropped}


async def _key_on(node: Node) -> str:
    """The first of `k-0`, `k-1`, … whose owner is `node`, asked from it: every key asked leaves a ledger behind."""
    for index in range(_TRIES):
        key = f"k-{index}"
        if (await node.system.ref(ledger, key).ask(Entries)).node == node.system.node:
            return key
    raise AssertionError(f"none of the first {_TRIES} keys is owned by {node.address}")


_TRIES = 100
