import asyncio
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import timedelta
from itertools import pairwise
from typing import assert_never

import pytest

from casty import (
    ActivationEnded,
    ActivationStarted,
    ActorSystem,
    Context,
    Event,
    HandoffEnded,
    HandoffStarted,
    NodeId,
    Unavailable,
    actor,
)
from tests.app import FEED, Account, Append, Deposit, Location, Where, account, consumer, ledger
from tests.cluster import FAST, Harness, Node
from tests.support import eventually
from tests.traffic import Traffic

WITHIN = timedelta(seconds=15)
LAST = 99
"""Offset the consumers of the third test read up to."""
KEPT = replace(FAST, remove_after=None)
"""A machine that died is buried but never removed, so the ring holds and a key changes hands once."""
NAMES = tuple(f"worker-{index}" for index in range(24))
"""The keys pinned to each node: enough that some of them lie in the ranges a change of the ring moves."""

type PostMsg = Deposit | Where


@actor(pinned=True, initial=Account())
async def post(ctx: Context[Account, PostMsg]) -> None:
    """An account that runs on the node its ref names: its one copy is there, and no change of the ring moves it."""
    async for msg in ctx.inbox:
        match msg:
            case Deposit(reply_to, amount):
                await ctx.state.set(Account(ctx.state.value.balance + amount))
                reply_to.tell(ctx.state.value.balance)
            case Where(reply_to):
                reply_to.tell(Location(ctx.state.value.balance, ctx.system.node))
            case _:
                assert_never(msg)


def describe_handoff() -> None:
    def when_nodes_join_during_traffic() -> None:
        async def it_moves_keys_to_them_without_losing_confirmed_state() -> None:
            async with Harness.start(3) as harness:
                async with Traffic.running(harness) as traffic:
                    await traffic.settle()
                    joined = await asyncio.gather(harness.add(), harness.add(), harness.add())
                    await _everyone_sees(harness, 6)
                    await traffic.settle(timedelta(seconds=2))
                listings = await traffic.verify()

                owners = {listing.node for listing in listings.values()}
                assert {node.system.node for node in joined} <= owners

    def when_a_machine_disappears_during_traffic() -> None:
        async def it_moves_its_keys_without_losing_confirmed_state() -> None:
            async with Harness.start(3) as harness:
                a, gone, c = harness.nodes
                async with Traffic.running(harness) as traffic:
                    await traffic.settle()
                    harness.isolate(gone)
                    await harness.crash(gone)
                    await _everyone_sees(harness, 2)
                    await traffic.settle(timedelta(seconds=1))
                listings = await traffic.verify()

                owners = {listing.node for listing in listings.values()}
                assert owners <= {a.system.node, c.system.node}

    def when_an_active_consumer_loses_its_machine() -> None:
        async def it_resumes_on_another_node_from_the_last_saved_offset() -> None:
            FEED.reset(last=LAST, pace=timedelta(milliseconds=10))
            partitions = tuple(f"partition-{index}" for index in range(6))
            # The machine is buried but never removed, so the ring holds and each partition changes hands once.
            async with Harness.start(3, timing=KEPT) as harness:
                starting = harness.nodes[0].system
                for partition in partitions:
                    starting.ref(consumer, partition)
                await _every_partition_past(partitions, 10)
                gone = _busiest(harness)
                lost, steady = gone.system.node, len(FEED.log)
                harness.isolate(gone)
                await harness.crash(gone)
                # Nothing is sent to the consumers from here on: what brings them back is the mark on their state.
                await _every_partition_past(partitions, LAST)

            log = FEED.log
            taken = {partition for partition, _, node in log[:steady] if node == lost}
            assert taken, "the machine that was lost was running no partition"
            for partition in partitions:
                offsets = [offset for held, offset, _ in log if held == partition]
                assert set(offsets) == set(range(LAST + 1)), f"{partition} skipped offsets"
                for before, after in pairwise(offsets):
                    # Either the next record, or a restart at one already processed: never a record nobody read.
                    assert after <= before + 1, f"{partition} went from {before} to {after}"
                repeated = {offset for offset in offsets if offsets.count(offset) > 1}
                elsewhere = repeated - {offset for held, offset, node in log if held == partition and node == lost}
                assert not elsewhere, f"{partition} read {elsewhere} twice away from the machine that was lost"
            for partition in taken:
                assert lost not in {node for held, offset, node in log if held == partition and offset == LAST}

    def when_a_node_joins_and_takes_over_active_consumers() -> None:
        async def it_resumes_them_there_once_their_state_has_arrived() -> None:
            FEED.reset(last=LAST, pace=timedelta(milliseconds=10))
            partitions = tuple(f"partition-{index}" for index in range(24))
            # An hour to suspect: the table stops changing once the node is in, so no later sweep can do the work of
            # the one that has to follow the transfer.
            timing = replace(FAST, suspect_after=timedelta(hours=1))
            async with Harness.start(3, timing=timing) as harness:
                starting = harness.nodes[0].system
                for partition in partitions:
                    starting.ref(consumer, partition)
                await _every_partition_past(partitions, 10)
                joined = (await harness.add()).system.node
                # Nothing is sent to the consumers: a partition that moved is only brought back by its mark, and the
                # mark reaches the node that joined inside the transfer, after the sweep of the change already ran.
                await _every_partition_past(partitions, LAST)

            assert joined in {node for _, offset, node in FEED.log if offset == LAST}

    def when_a_node_joins_while_keys_run() -> None:
        async def it_ends_each_activation_whose_key_moved_to_it() -> None:
            events: defaultdict[int, list[Event]] = defaultdict(list)
            # Nothing idles out while the test runs: an activation that ended did so because its key moved.
            timing = replace(FAST, idle_after=timedelta(minutes=5))
            async with Harness.start(3, timing=timing, observer=lambda source: events[source].append) as harness:
                starting = harness.nodes[0].system
                keys = tuple(f"a-{index}" for index in range(48))
                before = await _located(starting, keys)
                joined = (await harness.add()).system.node
                ids = {node.system.node: node.id for node in harness.nodes}

                async def the_keys_that_moved_ended_where_they_ran() -> None:
                    after = await _located(starting, keys)
                    moved = [key for key in keys if after[key] == joined]
                    assert moved, "no key moved to the node that joined"
                    running = [
                        key for key in moved if ActivationEnded(account.name, key) not in events[ids[before[key]]]
                    ]
                    assert not running, f"{running} still run on the node that had them before"

                await eventually(the_keys_that_moved_ended_where_they_ran, WITHIN)

    def when_a_node_leaves_and_nobody_takes_what_it_keeps() -> None:
        async def it_reports_the_keys_it_went_without() -> None:
            events: defaultdict[int, list[Event]] = defaultdict(list)
            # Never suspected, the node cut off stays one the handover waits for, until the leave gives up on it.
            timing = replace(FAST, suspect_after=timedelta(hours=1), leave_timeout=timedelta(milliseconds=500))
            async with Harness.start(2, timing=timing, observer=lambda source: events[source].append) as harness:
                going, kept = harness.nodes
                assert await going.system.ref(ledger, "l-1").ask(Append, 1)
                harness.isolate(kept)
                await harness.leave(going)

            abandoned = [event for event in events[going.id] if isinstance(event, HandoffEnded) and event.abandoned]
            assert abandoned == [HandoffEnded(ledger.name, "out", ("l-1",))]

    def when_keys_are_pinned() -> None:
        async def it_neither_moves_nor_copies_them_while_the_ring_changes() -> None:
            events: defaultdict[int, list[Event]] = defaultdict(list)
            # Nothing idles out while the test runs: each activation counted is the one the deposit started.
            timing = replace(FAST, idle_after=timedelta(minutes=5))
            async with Harness.start(4, timing=timing, observer=lambda source: events[source].append) as harness:
                *homes, going = harness.nodes
                asking = homes[0].system
                pinned = [(home, name) for home in homes for name in NAMES]
                deposited = await asyncio.gather(
                    *(asking.ref(post, name, at=home.address).ask(Deposit, 1) for home, name in pinned)
                )
                assert deposited == [1] * len(pinned)
                # Every node runs a type the ring places as well, whose ranges the change below moves.
                for node in harness.nodes:
                    assert await node.system.ref(account, f"a-{node.id}").ask(Deposit, 1) == 1
                left = going.system.node
                # The others gain the ranges it had: they pull those of `account`, and none of `post`.
                await harness.leave(going)

                async def the_ranges_arrived() -> None:
                    for node in harness.nodes:
                        assert _status(node, left) is None, f"{node.address} still lists the node that left"
                    filling = [node.address for node in harness.nodes if _filling(events[node.id], account.name)]
                    assert not filling, f"{filling} are still pulling ranges of {account.name}"
                    assert any(HandoffEnded(account.name, "in") in events[node.id] for node in harness.nodes)

                await eventually(the_ranges_arrived, WITHIN)
                found = await asyncio.gather(
                    *(asking.ref(post, name, at=home.address).ask(Where) for home, name in pinned)
                )
                assert found == [Location(1, home.system.node) for home, _ in pinned]

            started = sorted(
                (source, event.key)
                for source, seen in events.items()
                for event in seen
                if isinstance(event, ActivationStarted) and event.actor == post.name
            )
            assert started == sorted((home.id, _pinned(home, name)) for home, name in pinned)
            moved = [
                (source, event)
                for source, seen in events.items()
                for event in seen
                if isinstance(event, HandoffStarted | HandoffEnded) and event.actor == post.name
            ]
            assert not moved, f"a change of the ring moved the type whose keys are pinned: {moved}"

        async def it_refuses_them_once_their_node_is_gone_and_no_other_node_runs_them() -> None:
            events: defaultdict[int, list[Event]] = defaultdict(list)
            async with Harness.start(3, observer=lambda source: events[source].append) as harness:
                asking, gone, _ = harness.nodes
                for name in NAMES:
                    assert await asking.system.ref(post, name, at=gone.address).ask(Deposit, 1) == 1
                lost = gone.system.node
                harness.isolate(gone)
                await harness.crash(gone)

                async def dead() -> None:
                    for node in harness.nodes:
                        assert _status(node, lost) in {"dead", None}, f"{node.address} sees it {_status(node, lost)}"

                async def removed() -> None:
                    for node in harness.nodes:
                        assert _status(node, lost) is None, f"{node.address} sees it {_status(node, lost)}"

                await eventually(dead, WITHIN)
                await _refused(harness, gone)
                # Removed, it is out of the ring too, which moves every other key it had: none pinned to it.
                await eventually(removed, WITHIN)
                await _refused(harness, gone)

            elsewhere = [
                (source, event.key)
                for source, seen in events.items()
                if source != gone.id
                for event in seen
                if isinstance(event, ActivationStarted) and event.actor == post.name
            ]
            assert not elsewhere, f"{elsewhere} ran away from the node they are pinned to"

        async def it_starts_them_over_from_initial_on_a_process_back_on_the_address() -> None:
            async with Harness.start(3) as harness:
                asking, restarting, _ = harness.nodes
                refs = [asking.system.ref(post, name, at=restarting.address) for name in NAMES]
                for ref in refs:
                    assert await ref.ask(Deposit, 5) == 5
                old = restarting.system.node
                await harness.crash(restarting)
                revived = await harness.add(address=restarting.address)

                async def replaced() -> None:
                    for node in (asking, revived):
                        assert _status(node, revived.system.node) == "alive"
                        assert _status(node, old) != "alive"

                await eventually(replaced, WITHIN)
                # The refs taken before the crash name the address: they reach the new process, where the keys start
                # over, since their one copy died with the process before it.
                for ref in refs:
                    assert await ref.ask(Where) == Location(0, revived.system.node)

        async def it_ends_them_when_their_node_leaves_and_owes_them_to_nobody() -> None:
            events: defaultdict[int, list[Event]] = defaultdict(list)
            async with Harness.start(3, observer=lambda source: events[source].append) as harness:
                asking, going, _ = harness.nodes
                for name in NAMES:
                    assert await asking.system.ref(post, name, at=going.address).ask(Deposit, 1) == 1
                await harness.leave(going)
                await _everyone_sees(harness, 2)
                await _refused(harness, going)

            keys = {_pinned(going, name) for name in NAMES}
            ended = {event.key for event in events[going.id] if isinstance(event, ActivationEnded)}
            assert keys <= ended
            moved = [
                event
                for event in events[going.id]
                if isinstance(event, HandoffStarted | HandoffEnded) and event.actor == post.name
            ]
            assert not moved, f"the keys pinned to the node that left were handed over: {moved}"
            elsewhere = [
                (source, event.key)
                for source, seen in events.items()
                if source != going.id
                for event in seen
                if isinstance(event, ActivationStarted) and event.key in keys
            ]
            assert not elsewhere, f"{elsewhere} ran away from the node they are pinned to"


async def _located(system: ActorSystem, keys: tuple[str, ...], /) -> dict[str, NodeId]:
    """The node each of `keys` runs on, asked from `system`: a key that was not running starts."""
    found = await asyncio.gather(*(system.ref(account, key).ask(Where) for key in keys))
    return {key: location.node for key, location in zip(keys, found, strict=True)}


def _busiest(harness: Harness, /) -> Node:
    """The node running the most partitions, which is the one worth taking away."""
    counts = Counter(node for _, _, node in FEED.log)
    return max(harness.nodes, key=lambda node: counts[node.system.node])


async def _every_partition_past(partitions: tuple[str, ...], offset: int, /) -> None:
    async def all_of_them_got_there() -> None:
        reached = {partition: -1 for partition in partitions}
        for partition, at, _ in FEED.log:
            reached[partition] = max(reached[partition], at)
        behind = {partition: at for partition, at in reached.items() if at < offset}
        assert not behind, f"{behind} are short of {offset}"

    await eventually(all_of_them_got_there, WITHIN)


def _pinned(node: Node, name: str, /) -> str:
    """The key `name` of `post` pinned to `node`, as `ctx.key` and the events read it."""
    return f"@{node.address}/{name}"


def _status(node: Node, of: NodeId, /) -> str | None:
    """The status `node` sees `of` in, or nothing once its table no longer lists it."""
    return next((member.status for member in node.system.members if member.node == of), None)


def _filling(seen: list[Event], actor: str, /) -> bool:
    """Whether the ranges of `actor` a node last started pulling, as `seen` by its observer, are still arriving."""
    moves = [
        event
        for event in seen
        if isinstance(event, HandoffStarted | HandoffEnded) and event.actor == actor and event.direction == "in"
    ]
    return bool(moves) and isinstance(moves[-1], HandoffStarted)


async def _refused(harness: Harness, gone: Node, /) -> None:
    """Ask every key of `post` pinned to `gone` from every node still running, which each time raises `Unavailable`."""
    for node in harness.nodes:
        for name in NAMES:
            with pytest.raises(Unavailable):
                await node.system.ref(post, name, at=gone.address).ask(Where)


async def _everyone_sees(harness: Harness, count: int, /) -> None:
    async def every_node_sees_the_others() -> None:
        for node in harness.nodes:
            alive = {member.node for member in node.system.members if member.status == "alive"}
            assert alive == {other.system.node for other in harness.nodes}, f"{node.address} sees {len(alive)}"
        assert len(harness.nodes) == count

    await eventually(every_node_sees_the_others, WITHIN)
