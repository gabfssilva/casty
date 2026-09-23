import asyncio
import hashlib
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import assert_never

import pytest

from casty import Context, DefaultedActor, NodeId, Ref, Unavailable, actor
from tests.app import Append, Entries, Ledger, LedgerMsg, Pay, Pending, ledger, loose, order
from tests.cluster import FAST, NARROW, WITHIN, Harness, Node
from tests.support import eventually

STUBBORN = replace(FAST, remove_after=None)


@dataclass(frozen=True)
class Blob:
    data: bytes = b""


@dataclass(frozen=True)
class Fill:
    reply_to: Ref[int]
    size: int


@dataclass(frozen=True)
class Summary:
    size: int
    sha256: str
    node: NodeId


@dataclass(frozen=True)
class Digest:
    reply_to: Ref[Summary]


type BlobMsg = Fill | Digest


@dataclass(frozen=True)
class Deposit:
    reply_to: Ref[int]
    amount: int


@dataclass(frozen=True)
class Balance:
    reply_to: Ref[tuple[int, NodeId]]


@dataclass(frozen=True)
class Close:
    reply_to: Ref[None]


type SessionMsg = Deposit | Balance | Close


@actor(initial=0)
async def session(ctx: Context[int, SessionMsg]) -> None:
    """A balance that `Close` deletes, going on from 0."""
    async for msg in ctx.inbox:
        match msg:
            case Deposit(reply_to, amount):
                reply_to.tell(await ctx.state.update(lambda held: held + amount))
            case Balance(reply_to):
                reply_to.tell((ctx.state.value, ctx.system.node))
            case Close(reply_to):
                await ctx.state.delete()
                reply_to.tell(None)
            case _:
                assert_never(msg)


@actor(initial=Blob())
async def blob(ctx: Context[Blob, BlobMsg]) -> None:
    """Keeps one field larger than a message, built here so that no message has to carry it."""
    async for msg in ctx.inbox:
        match msg:
            case Fill(reply_to, size):
                await ctx.state.set(Blob(_pattern(size)))
                reply_to.tell(len(ctx.state.value.data))
            case Digest(reply_to):
                data = ctx.state.value.data
                reply_to.tell(Summary(len(data), hashlib.sha256(data).hexdigest(), ctx.system.node))
            case _:
                assert_never(msg)


def describe_replication() -> None:
    def when_the_owner_machine_disappears() -> None:
        async def it_keeps_every_confirmed_save() -> None:
            async with Harness.start(3) as harness:
                a, b, _ = harness.nodes
                key = await _key_on(b, a)
                gone = b.system.node
                for entry in range(1, 11):
                    assert await a.system.ref(ledger, key).ask(Append, entry)

                harness.isolate(b)
                await harness.crash(b)

                async def another_node_answers_with_everything() -> None:
                    listing = await a.system.ref(ledger, key).ask(Entries)
                    assert listing.entries == tuple(range(1, 11))
                    assert listing.node != gone

                await eventually(another_node_answers_with_everything, WITHIN)

    def when_the_owner_is_on_the_minority_side_of_a_partition() -> None:
        async def it_confirms_nothing_there_while_the_majority_continues() -> None:
            async with Harness.start(3, timing=STUBBORN) as harness:
                a, b, c = harness.nodes
                key = await _key_on(a, a)
                for entry in range(1, 6):
                    assert await a.system.ref(ledger, key).ask(Append, entry)

                harness.partition({a}, {b, c})

                with pytest.raises(Unavailable):
                    await a.system.ref(ledger, key).ask(Append, 100)

                tried: list[int] = []
                confirmed: list[int] = []

                async def the_majority_appends() -> None:
                    # A new entry per attempt: `Unavailable` says the message may still have been applied, and a
                    # blocked proxy delivers what it held on the healing.
                    entry = 10 + len(tried)
                    tried.append(entry)
                    assert await b.system.ref(ledger, key).ask(Append, entry)
                    confirmed.append(entry)

                await eventually(the_majority_appends, WITHIN)
                harness.heal()

                async def every_node_answers_with_the_majority_state() -> None:
                    for node in (a, b, c):
                        entries = (await node.system.ref(ledger, key).ask(Entries)).entries
                        assert entries[:5] == tuple(range(1, 6))
                        assert len(set(entries)) == len(entries), f"an entry was applied twice: {entries}"
                        assert set(confirmed) <= set(entries) <= {*range(1, 6), *tried}

                await eventually(every_node_answers_with_the_majority_state, WITHIN)

    def when_the_owner_comes_back_after_the_majority_took_its_key_over() -> None:
        async def it_never_answers_from_the_state_it_had_before() -> None:
            async with Harness.start(3, timing=STUBBORN) as harness:
                a, b, c = harness.nodes
                key = await _key_on(a, a)
                for entry in range(1, 6):
                    assert await a.system.ref(ledger, key).ask(Append, entry)

                harness.partition({a}, {b, c})

                # Sent only once the majority owns the key, so that nothing `a` would be fenced by waits for the heal.
                async def the_majority_owns_the_key() -> None:
                    assert (await b.system.placement(ledger, key)).owner not in (None, a.system.node)

                await eventually(the_majority_owns_the_key, WITHIN)
                assert await b.system.ref(ledger, key).ask(Append, 6)
                harness.heal()

                # A read writes nothing, so no fence stops an activation `a` kept from before: whichever node answers,
                # it must answer with what the majority confirmed meanwhile, until the key is back on `a`.
                deadline = asyncio.get_running_loop().time() + WITHIN.total_seconds()
                while True:
                    with suppress(Unavailable, TimeoutError):
                        listing = await b.system.ref(ledger, key).ask(Entries)
                        assert listing.entries == tuple(range(1, 7)), f"{listing.node} answered {listing.entries}"
                        if listing.node == a.system.node:
                            break
                    assert asyncio.get_running_loop().time() < deadline, "the key never came back to its owner"
                    await asyncio.sleep(0.05)

    def when_the_old_owner_receives_a_message_after_healing() -> None:
        async def it_applies_the_message_to_the_current_state_only() -> None:
            patient = replace(STUBBORN, idle_after=timedelta(minutes=5))

            async with Harness.start(3, timing=patient) as harness:
                a, b, c = harness.nodes
                key = await _key_on(a, a)
                for entry in range(1, 6):
                    assert await a.system.ref(ledger, key).ask(Append, entry)

                harness.partition({a}, {b, c})
                tried: list[int] = []
                confirmed: list[int] = []

                async def appends(node: Node) -> None:
                    # A new id per attempt: a refused `ask` may still have been applied, so reusing one would show up
                    # as a repetition that no rule was broken to produce.
                    entry = 10 + len(tried)
                    tried.append(entry)
                    assert await node.system.ref(ledger, key).ask(Append, entry)
                    confirmed.append(entry)

                await eventually(lambda: appends(b), WITHIN)
                harness.heal()

                # `a` gave the key up when it lost sight of the majority, and takes it back only once it sees them
                # again and the state they built has arrived: until then the key refuses what `a` is sent, this and
                # any attempt the blocked proxy held and delivers on the healing. None of it may apply to the state
                # `a` had before.
                seven = await _appended(a, key, 7)
                await eventually(lambda: appends(a), WITHIN)
                kept: set[int] = {*range(1, 6), *confirmed, *([7] if seven else [])}
                possible = {*range(1, 6), *tried, 7}

                async def every_node_answers_with_what_the_majority_confirmed() -> None:
                    for node in (a, b, c):
                        entries = (await node.system.ref(ledger, key).ask(Entries)).entries
                        where = f"{node.address} answered {entries}"
                        # Without the fencing, the old owner writes its own state over the one the majority built, and
                        # what it confirmed there is gone.
                        assert kept <= set(entries), where
                        assert set(entries) <= possible, where
                        assert len(set(entries)) == len(entries), where

                await eventually(every_node_answers_with_what_the_majority_confirmed, WITHIN)

    def when_a_type_writes_with_one_replica() -> None:
        async def it_keeps_confirming_on_the_minority_side() -> None:
            async with Harness.start(3, timing=STUBBORN) as harness:
                a, b, c = harness.nodes
                key = await _key_on(a, a, loose)

                harness.partition({a}, {b, c})

                # What survives the healing is not asserted: `one` promises nothing about it.
                assert await a.system.ref(loose, key).ask(Append, 1)

    def when_a_key_that_became_another_behavior_loses_its_machine() -> None:
        async def it_comes_back_on_another_node_as_the_behavior_it_became() -> None:
            orders = tuple(f"o-{index}" for index in range(12))
            async with Harness.start(3) as harness:
                a, gone, _ = harness.nodes
                for key in orders:
                    assert await a.system.ref(order, key, initial=Pending()).ask(Pay, 10) is True
                harness.isolate(gone)
                await harness.crash(gone)

                async def every_order_is_still_paid() -> None:
                    # `True` would be an order that came back pending, and paid twice.
                    for key in orders:
                        assert await a.system.ref(order, key, initial=Pending()).ask(Pay, 10) is False, key

                await eventually(every_order_is_still_paid, WITHIN)

    def when_a_field_is_several_times_the_message_limit() -> None:
        async def it_saves_it_in_parts_and_the_next_owner_reads_it_back_identically() -> None:
            size = 3 * NARROW.message
            written = hashlib.sha256(_pattern(size)).hexdigest()
            async with Harness.start(3, limits=NARROW) as harness:
                a, b, _ = harness.nodes
                key = await _blob_on(b, a)
                gone = b.system.node
                assert await a.system.ref(blob, key).ask(Fill, size) == size

                harness.isolate(b)
                await harness.crash(b)

                async def another_node_reads_it_back() -> None:
                    summary = await a.system.ref(blob, key).ask(Digest)
                    assert summary.node != gone
                    assert (summary.size, summary.sha256) == (size, written)

                await eventually(another_node_reads_it_back, WITHIN)

    def when_a_key_deletes_its_state() -> None:
        async def it_keeps_it_on_no_node_and_starts_again_from_initial_where_it_goes_next() -> None:
            async with Harness.start(3) as harness:
                a, b, _ = harness.nodes
                key = await _session_on(b, a)
                gone = b.system.node
                assert await a.system.ref(session, key).ask(Deposit, 5) == 5

                await a.system.ref(session, key).ask(Close)

                # The body goes on from the default of its type.
                assert (await a.system.ref(session, key).ask(Balance))[0] == 0

                async def no_node_keeps_a_page_of_it() -> None:
                    for node in harness.nodes:
                        stored = await node.system._stored()  # pyright: ignore[reportPrivateUsage]
                        assert all(deleted for actor, held, deleted in stored if (actor, held) == (session.name, key))

                async def no_node_keeps_anything_of_it() -> None:
                    for node in harness.nodes:
                        stored = await node.system._stored()  # pyright: ignore[reportPrivateUsage]
                        assert (session.name, key) not in {(actor, held) for actor, held, _ in stored}

                await eventually(no_node_keeps_a_page_of_it, WITHIN)
                await eventually(no_node_keeps_anything_of_it, timedelta(seconds=30))

                harness.isolate(b)
                await harness.crash(b)

                async def another_node_starts_it_from_initial() -> None:
                    balance, node = await a.system.ref(session, key).ask(Balance)
                    assert node != gone
                    assert balance == 0

                await eventually(another_node_starts_it_from_initial, WITHIN)
                assert await a.system.ref(session, key).ask(Deposit, 2) == 2


async def _appended(node: Node, key: str, entry: int) -> bool:
    """Whether `entry` was applied, or refused because the key had moved on while this node was away."""
    try:
        return await node.system.ref(ledger, key).ask(Append, entry)
    except Unavailable:
        return False


async def _key_on(node: Node, asked_from: Node, definition: DefaultedActor[Ledger, LedgerMsg] = ledger) -> str:
    """The first of `k-0`, `k-1`, … whose owner is `node`, seen from `asked_from`."""
    for index in range(_TRIES):
        key = f"k-{index}"
        listing = await asked_from.system.ref(definition, key).ask(Entries)
        if listing.node == node.system.node:
            return key
    raise AssertionError(f"none of the first {_TRIES} keys is owned by {node.address}")


async def _session_on(node: Node, asked_from: Node) -> str:
    """The first of `s-0`, `s-1`, … whose owner for `session` is `node`, seen from `asked_from`."""
    for index in range(_TRIES):
        key = f"s-{index}"
        _, owner = await asked_from.system.ref(session, key).ask(Balance)
        if owner == node.system.node:
            return key
    raise AssertionError(f"none of the first {_TRIES} keys is owned by {node.address}")


async def _blob_on(node: Node, asked_from: Node) -> str:
    """The first of `k-0`, `k-1`, … whose owner for `blob` is `node`, seen from `asked_from`."""
    for index in range(_TRIES):
        key = f"k-{index}"
        if (await asked_from.system.ref(blob, key).ask(Digest)).node == node.system.node:
            return key
    raise AssertionError(f"none of the first {_TRIES} keys is owned by {node.address}")


def _pattern(size: int) -> bytes:
    """`size` bytes that differ from their neighbours, so that a piece out of place changes the digest."""
    return (bytes(range(256)) * (size // 256 + 1))[:size]


_TRIES = 100
