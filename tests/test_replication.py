from dataclasses import replace
from datetime import timedelta

import pytest

from casty import DefaultedActor, Unavailable
from tests.app import Append, Entries, Ledger, LedgerMsg, Pay, Pending, ledger, loose, order
from tests.cluster import FAST, Harness, Node
from tests.support import eventually

WITHIN = timedelta(seconds=10)
STUBBORN = replace(FAST, remove_after=None)


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

                async def the_majority_appends() -> None:
                    assert await b.system.ref(ledger, key).ask(Append, 6)

                await eventually(the_majority_appends, WITHIN)
                harness.heal()

                async def every_node_answers_with_the_majority_state() -> None:
                    for node in (a, b, c):
                        assert (await node.system.ref(ledger, key).ask(Entries)).entries == tuple(range(1, 7))

                await eventually(every_node_answers_with_the_majority_state, WITHIN)

    def when_the_old_owner_receives_a_message_after_healing() -> None:
        async def it_is_fenced_and_the_message_applies_to_the_current_state() -> None:
            patient = replace(STUBBORN, idle_after=timedelta(minutes=5))

            async with Harness.start(3, timing=patient) as harness:
                a, b, c = harness.nodes
                key = await _key_on(a, a)
                for entry in range(1, 6):
                    assert await a.system.ref(ledger, key).ask(Append, entry)

                harness.partition({a}, {b, c})
                tried: list[int] = []
                confirmed: list[int] = []

                async def the_majority_appends() -> None:
                    # A new id per attempt: a refused `ask` may still have been applied, so reusing one would show up
                    # as a repetition that no rule was broken to produce.
                    entry = 10 + len(tried)
                    tried.append(entry)
                    assert await b.system.ref(ledger, key).ask(Append, entry)
                    confirmed.append(entry)

                await eventually(the_majority_appends, WITHIN)
                harness.heal()

                # The activation of `a` is the one from before the partition, and its epoch is behind. The first write
                # that reaches it is refused: this one, or an attempt the blocked proxy held and delivers on the
                # healing. Which of the two it is depends on the order the held bytes arrive in; both are a fence.
                seven = await _appended(a, key, 7)
                assert await a.system.ref(ledger, key).ask(Append, 8)
                kept: set[int] = {*range(1, 6), *confirmed, 8, *([7] if seven else [])}
                possible = {*range(1, 6), *tried, 7, 8}

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


_TRIES = 100
