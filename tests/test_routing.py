import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import timedelta

import pytest

from casty import Context, NodeId, Unavailable, UnknownActor, actor
from tests.app import Deposit, Entries, Hold, Locate, Note, Notes, Where, account, gate, ledger, notes
from tests.cluster import FAST, Harness, Node
from tests.support import eventually

WITHIN = timedelta(seconds=10)


def describe_routing() -> None:
    def when_keys_are_used_from_every_node() -> None:
        async def it_reaches_one_activation_per_key_spread_over_all_nodes() -> None:
            keys = [f"acc-{index}" for index in range(_KEYS)]

            async with Harness.start(3) as harness:
                async with asyncio.TaskGroup() as depositing:
                    for node in harness.nodes:
                        for key in keys:
                            depositing.create_task(node.system.ref(account, key).ask(Deposit, 1))

                first = harness.nodes[0].system
                located = [await first.ref(account, key).ask(Where) for key in keys]

                # The state of a key lives only where it is active, so a second activation would answer its own sum.
                assert [where.balance for where in located] == [3] * _KEYS
                assert {where.node for where in located} == {node.system.node for node in harness.nodes}

    def when_a_node_sends_many_messages_to_a_remote_key() -> None:
        async def it_delivers_them_in_order() -> None:
            async with Harness.start(2) as harness:
                a, b = harness.nodes

                async def answered_from(key: str) -> NodeId:
                    return (await a.system.ref(notes, key).ask(Notes)).node

                key = await _key_on(b, answered_from)
                for value in range(_MANY):
                    a.system.ref(notes, key).tell(Note(value))

                written = await a.system.ref(notes, key).ask(Notes)

                assert written.node == b.system.node
                assert written.notes == tuple(range(_MANY))

    def when_the_owner_crashes_with_an_ask_in_flight() -> None:
        async def it_fails_the_ask_with_unavailable_before_the_ask_timeout() -> None:
            patient = replace(FAST, ask_timeout=timedelta(minutes=1))

            async with Harness.start(3, timing=patient) as harness:
                a, b, _ = harness.nodes

                async def answered_from(key: str) -> NodeId:
                    return await a.system.ref(gate, key).ask(Locate)

                key = await _key_on(b, answered_from)
                waiting = asyncio.ensure_future(a.system.ref(gate, key).ask(Hold))
                harness.isolate(b)
                await harness.crash(b)

                with pytest.raises(Unavailable):
                    # Far below `ask_timeout`: an `ask` that ends on its deadline, and not on the member table,
                    # never reaches the assertion.
                    async with asyncio.timeout(WITHIN.total_seconds()):
                        await waiting

    def when_a_type_is_used_for_the_first_time() -> None:
        async def it_runs_on_nodes_that_never_touched_it_and_every_member_learns_of_it() -> None:
            async with Harness.start(3) as harness:
                a = harness.nodes[0]
                # Only `a` ever names the type. The others meet it by its name, and import it from there.
                ledgers = [await a.system.ref(ledger, f"l-{index}").ask(Entries) for index in range(_SPREAD)]
                assert {listing.node for listing in ledgers} == {node.system.node for node in harness.nodes}

                async def every_member_has_the_type() -> None:
                    for node in harness.nodes:
                        assert all(ledger.name in member.types for member in node.system.members)

                await eventually(every_member_has_the_type, WITHIN)

    def when_the_owner_of_a_key_does_not_have_the_type() -> None:
        async def it_raises_unknown_actor_and_still_serves_the_keys_it_owns() -> None:
            # Defined in here, so no import finds it: to every other node it is a type of a version they do not run.
            @actor(initial=0)
            async def unreleased(ctx: Context[int, Locate]) -> None:
                async for msg in ctx.inbox:
                    msg.reply_to.tell(ctx.system.node)

            async with Harness.start(3) as harness:
                a = harness.nodes[0].system
                answers = await asyncio.gather(
                    *(a.ref(unreleased, f"k-{index}").ask(Locate) for index in range(_SPREAD)), return_exceptions=True
                )
                assert {answer for answer in answers if isinstance(answer, NodeId)} == {a.node}
                assert any(isinstance(answer, UnknownActor) for answer in answers)
                assert all(isinstance(answer, NodeId | UnknownActor) for answer in answers)


async def _key_on(node: Node, answered_from: Callable[[str], Awaitable[NodeId]]) -> str:
    """The first of `k-0`, `k-1`, … that `answered_from` places on `node`."""
    for index in range(_TRIES):
        key = f"k-{index}"
        if await answered_from(key) == node.system.node:
            return key
    raise AssertionError(f"none of the first {_TRIES} keys is placed on {node.address}")


_KEYS = 60
_SPREAD = 30
_MANY = 1_000
_TRIES = 100
