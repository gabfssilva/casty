import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import timedelta

import pytest

from casty import ActorSystem, Context, NodeId, Unavailable, UnknownActor, actor
from tests.app import Deposit, Entries, Gate, Hold, Locate, Note, Notes, Where, account, gate, ledger, notes
from tests.cluster import FAST, WITHIN, Harness, Node
from tests.support import eventually

_STARTED: list[tuple[str, NodeId]] = []
"""Every activation of `station`, as `(key, node)`, on whichever node of this process it happened."""


@actor(pinned=True, initial=Gate())
async def station(ctx: Context[Gate, Locate]) -> None:
    """One per node, reached by the node: says where it runs, and notes every start."""
    _STARTED.append((ctx.key, ctx.system.node))
    async for msg in ctx.inbox:
        msg.reply_to.tell(ctx.system.node)


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

    def when_a_type_is_pinned() -> None:
        async def it_keeps_one_copy_and_refuses_more() -> None:
            assert station.pinned
            assert station.replicas == 1
            assert not account.pinned
            assert account.replicas == 3

            with pytest.raises(ValueError, match="pinned"):

                @actor(pinned=True, replicas=3, initial=Gate())
                async def spread(ctx: Context[Gate, Locate]) -> None:
                    async for msg in ctx.inbox:
                        msg.reply_to.tell(ctx.system.node)

        async def it_takes_the_node_with_at_and_no_other_type_does() -> None:
            async with ActorSystem() as system:
                with pytest.raises(TypeError, match="at="):
                    system.ref(station, "worker")
                with pytest.raises(TypeError, match="at="):
                    system.ref(account, "acc-1", at="127.0.0.1:7400")
                # Placement reads the key alone, so a key of the ring never looks like a pinned one.
                with pytest.raises(ValueError, match="@host:port"):
                    system.ref(account, "@127.0.0.1:7400/acc-1")
                with pytest.raises(ValueError, match="host:port"):
                    system.ref(station, "worker", at="nowhere")
                with pytest.raises(ValueError, match="no address"):
                    system.ref(station, "worker", at=system.node)

        async def it_runs_each_key_on_the_node_it_names_and_nowhere_else() -> None:
            _STARTED.clear()
            async with Harness.start(5) as harness:
                for target in harness.nodes:
                    member = next(
                        member for member in harness.nodes[0].system.members if member.node == target.system.node
                    )
                    # The node named in each of the ways a caller holds it, from every node.
                    for at in (target.address, target.system.node, member):
                        for node in harness.nodes:
                            assert await node.system.ref(station, "worker", at=at).ask(Locate) == target.system.node

                pinned = {f"@{node.address}/worker": node.system.node for node in harness.nodes}
                assert Counter(key for key, _ in _STARTED) == dict.fromkeys(pinned, 1)
                assert dict(_STARTED) == pinned

        async def it_stays_on_its_node_while_nodes_join_and_leave() -> None:
            _STARTED.clear()
            async with Harness.start(3) as harness:
                asking, going, third = harness.nodes
                for node in harness.nodes:
                    assert await asking.system.ref(station, "worker", at=node.address).ask(Locate) == node.system.node

                joined = await asyncio.gather(harness.add(), harness.add())
                pinned = {f"@{node.address}/worker": node.system.node for node in (asking, going, third, *joined)}
                await harness.leave(going)
                await _sees_only(asking, harness.nodes)

                for node in harness.nodes:
                    assert await asking.system.ref(station, "worker", at=node.address).ask(Locate) == node.system.node
                # The key of the node that left is taken by no other.
                with pytest.raises(Unavailable):
                    await asking.system.ref(station, "worker", at=going.address).ask(Locate)

                assert Counter(key for key, _ in _STARTED) == dict.fromkeys(pinned, 1)
                assert dict(_STARTED) == pinned

        async def it_is_reached_from_a_client_by_the_address_the_node_advertises() -> None:
            async with Harness.start(3) as harness:
                client = await harness.client()
                for node in harness.nodes:
                    assert await client.ref(station, "worker", at=node.address).ask(Locate) == node.system.node

        async def it_follows_its_node_through_a_restart_on_the_same_address() -> None:
            async with Harness.start(3) as harness:
                asking, restarting, _ = harness.nodes
                old = restarting.system.node
                ref = asking.system.ref(station, "worker", at=restarting.address)
                assert await ref.ask(Locate) == old

                await harness.crash(restarting)
                revived = await harness.add(address=restarting.address)

                async def replaced() -> None:
                    seen = {member.node: member.status for member in asking.system.members}
                    assert seen.get(revived.system.node) == "alive"
                    assert seen.get(old) != "alive"

                await eventually(replaced, WITHIN)
                # The same ref: it names the address, which outlives the incarnation that was running there.
                assert await ref.ask(Locate) == revived.system.node


async def _sees_only(node: Node, nodes: tuple[Node, ...]) -> None:
    """Wait until `node` counts exactly `nodes` as alive."""

    async def only_them() -> None:
        alive = {member.node for member in node.system.members if member.status == "alive"}
        assert alive == {other.system.node for other in nodes}

    await eventually(only_them, WITHIN)


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
