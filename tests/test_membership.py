import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import replace
from datetime import timedelta

import pytest

from casty import Event, MemberChanged, NodeId, Observer, Overlay, Refused
from tests.cluster import FAST, Harness, Node
from tests.support import eventually

WITHIN = timedelta(seconds=10)
QUIET = timedelta(milliseconds=500)
"""Ten heartbeats of `FAST`: long enough for a transition reported twice to arrive after the one it repeats."""


def describe_membership() -> None:
    def when_nodes_join_concurrently() -> None:
        async def it_converges_to_the_same_members_on_every_node() -> None:
            async with Harness.start(1, overlay=Overlay(active=3)) as harness:
                async with asyncio.TaskGroup() as joining:
                    added = [joining.create_task(harness.add()) for _ in range(7)]
                expected = {harness.nodes[0].system.node, *(task.result().system.node for task in added)}

                async def every_node_knows_the_eight_members() -> None:
                    for node in harness.nodes:
                        alive = {member.node for member in node.system.members if member.status == "alive"}
                        assert alive == expected

                await eventually(every_node_knows_the_eight_members, WITHIN)

    def when_the_seed_belongs_to_another_cluster() -> None:
        async def it_refuses_to_start() -> None:
            async with Harness.start(2) as harness:
                before = [members(node) for node in harness.nodes]

                with pytest.raises(Refused):
                    await harness.add(name="another")

                assert [members(node) for node in harness.nodes] == before

    def when_a_machine_disappears() -> None:
        async def it_marks_the_node_suspect_then_dead_then_removes_it() -> None:
            async with Harness.start(3) as harness:
                a, b, c = harness.nodes
                gone = c.system.node

                harness.isolate(c)
                await harness.crash(c)

                await eventually(seen_as(gone, "suspect", a, b), WITHIN)
                await eventually(seen_as(gone, "dead", a, b), WITHIN)
                await eventually(seen_as(gone, None, a, b), WITHIN)

    def when_a_short_isolation_heals() -> None:
        async def it_refutes_the_suspicion_and_keeps_the_node_identity() -> None:
            patient = replace(FAST, dead_after=timedelta(seconds=30))

            async with Harness.start(3, timing=patient) as harness:
                a, b, c = harness.nodes
                isolated = c.system.node

                harness.isolate(c)
                await eventually(seen_as(isolated, "suspect", a), WITHIN)
                harness.heal()

                await eventually(seen_as(isolated, "alive", a, b), WITHIN)
                assert c.system.node == isolated

    def when_the_network_partitions() -> None:
        async def it_lets_only_the_majority_remove_and_readmits_the_minority_after_healing() -> None:
            # An active view smaller than the cluster is what makes this test honest: with the default one every node
            # is everyone's neighbor, and the failure detector never has to look past its own view.
            async with Harness.start(5, overlay=Overlay(active=2)) as harness:
                a, b, c, d, e = harness.nodes
                minority = {a.system.node, b.system.node}
                majority = {node.system.node for node in (c, d, e)}

                harness.partition({a, b}, {c, d, e})

                async def the_majority_forgot_the_minority() -> None:
                    for node in (c, d, e):
                        assert members(node).keys() == majority

                async def the_minority_still_sees_the_majority_as_dead() -> None:
                    for node in (a, b):
                        assert {other: seen(node, other) for other in majority} == dict.fromkeys(majority, "dead")

                await eventually(the_majority_forgot_the_minority, WITHIN)
                await eventually(the_minority_still_sees_the_majority_as_dead, WITHIN)
                for _ in range(20):
                    # Two of five are not a majority, so no amount of time lets them remove the other three.
                    await asyncio.sleep(0.05)
                    await the_minority_still_sees_the_majority_as_dead()

                harness.heal()

                async def the_five_agree_again() -> None:
                    identities = {node.system.node for node in harness.nodes}
                    assert len(identities) == 5
                    for node in harness.nodes:
                        assert {member.node for member in node.system.members if member.status == "alive"} == identities

                await eventually(the_five_agree_again, timedelta(seconds=30))
                assert {a.system.node, b.system.node}.isdisjoint(minority)

    def when_a_node_restarts_on_the_same_address() -> None:
        async def it_replaces_the_old_incarnation_without_waiting_for_removal() -> None:
            forgiving = replace(FAST, remove_after=timedelta(hours=1))

            async with Harness.start(3, timing=forgiving) as harness:
                a, b, c = harness.nodes
                old = b.system.node

                await harness.crash(b)
                revived = await harness.add(address=b.address)

                async def the_others_replaced_the_old_incarnation() -> None:
                    for node in (a, c):
                        assert seen(node, revived.system.node) == "alive"
                        assert seen(node, old) is None

                await eventually(the_others_replaced_the_old_incarnation, WITHIN)


def describe_membership_events() -> None:
    def when_a_machine_disappears() -> None:
        async def it_reports_suspect_then_dead_once_each_on_every_survivor() -> None:
            changes: defaultdict[int, list[MemberChanged]] = defaultdict(list)

            async with Harness.start(4, observer=recording(changes)) as harness:
                a, b, c, d = harness.nodes
                gone = d.system.node

                harness.isolate(d)
                await harness.crash(d)

                async def every_survivor_saw_it_die() -> None:
                    for node in (a, b, c):
                        assert since_alive(changes[node.id], gone) == ["suspect", "dead", "left"]

                await eventually(every_survivor_saw_it_die, WITHIN)
                await asyncio.sleep(QUIET.total_seconds())
                await every_survivor_saw_it_die()

    def when_a_node_shuts_down() -> None:
        async def it_reports_leaving_then_left_once_each_on_every_other_node() -> None:
            changes: defaultdict[int, list[MemberChanged]] = defaultdict(list)

            async with Harness.start(3, observer=recording(changes)) as harness:
                a, b, c = harness.nodes
                going = c.system.node

                await harness.leave(c)

                async def the_others_saw_it_go() -> None:
                    for node in (a, b):
                        assert since_alive(changes[node.id], going) == ["leaving", "left"]

                await eventually(the_others_saw_it_go, WITHIN)
                await asyncio.sleep(QUIET.total_seconds())
                await the_others_saw_it_go()


def recording(changes: defaultdict[int, list[MemberChanged]]) -> Callable[[int], Observer]:
    """An observer for each node of a harness, keeping the member changes it reports under the id of the node."""

    def observer(source: int) -> Observer:
        def observe(event: Event, /) -> None:
            if isinstance(event, MemberChanged):
                changes[source].append(event)

        return observe

    return observer


def since_alive(changes: Sequence[MemberChanged], of: NodeId) -> list[str]:
    """The statuses `of` took after it was last alive, once every event about it is checked to follow the one before.

    An event whose `previous` is not the status before it is a transition reported twice, or one lost on the way.
    """
    statuses: list[str] = []
    for change in changes:
        if change.node == of:
            assert change.previous == (statuses[-1] if statuses else None), f"{change} after {statuses}"
            statuses.append(change.status)
    alive = max((index for index, status in enumerate(statuses) if status == "alive"), default=-1)
    return statuses[alive + 1 :]


def members(node: Node) -> dict[NodeId, str]:
    return {member.node: member.status for member in node.system.members}


def seen(node: Node, other: NodeId) -> str | None:
    return members(node).get(other)


def seen_as(other: NodeId, status: str | None, *nodes: Node) -> Callable[[], Awaitable[None]]:
    async def check() -> None:
        assert [seen(node, other) for node in nodes] == [status] * len(nodes)

    return check
