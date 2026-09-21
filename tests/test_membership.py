import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import timedelta

import pytest

from casty import NodeId, Overlay, Refused
from tests.cluster import FAST, Harness, Node
from tests.support import eventually

WITHIN = timedelta(seconds=10)


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


def members(node: Node) -> dict[NodeId, str]:
    return {member.node: member.status for member in node.system.members}


def seen(node: Node, other: NodeId) -> str | None:
    return members(node).get(other)


def seen_as(other: NodeId, status: str | None, *nodes: Node) -> Callable[[], Awaitable[None]]:
    async def check() -> None:
        assert [seen(node, other) for node in nodes] == [status] * len(nodes)

    return check
