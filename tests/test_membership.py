import asyncio
import socket
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AsyncExitStack
from dataclasses import replace
from datetime import timedelta

import pytest

from casty import (
    ActorSystem,
    Cluster,
    Event,
    Limits,
    MemberChanged,
    NodeId,
    Observer,
    Overlay,
    Refused,
)
from tests.app import Fetch, Load, sink, source
from tests.cluster import FAST, WITHIN, Harness, Node
from tests.support import eventually

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

    def when_the_seed_that_starts_the_cluster_comes_up_after_the_nodes_that_join_through_it() -> None:
        async def it_forms_one_cluster_and_not_one_each() -> None:
            addresses = [_unused() for _ in range(4)]
            opener, joiners = addresses[0], addresses[1:]

            def system(address: str, seeds: tuple[str, ...]) -> ActorSystem:
                return ActorSystem(
                    cluster=Cluster(
                        bind=address,
                        seeds=seeds,
                        heartbeat=FAST.heartbeat,
                        suspect_after=FAST.suspect_after,
                        dead_after=FAST.dead_after,
                        anti_entropy=FAST.anti_entropy,
                    )
                )

            async def alive(systems: Sequence[ActorSystem], count: int) -> None:
                for each in systems:
                    assert sum(member.status == "alive" for member in each.members) == count

            async with AsyncExitStack() as stack:
                # Every node but the first names all the others as seeds, the first among them, and the first none.
                started = [
                    system(address, tuple(other for other in addresses if other != address)) for address in joiners
                ]
                await asyncio.gather(*(stack.enter_async_context(each) for each in started))
                await eventually(lambda: alive(started, 3), WITHIN)

                started.append(await stack.enter_async_context(system(opener, ())))

                await eventually(lambda: alive(started, 4), WITHIN)

    def when_large_messages_fill_a_slow_link() -> None:
        async def it_keeps_every_member_alive_while_they_cross() -> None:
            changes: defaultdict[int, list[MemberChanged]] = defaultdict(list)

            async with Harness.start(3, limits=Limits(message=4 * 2**20), observer=recording(changes)) as harness:
                # About 3 MB/s each way: each node queues seconds of messages to the others, ten times `suspect_after`.
                harness.slow(timedelta(milliseconds=20))
                payload = bytes(2 * 2**20)
                sizes = await asyncio.gather(
                    *(
                        node.system.ref(sink, f"{node.id}-{index}").ask(Load(payload))
                        for node in harness.nodes
                        for index in range(4)
                    )
                )

                assert sizes == [len(payload)] * 12
                assert [change for seen in changes.values() for change in seen if change.status != "alive"] == []

    def when_the_link_toward_a_node_slows_to_a_crawl_under_a_large_answer() -> None:
        async def it_keeps_hearing_the_peer_whose_answer_is_still_arriving() -> None:
            changes: defaultdict[int, list[MemberChanged]] = defaultdict(list)

            async with Harness.start(2, observer=recording(changes)) as harness:
                a, b = harness.nodes
                fetching = b.system.ref(source, "s", at=a.system.node)
                assert len(await fetching.ask(Fetch(1024))) == 1024

                # The answer and whatever queues behind it take seconds to arrive, many times `suspect_after`, and
                # the heartbeats of `a` wait behind them.
                harness.throttle(b, 64 * 1024)
                fetched = await fetching.ask(Fetch(192 * 1024))

                assert len(fetched) == 192 * 1024
                assert [change for seen in changes.values() for change in seen if change.status != "alive"] == []

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

    def when_a_process_dies_and_its_address_refuses_the_connection() -> None:
        async def it_suspects_it_at_the_first_failed_dial_without_waiting_for_silence() -> None:
            # An hour of silence before a suspicion: only the refused dial can start one.
            deaf = replace(FAST, suspect_after=timedelta(hours=1))

            async with Harness.start(3, timing=deaf) as harness:
                a, b, c = harness.nodes
                gone = c.system.node

                await harness.crash(c)

                await eventually(seen_as(gone, "dead", a, b), WITHIN)

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


def _unused() -> str:
    """An address on loopback nothing listens on yet."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{probe.getsockname()[1]}"
