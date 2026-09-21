import asyncio
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from itertools import pairwise

from tests.app import FEED, consumer
from tests.cluster import FAST, Harness, Node
from tests.support import eventually
from tests.traffic import Traffic

WITHIN = timedelta(seconds=15)
LAST = 99
"""Offset the consumers of the third test read up to."""
KEPT = replace(FAST, remove_after=None)
"""A machine that died is buried but never removed, so the ring holds and a key changes hands once."""


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


async def _everyone_sees(harness: Harness, count: int, /) -> None:
    async def every_node_sees_the_others() -> None:
        for node in harness.nodes:
            alive = {member.node for member in node.system.members if member.status == "alive"}
            assert alive == {other.system.node for other in harness.nodes}, f"{node.address} sees {len(alive)}"
        assert len(harness.nodes) == count

    await eventually(every_node_sees_the_others, WITHIN)
