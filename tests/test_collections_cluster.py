import asyncio
import time
from dataclasses import replace
from datetime import timedelta
from itertools import pairwise
from uuid import uuid4

import pytest

from casty import ActorDefinition, Collections, NodeId, Unavailable, replicas
from casty.collections import (
    Binding,
    ConfigurationError,
    Missing,
    Value,
    barrier,
    configured,
    entry,
    queue,
    queue_segment,
    semaphore,
    table_segment,
)
from tests.cluster import FAST, Harness, Node
from tests.support import eventually


def _lock_owner(harness: Harness, name: str) -> Node:
    actor = configured(semaphore.actor, 3, "majority")
    return _owner(harness, actor, "lock", name)


def _owner(harness: Harness, actor: ActorDefinition, kind: str, name: str) -> Node:
    owner = _holder(harness, actor, f"{kind}:{len(name)}:{name}:0")
    return next(node for node in harness.nodes if node.system.node == owner)


def _holder(harness: Harness, actor: ActorDefinition, key: str) -> NodeId:
    """The node the ring gives `key` to, which is the one a test takes away."""
    nodes = [node.system.node for node in harness.nodes]
    return replicas(actor.name, key, nodes, 3)[0]


def describe_distributed_collections() -> None:
    async def it_updates_listed_entries_without_the_index_owner() -> None:
        async with Harness.start(3, timing=replace(FAST, remove_after=None)) as harness:
            # The first segment of the only shard, which lists every key until the shard splits.
            segment = configured(table_segment.actor, 3, "majority")
            index = _holder(harness, segment, "dict:7:entries:0.0")
            index_owner = next(node for node in harness.nodes if node.system.node == index)
            definition = configured(entry.actor, 3, "majority")
            keys = Value(str, Binding(harness.nodes[0].system, "dict", "entries", replicas=3, write="majority").system)
            key = next(
                str(i)
                for i in range(1000)
                if _holder(harness, definition, f"dict:7:entries:key:{keys.dump(str(i)).hex()}")
                != index_owner.system.node
            )
            sender = next(node for node in harness.nodes if node is not index_owner)
            entries = Collections(sender.system).dict("entries", key=str, value=int, index_shards=1)
            await entries.put(key, 1)
            harness.isolate(index_owner)
            await entries.put(key, 2)
            assert await entries.get(key) == 2
            harness.heal()

            # Removing a key and putting it back change its listing, so they wait for the index to be reachable.
            async def removed_and_listed_again() -> None:
                await entries.remove(key)
                await entries.put(key, 3)
                assert await entries.items() == [(key, 3)]

            await eventually(removed_and_listed_again, timedelta(seconds=15))

    async def it_distributes_entries_and_recovers_after_an_entry_owner_dies() -> None:
        async with Harness.start(3) as harness:
            entries = Collections(await harness.client()).dict("entries", key=str, value=int, index_shards=1)
            definition = configured(entry.actor, 3, "majority")
            keys = Value(str, Binding(harness.nodes[0].system, "dict", "entries", replicas=3, write="majority").system)
            owners = {_holder(harness, definition, f"dict:7:entries:key:{keys.dump(str(i)).hex()}") for i in range(32)}
            assert len(owners) > 1
            for i in range(32):
                await entries.put(str(i), i)
            owner = next(node for node in harness.nodes if node.system.node in owners)
            harness.isolate(owner)
            await harness.crash(owner)

            async def values_survive() -> None:
                assert dict(await entries.items()) == {str(i): i for i in range(32)}

            await eventually(values_survive, timedelta(seconds=15))

    async def it_preserves_confirmed_collections_and_fencing_tokens_after_owner_death() -> None:
        async with Harness.start(3) as harness:
            client = await harness.client()
            collections = Collections(client)
            counts = collections.counter("counts", stripes=8)
            entries = collections.dict("entries", key=str, value=int, index_shards=8)
            members = collections.set("members", value=str, shards=8)
            associations = collections.multimap("associations", key=str, value=int, shards=8)
            queue = collections.queue("queue", value=int)
            register = collections.register("register", value=int)
            lock = collections.lock("critical", ttl=60)
            held = await lock.acquire()
            for i in range(24):
                await counts.add()
                await entries.put(str(i), i)
                await members.add(str(i))
                await associations.put(str(i), i)
                await queue.offer(i)
            await register.set(24)
            owner = _lock_owner(harness, "critical")
            harness.isolate(owner)
            await harness.crash(owner)

            async def all_confirmed_state_survives() -> None:
                assert await counts.get() == 24
                assert dict(await entries.items()) == {str(i): i for i in range(24)}
                assert set(await members.items()) == {str(i) for i in range(24)}
                assert await associations.size() == 24
                assert await queue.size() == 24
                assert await queue.peek() == 0
                assert await register.get() == 24
                assert await lock.locked()
                assert await lock.try_lock() is None

            await eventually(all_confirmed_state_survives, timedelta(seconds=15))
            held.release()
            successor = await lock.acquire()
            assert successor.token > held.token
            assert not await held.renew()
            successor.release()
            assert await queue.drain(30) == list(range(24))

    async def it_refuses_minority_grants_and_keeps_majority_lease_ownership() -> None:
        async with Harness.start(3, timing=replace(FAST, remove_after=None)) as harness:
            owner = _lock_owner(harness, "critical")
            first = Collections(owner.system).lock("critical", ttl=60)
            held = await first.acquire()
            others = tuple(node for node in harness.nodes if node is not owner)
            minority = Collections(owner.system).lock("critical", ttl=60)
            majority = Collections(others[0].system).lock("critical", ttl=60)
            assert await minority.locked()
            assert await majority.locked()
            harness.partition({owner}, set(others))

            with pytest.raises(Unavailable):
                await held.renew()

            async def majority_still_respects_the_lease() -> None:
                assert await majority.locked()
                assert await majority.try_lock() is None

            await eventually(majority_still_respects_the_lease, timedelta(seconds=15))
            harness.heal()

            async def release_after_healing() -> None:
                held.release()
                assert not await majority.locked()

            await eventually(release_after_healing, timedelta(seconds=15))
            successor = await majority.acquire()
            assert successor.token > held.token
            successor.release()

    async def it_coordinates_clients_and_rejects_conflicting_capacity_and_parties() -> None:
        async with Harness.start(3) as harness:
            left = Collections(await harness.client())
            right = Collections(await harness.client())
            semaphore_left = left.semaphore("capacity", capacity=1)
            semaphore_right = right.semaphore("capacity", capacity=1)
            held = await semaphore_left.acquire()
            with pytest.raises(ConfigurationError):
                await right.semaphore("capacity", capacity=2).available()
            async with asyncio.timeout(5), asyncio.TaskGroup() as group:
                pending = group.create_task(semaphore_right.acquire())
                held.release()
            pending.result().release()

            first = left.barrier("round", parties=2)
            second = right.barrier("round", parties=2)
            async with asyncio.timeout(5), asyncio.TaskGroup() as group:
                group.create_task(first.wait())
                group.create_task(second.wait())
            assert await first.waiting() == 0
            with pytest.raises(ConfigurationError):
                await right.barrier("round", parties=3).waiting()

    async def it_preserves_entries_and_their_index_when_a_member_joins() -> None:
        async with Harness.start(3) as harness:
            entries = Collections(await harness.client()).dict("entries", key=str, value=int, index_shards=16)
            for i in range(64):
                await entries.put(str(i), i)
            await harness.add()

            async def everyone_reads_every_entry() -> None:
                for node in harness.nodes:
                    assert len(node.system.members) == 4
                    view = Collections(node.system).dict("entries", key=str, value=int, index_shards=16)
                    assert dict(await view.items()) == {str(i): i for i in range(64)}

            await eventually(everyone_reads_every_entry, timedelta(seconds=15))

    async def it_expires_an_abandoned_barrier_arrival_after_owner_death() -> None:
        async with Harness.start(3) as harness:
            client = await harness.client()
            gate = Collections(client).barrier("round", parties=2)
            assert await gate.waiting() == 0
            ref = Binding(client, "barrier", "round", replicas=3, write="majority", signature=("2",)).ref(barrier.actor)

            async def abandoned() -> bool:
                try:
                    return await ref.ask(barrier.Arrive(uuid4(), 2, time.time() + 0.5))
                except Unavailable:
                    return False

            async def one_is_waiting() -> None:
                assert await gate.waiting() == 1

            async with asyncio.timeout(15), asyncio.TaskGroup() as group:
                pending = group.create_task(abandoned())
                await eventually(one_is_waiting)
                rounds = configured(barrier.actor, 3, "majority")
                owner = _owner(harness, rounds, "barrier", "round")
                harness.isolate(owner)
                await harness.crash(owner)
            assert not pending.result()

            async def no_ghost_arrival() -> None:
                assert await gate.waiting() == 0

            await eventually(no_ghost_arrival, timedelta(seconds=15))
            async with asyncio.timeout(5), asyncio.TaskGroup() as group:
                group.create_task(gate.wait())
                group.create_task(gate.wait())

    async def it_does_not_roll_back_deleted_entries_when_clear_fails() -> None:
        async with Harness.start(3, timing=replace(FAST, remove_after=None)) as harness:
            sender, isolated, _ = harness.nodes
            entries = Collections(sender.system).dict("entries", key=str, value=int, index_shards=16)
            for i in range(64):
                await entries.put(str(i), i)
            harness.isolate(isolated)
            with pytest.raises(ExceptionGroup):
                await entries.clear()
            harness.heal()

            async def successful_deletions_remain_deleted() -> None:
                remaining = dict(await entries.items())
                assert len(remaining) < 64
                assert remaining.items() <= {str(i): i for i in range(64)}.items()

            await eventually(successful_deletions_remain_deleted, timedelta(seconds=15))

    async def it_keeps_queue_order_while_the_owner_of_a_segment_crashes_under_load() -> None:
        async with Harness.start(3) as harness:
            client = await harness.client()
            jobs = Collections(client).queue("jobs", value=int)
            index = Binding(client, "queue", "jobs", replicas=3, write="majority").ref(queue.actor)
            acked: list[int] = []
            polled: list[int] = []
            failed_polls = 0

            async def produce() -> None:
                for i in range(3_000):
                    # A failed offer may have committed, so offering it again can repeat it but never reorder it.
                    while True:
                        try:
                            await jobs.offer(i)
                            break
                        except (TimeoutError, Unavailable):
                            await asyncio.sleep(0.05)
                    acked.append(i)

            async def consume(producer: asyncio.Task[None]) -> None:
                nonlocal failed_polls
                while True:
                    finished = producer.done()
                    try:
                        item = await jobs.poll()
                    except (TimeoutError, Unavailable):
                        failed_polls += 1
                        await asyncio.sleep(0.05)
                        continue
                    if not isinstance(item, Missing):
                        polled.append(item)
                    elif finished:
                        return
                    else:
                        await asyncio.sleep(0.01)

            async def backlog_spans_segments() -> None:
                assert len(acked) >= 1_500

            async with asyncio.timeout(120), asyncio.TaskGroup() as group:
                producer = group.create_task(produce())
                await eventually(backlog_spans_segments, timedelta(seconds=60))
                group.create_task(consume(producer))
                _, tail = await index.ask(queue.Advance(0, 0))
                segments = configured(queue_segment.actor, 3, "majority")
                owner = _holder(harness, segments, f"queue:4:jobs:{tail}")
                victim = next(node for node in harness.nodes if node.system.node == owner)
                harness.isolate(victim)
                await harness.crash(victim)

            assert all(earlier <= later for earlier, later in pairwise(polled))
            # A failed poll may have taken an item it never answered with; nothing else may be missing.
            assert len(set(acked) - set(polled)) <= failed_polls
