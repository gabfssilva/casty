import asyncio
import gc
import os
import subprocess
import sys
import time
import weakref
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from casty import ActorSystem, Collections, Context, NotStarted, Ref, Unavailable, actor
from casty import collections as kinds
from casty.collections import (
    MISSING,
    Acquired,
    Binding,
    ConfigurationError,
    Counter,
    Denied,
    Queue,
    SemaphoreState,
    Status,
    _after,  # pyright: ignore[reportPrivateUsage]
    _place,  # pyright: ignore[reportPrivateUsage]
    barrier,
    configured,
    counter,
    queue_segment,
    register,
    semaphore,
)
from tests.cluster import Harness
from tests.support import eventually


@dataclass(frozen=True)
class User:
    name: str
    nickname: str | None = None


@dataclass(frozen=True)
class Index:
    values: Mapping[str, int]
    labels: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Take:
    """Ask the pool for a permit under the key's name, answered to the key itself."""

    wait: float | None


@dataclass(frozen=True)
class Heard:
    reply_to: Ref[tuple[str, ...]]


@actor(initial=0)
async def worker(ctx: Context[int, Take | Heard | Acquired | Denied]) -> None:
    pool = ctx.system.ref(semaphore.actor, "pool", initial=SemaphoreState(capacity=1))
    heard: list[str] = []
    async for msg in ctx.inbox:
        match msg:
            case Take(wait):
                pool.tell(semaphore.Acquire(ctx.self, wait=wait, lease_id=ctx.key))
            case Acquired(lease_id, _):
                heard.append(f"acquired {lease_id}")
            case Denied(lease_id):
                heard.append(f"denied {lease_id}")
            case Heard(reply_to):
                reply_to.tell(tuple(heard))


def describe_counter_actor() -> None:
    async def it_serializes_additions_and_resets_the_named_counter() -> None:
        async with ActorSystem() as system:
            ref = system.ref(counter.actor, "requests")
            assert await ref.ask(counter.Get) == 0
            async with asyncio.TaskGroup() as group:
                for _ in range(100):
                    group.create_task(ref.ask(counter.Add, 1))

            assert await ref.ask(counter.Get) == 100
            await ref.ask(counter.Add, -7)
            assert await ref.ask(counter.Get) == 93
            assert await system.ref(counter.actor, "other").ask(counter.Get) == 0
            await ref.ask(counter.Reset)
            assert await ref.ask(counter.Get) == 0


def describe_semaphore_actor() -> None:
    async def it_tells_an_actor_its_grant_while_the_actor_goes_on_reading() -> None:
        async with ActorSystem() as system:
            pool = system.ref(semaphore.actor, "pool", initial=SemaphoreState(capacity=1))
            held = await pool.ask(semaphore.Acquire)
            assert isinstance(held, Acquired)
            first = system.ref(worker, "first")
            first.tell(Take(None))
            assert await first.ask(Heard) == ()
            second = system.ref(worker, "second")
            second.tell(Take(0.0))

            async def second_is_denied() -> None:
                assert await second.ask(Heard) == ("denied second",)

            await eventually(second_is_denied)
            assert await pool.ask(semaphore.Get) == Status(capacity=1, available=0, waiting=1)
            pool.tell(semaphore.Release(held.lease_id))

            async def first_acquires() -> None:
                assert await first.ask(Heard) == ("acquired first",)

            await eventually(first_acquires)
            assert await pool.ask(semaphore.Get) == Status(capacity=1, available=0, waiting=0)

    async def it_names_a_lease_nobody_named_and_answers_a_request_sent_again_with_its_grant() -> None:
        async with ActorSystem() as system:
            pool = system.ref(semaphore.actor, "pool", initial=SemaphoreState(capacity=2))
            named = await pool.ask(semaphore.Acquire)
            chosen = await pool.ask(semaphore.Acquire, lease_id="mine")
            assert isinstance(named, Acquired)
            assert chosen == Acquired("mine", named.token + 1)
            assert named.lease_id != "mine"
            assert await pool.ask(semaphore.Acquire, lease_id="mine") == chosen
            denied = await pool.ask(semaphore.Acquire, wait=0)
            assert isinstance(denied, Denied)
            assert denied.lease_id not in {named.lease_id, "mine"}

    async def it_starts_from_the_capacity_its_first_ref_gave_it() -> None:
        async with ActorSystem() as system:
            system.ref(semaphore.actor, "pool", initial=SemaphoreState(capacity=1))
            again = system.ref(semaphore.actor, "pool", initial=SemaphoreState(capacity=5))
            assert await again.ask(semaphore.Get) == Status(capacity=1, available=1, waiting=0)


def describe_register_actor() -> None:
    async def it_allows_only_one_competing_compare_and_set() -> None:
        async with ActorSystem() as system:
            ref = system.ref(register.actor, "leader")
            assert await ref.ask(register.Get) is None
            async with asyncio.TaskGroup() as group:
                attempts = [
                    group.create_task(ref.ask(register.CompareAndSet, None, str(i).encode())) for i in range(20)
                ]

            assert sum(attempt.result() for attempt in attempts) == 1
            winner = await ref.ask(register.Get)
            assert winner is not None
            assert await ref.ask(register.GetAndSet, b"next") == winner
            assert await ref.ask(register.Get) == b"next"
            assert not await ref.ask(register.CompareAndSet, winner, b"stale")
            await ref.ask(register.Put, b"")
            assert await ref.ask(register.Get) == b""
            assert await system.ref(register.actor, "other").ask(register.Get) is None


def describe_collections() -> None:
    def it_resolves_types_and_encodes_unordered_values_identically_in_fresh_processes() -> None:
        program = """
from collections.abc import Mapping
from casty import ActorSystem

system = ActorSystem()
definition = system._resolve('casty.collections:counter_5_all')
assert definition is not None and definition.replicas == 5 and definition.write == 'all'
schema = system._schema(Mapping[str, frozenset[str]])
values = {key: frozenset({'left', 'right'}) for key in frozenset({'a', 'b', 'c'})}
print(system._encode(schema, values).hex())
"""
        outputs = [
            subprocess.run(
                [sys.executable, "-c", program],
                env={**os.environ, "PYTHONHASHSEED": seed},
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            for seed in ("1", "2")
        ]
        assert outputs[0] == outputs[1]

    async def it_exposes_named_counters_and_typed_registers() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)
            counter = collections.counter("visits", stripes=4)
            async with asyncio.TaskGroup() as group:
                for _ in range(100):
                    group.create_task(counter.add())
            assert await counter.get() == 100
            assert await Collections(system).counter("visits", stripes=4).get() == 100
            await counter.reset()
            assert await counter.get() == 0

            register = collections.register("user", value=User)
            assert await register.get() == MISSING
            assert await register.compare_and_set(MISSING, User("one"))
            assert not await register.compare_and_set(MISSING, User("two"))
            assert await register.get_and_set(User("three")) == User("one")
            await register.set(User("four"))
            assert await register.get() == User("four")

    async def it_rejects_incompatible_configuration_for_the_same_name() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)
            await collections.counter("visits", stripes=2).add()
            with pytest.raises(ConfigurationError):
                await collections.counter("visits", stripes=3).get()
            with pytest.raises(ConfigurationError):
                await collections.counter("visits", stripes=2, replicas=5).get()
            await collections.register("user", value=User).set(User("one"))
            with pytest.raises(ConfigurationError):
                await collections.register("user", value=str).get()

    async def it_answers_the_same_facade_for_equal_arguments() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)
            entries = collections.dict("x", key=str, value=bytes)
            assert collections.dict("x", key=str, value=bytes) is entries
            assert collections.dict(name="x", value=bytes, key=str, replicas=3) is entries
            assert collections.lock("resource") is collections.lock("resource", ttl=30.0)
            # Facades are kept per `Collections`, and each of those is over one system.
            assert Collections(system).dict("x", key=str, value=bytes) is not entries

    async def it_checks_the_configuration_with_the_cluster_once_for_many_operations(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        compared = 0

        def compare_and_set(reply_to: Ref[bool], expected: bytes | None, value: bytes) -> register.CompareAndSet:
            nonlocal compared
            compared += 1
            return register.CompareAndSet(reply_to, expected, value)

        # Every binding asks its metadata register through `register`, which now counts the compare-and-sets.
        monkeypatch.setattr(
            kinds, "register", SimpleNamespace(actor=register.actor, Get=register.Get, CompareAndSet=compare_and_set)
        )
        async with ActorSystem() as system:
            collections = Collections(system)
            entries = collections.dict("x", key=str, value=bytes)
            for i in range(20):
                await entries.put(str(i), b"")
                assert await collections.dict("x", key=str, value=bytes).get(str(i)) == b""
            assert compared == 1
            await Collections(system).dict("x", key=str, value=bytes).get("0")
            assert compared == 2
            # Let go of, the facade goes with its binding, and the one built in its place checks again.
            del entries
            gc.collect()
            await collections.dict("x", key=str, value=bytes).get("0")
            assert compared == 3

    async def it_lets_go_of_the_facades_nobody_holds() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)

            async def touched(name: str) -> weakref.ref[Counter]:
                counter = collections.counter(name)
                await counter.add()
                return weakref.ref(counter)

            held = collections.counter("entity-0")
            await held.add()
            # A collection per entity name: what the caller let go of is not kept for it.
            gone = [await touched(f"entity-{i}") for i in range(1, 100)]
            gc.collect()
            assert [facade for facade in gone if facade() is not None] == []
            assert collections.counter("entity-0") is held
            assert await collections.counter("entity-1").get() == 1

    async def it_rejects_other_replicas_for_a_name_with_or_without_asking_the_cluster() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)
            entries = collections.dict("x", key=str, value=bytes)
            await entries.put("one", b"1")
            # The factory raises while the facade whose binding confirmed the settings is held: nothing was asked.
            with pytest.raises(ConfigurationError):
                collections.dict("x", key=str, value=bytes, replicas=5)
            with pytest.raises(ConfigurationError):
                await Collections(system).dict("x", key=str, value=bytes, replicas=5).get("one")
            first = collections.counter("y", replicas=3)
            second = collections.counter("y", replicas=5)
            await first.add()
            with pytest.raises(ConfigurationError):
                await second.add()
            assert await collections.dict("x", key=str, value=bytes).get("one") == b"1"

    async def it_resolves_configured_actor_types_without_a_local_facade() -> None:
        async with ActorSystem() as system:
            counter = Collections(system).counter("visits", replicas=5, write="all")
            await counter.add()
            actor = kinds.configured(kinds.counter.actor, 5, "all")
            # A node that never touched the type finds it by its name, with the configuration the name carries.
            resolved = ActorSystem()._resolve(actor.name)  # pyright: ignore[reportPrivateUsage]
            assert resolved is not None
            assert (resolved.name, resolved.replicas, resolved.write) == (actor.name, 5, "all")

    async def it_indexes_dictionary_entries_and_preserves_typed_values() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)
            users = collections.dict("users", key=str, value=User, index_shards=4)
            assert await users.get("absent") == MISSING
            assert not await users.remove("absent")
            for i in range(20):
                await users.put(str(i), User(str(i)))
            assert await users.size() == 20
            assert dict(await users.items()) == {str(i): User(str(i)) for i in range(20)}
            await users.put("1", User("updated"))
            assert await users.get("1") == User("updated")
            assert await users.contains("1")
            assert await users.remove("1")
            assert not await users.contains("1")
            await users.clear()
            assert await users.items() == []

            nullable = collections.dict("nulls", key=str, value=type(None))
            await nullable.put("present", None)
            assert await nullable.get("present") is None
            assert await nullable.get("absent") == MISSING

    async def it_keeps_sets_unique_and_multimaps_unique_per_key() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)
            left = collections.set("left", value=str, shards=3)
            right = collections.set("right", value=str, shards=3)
            assert await left.add("a")
            assert not await left.add("a")
            assert await left.add("b")
            assert await right.add("b")
            assert await right.add("c")
            assert await left.contains("a")
            assert await left.size() == 2
            assert await left.union(right) == {"a", "b", "c"}
            assert await left.intersection(right) == {"b"}
            assert await left.difference(right) == {"a"}
            assert await left.remove("a")
            assert not await left.remove("a")
            await left.clear()
            assert await left.items() == []

            multi = collections.multimap("left", key=str, value=str, shards=3)
            assert await multi.put("a", "one")
            assert not await multi.put("a", "one")
            assert await multi.put("a", "two")
            assert await multi.put("b", "one")
            assert set(await multi.get("a")) == {"one", "two"}
            assert await multi.size() == 3
            assert await multi.contains("a", "two")
            assert await multi.remove("a", "one")
            assert not await multi.remove("a", "one")
            assert await multi.remove_key("a") == 1
            assert await multi.remove_key("a") == 0
            await multi.clear()
            assert await multi.size() == 0
            assert set(await right.items()) == {"b", "c"}

    async def it_finds_set_members_through_splits_a_lagging_facade_has_not_seen() -> None:
        async with ActorSystem() as system:
            first = Collections(system).set("members", value=int, shards=1)
            late = Collections(system).set("members", value=int, shards=1)
            assert await late.add(-1)
            for i in range(3_000):
                assert await first.add(i)
            # `late` still counts one segment: those that split since refuse its keys and send it to the directory.
            assert await late.contains(2_999)
            assert not await late.add(1_500)
            assert await late.remove(7)
            assert not await first.contains(7)
            assert await late.size() == 3_000
            assert sorted(await late.items()) == [-1, *range(7), *range(8, 3_000)]

    async def it_counts_a_set_growing_through_splits_without_losing_or_repeating_a_member() -> None:
        async with ActorSystem() as system:
            members = Collections(system).set("growing", value=int, shards=1)
            added = 0

            async def grow() -> None:
                nonlocal added
                for i in range(6_000):
                    await members.add(i)
                    added += 1

            async with asyncio.TaskGroup() as group:
                growing = group.create_task(grow())
                while not growing.done():
                    before = added
                    size = await members.size()
                    # The one addition in flight may be counted before it returns.
                    assert before <= size <= added + 1
            assert await members.size() == 6_000
            assert sorted(await members.items()) == list(range(6_000))

    async def it_keeps_the_values_of_a_multimap_key_together_through_splits() -> None:
        async with ActorSystem() as system:
            multi = Collections(system).multimap("grouped", key=int, value=str, shards=1)
            for i in range(1_500):
                assert await multi.put(i, "a")
                assert await multi.put(i, "b")
            assert await multi.size() == 3_000
            assert sorted(await multi.get(1_234)) == ["a", "b"]
            assert await multi.remove_key(1_234) == 2
            assert await multi.contains(99, "b")
            assert await multi.size() == 2_998
            scanned = [pair async for pair in multi.scan()]
            assert sorted(scanned) == [(i, value) for i in range(1_500) if i != 1_234 for value in ("a", "b")]

    def it_visits_every_segment_of_a_shard_once_from_the_cursor_of_a_scan() -> None:
        for count in range(1, 300):
            level = 1 << (count.bit_length() - 1)
            visited: list[int] = []
            position: int | None = 0
            while position is not None:
                at = _place(position, count)
                visited.append(at)
                # The segments this level split and the ones they split into tell twice as many hashes apart.
                position = _after(position, 2 * level if at < count - level or at >= level else level)
            assert sorted(visited) == list(range(count))

    async def it_scans_a_set_page_by_page_through_splits_made_while_it_runs() -> None:
        async with ActorSystem() as system:
            writer = Collections(system).set("moving", value=int, shards=1)
            for i in range(600):
                await writer.add(i)
            # A facade that has not seen the split: its segments refuse the positions they no longer hold.
            reader = Collections(system).set("moving", value=int, shards=1)
            seen: list[int] = []
            async for member in reader.scan():
                if not seen:
                    # The first segment was read: splits now move members out of it and out of those still to come.
                    for i in range(600, 6_000):
                        await writer.add(i)
                seen.append(member)
            assert len(seen) == len(set(seen))
            assert set(range(600)) <= set(seen) <= set(range(6_000))
            assert len(seen) > 600

    async def it_ends_a_scan_of_a_set_written_meanwhile_without_repeating_a_member() -> None:
        async with ActorSystem() as system:
            members = Collections(system).set("written", value=int, shards=2)
            for i in range(1_000):
                await members.add(i)

            async def write() -> None:
                for i in range(1_000, 8_000):
                    await members.add(i)

            async with asyncio.timeout(60), asyncio.TaskGroup() as group:
                group.create_task(write())
                seen = [member async for member in members.scan()]
            assert len(seen) == len(set(seen))
            assert set(range(1_000)) <= set(seen) <= set(range(8_000))

    async def it_builds_the_set_algebra_from_scans_over_many_segments() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)
            left = collections.set("left", value=int, shards=1)
            right = collections.set("right", value=int, shards=1)
            for i in range(2_000):
                await left.add(i)
                await right.add(i + 1_000)
            assert await left.union(right) == set(range(3_000))
            assert await left.intersection(right) == set(range(1_000, 2_000))
            assert await left.difference(right) == set(range(1_000))
            assert await right.difference(left) == set(range(2_000, 3_000))

    async def it_writes_as_much_per_member_added_to_a_large_set_as_to_a_small_one() -> None:
        async with ActorSystem() as system:
            writes = system._writes()  # pyright: ignore[reportPrivateUsage]
            members = Collections(system).set("flat", value=int, shards=1)

            async def written(values: range) -> int:
                total = 0
                for value in values:
                    await members.add(value)
                    total += sum(map(len, writes.payloads))
                    writes.payloads.clear()
                return total

            small = await written(range(2_000))
            await written(range(2_000, 30_000))
            # With the whole shard in one page, these would write about fifteen times what the first did.
            assert await written(range(30_000, 32_000)) < 2 * small

    async def it_delivers_queue_items_once_in_fifo_order_without_failures() -> None:
        async with ActorSystem() as system:
            queue = Collections(system).queue("jobs", value=int)
            assert await queue.poll() == MISSING
            for i in range(30):
                await queue.offer(i)
            assert await queue.peek() == 0
            assert await queue.size() == 30
            assert await queue.drain(3) == [0, 1, 2]
            async with asyncio.TaskGroup() as group:
                consumers = [group.create_task(queue.poll()) for _ in range(27)]
            assert {task.result() for task in consumers} == set(range(3, 30))
            assert await queue.peek() == MISSING
            await queue.offer(100)
            await queue.clear()
            assert await queue.size() == 0
            with pytest.raises(ValueError):
                await queue.drain(-1)

    async def it_keeps_fifo_order_over_100k_items_writing_at_most_a_segment_per_operation() -> None:
        async with ActorSystem() as system:
            writes = system._writes()  # pyright: ignore[reportPrivateUsage]
            queue = Collections(system).queue("jobs", value=int)
            largest = 0
            for i in range(100_000):
                await queue.offer(i)
                largest = max([largest, *map(len, writes.payloads)])
                writes.payloads.clear()
            assert await queue.size() == 100_000
            assert await queue.peek() == 0
            taken: list[int] = []
            while batch := await queue.drain(999):
                taken += batch
                largest = max([largest, *map(len, writes.payloads)])
                writes.payloads.clear()
            assert taken == list(range(100_000))
            # A segment holds at most 64 KiB, where one page used to hold the whole queue.
            assert largest <= 64 * 1024

    async def it_writes_as_much_per_offer_to_a_long_queue_as_to_a_short_one() -> None:
        async with ActorSystem() as system:
            writes = system._writes()  # pyright: ignore[reportPrivateUsage]
            queue = Collections(system).queue("jobs", value=bytes)

            async def written(offers: int) -> int:
                total = 0
                for _ in range(offers):
                    await queue.offer(b"x" * 100)
                    total += sum(map(len, writes.payloads))
                    writes.payloads.clear()
                return total

            short = await written(2_000)
            await written(20_000)
            # With the whole queue in one page, the last offers would write more than ten times what the first did.
            assert await written(2_000) < 2 * short

    async def it_keeps_order_across_segments_for_facades_that_lag_behind_the_index() -> None:
        async with ActorSystem() as system:
            first = Collections(system).queue("jobs", value=bytes)
            late = Collections(system).queue("jobs", value=bytes)
            items = [str(i).encode() for i in range(2_500)]
            for item in items:
                await first.offer(item)
            # `late` has not seen the tail move, so it reaches sealed segments and catches up through the index.
            await late.offer(b"late")
            large = b"x" * 100_000
            await first.offer(large)
            await late.offer(b"after")
            assert await late.size() == 2_503
            assert await late.peek() == b"0"
            assert await late.drain(1_500) == items[:1_500]
            assert await first.poll() == b"1500"
            assert await first.drain(2_000) == [*items[1_501:], b"late", large, b"after"]
            assert await late.poll() == MISSING
            for item in items:
                await late.offer(item)
            await first.clear()
            assert await late.size() == 0
            assert await first.poll() == MISSING
            await late.offer(b"again")
            assert await first.poll() == b"again"

    async def it_keeps_no_key_for_the_segments_it_drained() -> None:
        async with ActorSystem(idle_after=timedelta(milliseconds=200)) as system:

            def facade() -> Queue[int]:
                binding = Binding(system, "queue", "drained", replicas=3, write="majority", signature=(repr(int),))
                return Queue(binding, int)

            jobs, lagging = facade(), facade()
            await lagging.offer(-1)
            assert await jobs.poll() == -1
            count = 3 * 1024 + 10
            for item in range(count):
                await jobs.offer(item)
            assert await jobs.drain(count) == list(range(count))
            actor = configured(queue_segment.actor, 3, "majority").name

            async def only_the_tail_is_kept() -> None:
                stored = await system._stored()  # pyright: ignore[reportPrivateUsage]
                segments = [key for held, key, _ in stored if held == actor]
                assert len(segments) == 1, segments

            await eventually(only_the_tail_is_kept, timedelta(seconds=10))
            # `lagging` still offers to the first segment, which was sealed, drained and deleted since: it is refused
            # there as it was before, and the item goes to the tail.
            await lagging.offer(count)
            assert await jobs.poll() == count
            assert await jobs.poll() == MISSING
            assert await lagging.size() == 0
            await eventually(only_the_tail_is_kept, timedelta(seconds=10))

    async def it_canonicalizes_unordered_fields_used_as_keys_or_compared_values() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)
            entries = collections.dict("indices", key=Index, value=str)
            first = Index({"a": 1, "b": 2}, frozenset({"x", "y"}))
            same = Index({"b": 2, "a": 1}, frozenset({"y", "x"}))
            await entries.put(first, "present")
            assert await entries.get(same) == "present"
            register = collections.register("index", value=Index)
            await register.set(first)
            assert await register.compare_and_set(same, Index({}))

    async def it_shares_all_data_collections_between_members_and_a_client() -> None:
        async with Harness.start(3) as harness:
            writer = Collections(harness.nodes[0].system)
            reader = Collections(await harness.client())
            await writer.counter("c", replicas=3).add(5)
            assert await reader.counter("c").get() == 5
            await writer.register("r", value=User).set(User("remote"))
            assert await reader.register("r", value=User).get() == User("remote")
            await writer.dict("d", key=str, value=int).put("one", 1)
            assert await reader.dict("d", key=str, value=int).get("one") == 1
            await writer.set("s", value=str).add("one")
            assert await reader.set("s", value=str).contains("one")
            await writer.multimap("m", key=str, value=int).put("one", 1)
            assert await reader.multimap("m", key=str, value=int).get("one") == [1]
            await writer.queue("q", value=User).offer(User("queued"))
            assert await reader.queue("q", value=User).poll() == User("queued")

    async def it_grants_renews_and_expires_semaphore_leases() -> None:
        async with ActorSystem() as system:
            semaphore = Collections(system).semaphore("workers", capacity=2)
            first = await semaphore.acquire(2, ttl=0.1)
            assert await semaphore.available() == 0
            assert await semaphore.try_acquire() is None
            assert await first.renew(ttl=0.01)
            async with asyncio.timeout(1):
                second = await semaphore.acquire(2)
            assert second.token > first.token
            assert not await first.renew()
            second.release()
            assert await semaphore.available() == 2
            with pytest.raises(ValueError):
                await semaphore.acquire(3)
            with pytest.raises(ValueError):
                await semaphore.acquire(ttl=0)

    async def it_wakes_waiters_on_release_and_cleans_up_timeout_and_cancellation() -> None:
        async with ActorSystem() as system:
            semaphore = Collections(system).semaphore("workers", capacity=1)
            first = await semaphore.acquire()
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.01):
                    await semaphore.acquire()
            async with asyncio.TaskGroup() as group:
                waiting = group.create_task(semaphore.acquire())
                first.release()
            assert await semaphore.available() == 0
            held = waiting.result()
            task = asyncio.create_task(semaphore.acquire())
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            held.release()
            async with await semaphore.acquire():
                assert await semaphore.available() == 0
            assert await semaphore.available() == 1

    async def it_serializes_lock_holders_even_when_the_facade_is_shared() -> None:
        async with ActorSystem() as system:
            lock = Collections(system).lock("resource")
            active = 0
            tokens: list[int] = []

            async def work() -> None:
                nonlocal active
                async with lock as lease:
                    active += 1
                    assert active == 1
                    tokens.append(lease.token)
                    await asyncio.sleep(0)
                    active -= 1

            async with asyncio.TaskGroup() as group:
                for _ in range(10):
                    group.create_task(work())
            assert tokens == sorted(set(tokens))
            assert not await lock.locked()
            first = await lock.try_lock()
            assert first is not None
            assert await lock.try_lock() is None
            first.release()

    async def it_reuses_barrier_generations_and_withdraws_timed_out_arrivals() -> None:
        async with ActorSystem() as system:
            barrier = Collections(system).barrier("round", parties=3)
            assert barrier.parties == 3
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.01):
                    await barrier.wait()
            assert await barrier.waiting() == 0
            for _ in range(3):
                async with asyncio.timeout(1), asyncio.TaskGroup() as group:
                    for _ in range(3):
                        group.create_task(barrier.wait())
                assert await barrier.waiting() == 0

    async def it_allows_a_child_task_to_wait_for_its_parents_lock() -> None:
        async with ActorSystem() as system:
            lock = Collections(system).lock("resource")

            async def child() -> int:
                async with lock as lease:
                    return lease.token

            async with asyncio.timeout(1), asyncio.TaskGroup() as group:
                async with lock as held:
                    pending = group.create_task(child())
                    with pytest.raises(RuntimeError, match="reentrant"):
                        async with lock:
                            pass
                first_token = held.token
            assert pending.result() > first_token

    async def it_reattaches_timed_out_requests_without_duplicate_arrivals_or_grants() -> None:
        async with ActorSystem(ask_timeout=timedelta(milliseconds=20)) as system:
            collections = Collections(system)
            barrier = collections.barrier("round", parties=2)

            async def one_is_waiting() -> None:
                assert await barrier.waiting() == 1

            async with asyncio.timeout(2), asyncio.TaskGroup() as group:
                first = group.create_task(barrier.wait())
                await eventually(one_is_waiting)
                await asyncio.sleep(0.07)
                assert await barrier.waiting() == 1
                assert not first.done()
                group.create_task(barrier.wait())
            assert await barrier.waiting() == 0

            semaphore = collections.semaphore("permits", capacity=1)
            lease = await semaphore.acquire()
            async with asyncio.timeout(2), asyncio.TaskGroup() as group:
                pending = group.create_task(semaphore.acquire())
                await asyncio.sleep(0.07)
                lease.release()
            granted = pending.result()
            assert granted.token == lease.token + 1
            granted.release()
            assert await semaphore.available() == 1

    async def it_withdraws_only_the_cancelled_barrier_participant() -> None:
        async with ActorSystem() as system:
            barrier = Collections(system).barrier("round", parties=3)

            async def two_are_waiting() -> None:
                assert await barrier.waiting() == 2

            async with asyncio.timeout(2), asyncio.TaskGroup() as group:
                cancelled = group.create_task(barrier.wait())
                remaining = group.create_task(barrier.wait())
                await eventually(two_are_waiting)
                cancelled.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await cancelled
                assert await barrier.waiting() == 1
                assert not remaining.done()
                group.create_task(barrier.wait())
                group.create_task(barrier.wait())
            assert await barrier.waiting() == 0

    async def it_expires_abandoned_requests_without_granting_or_counting_them() -> None:
        async with ActorSystem() as system:
            permits = system.ref(semaphore.actor, "raw", initial=SemaphoreState(capacity=1))
            held = await permits.ask(semaphore.Acquire)
            assert isinstance(held, Acquired)
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.005):
                    await permits.ask(semaphore.Acquire, wait=0.02)
            round = system.ref(barrier.actor, "raw")
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.005):
                    await round.ask(barrier.Arrive, uuid4(), 2, time.time() + 0.02)
            await asyncio.sleep(0.04)
            permits.tell(semaphore.Release(held.lease_id))
            assert await permits.ask(semaphore.Get) == Status(capacity=1, available=1, waiting=0)
            assert await round.ask(barrier.Waiting) == 0

    async def it_does_not_replay_queue_removals_after_a_local_timeout() -> None:
        async with ActorSystem() as system:
            queue = Collections(system).queue("jobs", value=str)
            await queue.offer("first")
            await queue.offer("second")
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0):
                    await queue.poll()
            assert await queue.peek() == "second"
            assert await queue.size() == 1
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0):
                    await queue.drain(1)
            assert await queue.peek() == MISSING

    async def it_cleans_up_a_grant_racing_with_cancellation() -> None:
        async with ActorSystem() as system:
            semaphore = Collections(system).semaphore("permits", capacity=1)
            assert await semaphore.available() == 1
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0):
                    await semaphore.acquire()
            assert await semaphore.available() == 1

    async def it_answers_unavailable_when_the_semaphore_it_started_again_is_still_missing() -> None:
        async with ActorSystem() as system:
            permits = Collections(system).semaphore("permits", capacity=1)
            asked = 0

            # What the owner answers while its key changes hands after every replica of the semaphore was lost: the
            # ref that brings the capacity again starts the key without waiting, and the next ask can come first.
            async def missing(_: Ref[semaphore.Message]) -> int:
                nonlocal asked
                asked += 1
                raise NotStarted("casty.collections:semaphore_3_majority/permits was not started")

            with pytest.raises(Unavailable):
                await permits._asked(missing)  # pyright: ignore[reportPrivateUsage]
            assert asked == 2

    async def it_does_not_resubmit_a_wait_after_its_interest_expired(monkeypatch: pytest.MonkeyPatch) -> None:
        now = 100.0
        monkeypatch.setattr(time, "time", lambda: now)
        attempts = 0

        async def stalled(until: float) -> int:
            nonlocal now, attempts
            attempts += 1
            if attempts == 1:
                now = until + 1
                raise TimeoutError
            return 123

        with pytest.raises(TimeoutError):
            await kinds._wait(stalled)  # pyright: ignore[reportPrivateUsage]
        assert attempts == 1
