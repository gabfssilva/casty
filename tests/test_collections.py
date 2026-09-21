import asyncio
import os
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta

import pytest

from casty import ActorSystem


@dataclass(frozen=True)
class User:
    name: str
    nickname: str | None = None


@dataclass(frozen=True)
class Index:
    values: Mapping[str, int]
    labels: frozenset[str] = frozenset()


def describe_counter_actor() -> None:
    async def it_serializes_additions_and_resets_the_named_counter() -> None:
        from casty.collections import counter

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


def describe_register_actor() -> None:
    async def it_allows_only_one_competing_compare_and_set() -> None:
        from casty.collections import register

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
        from casty import Collections
        from casty.collections import MISSING

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
        from casty import Collections
        from casty.collections import ConfigurationError

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

    async def it_resolves_configured_actor_types_without_a_local_facade() -> None:
        from casty import Collections

        async with ActorSystem() as system:
            counter = Collections(system).counter("visits", replicas=5, write="all")
            await counter.add()
            from casty import collections as kinds

            actor = kinds.configured(kinds.counter.actor, 5, "all")
            # A node that never touched the type finds it by its name, with the configuration the name carries.
            resolved = ActorSystem()._resolve(actor.name)  # pyright: ignore[reportPrivateUsage]
            assert resolved is not None
            assert (resolved.name, resolved.replicas, resolved.write) == (actor.name, 5, "all")

    async def it_indexes_dictionary_entries_and_preserves_typed_values() -> None:
        from casty import Collections
        from casty.collections import MISSING

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
        from casty import Collections

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

    async def it_delivers_queue_items_once_in_fifo_order_without_failures() -> None:
        from casty import Collections
        from casty.collections import MISSING

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

    async def it_canonicalizes_unordered_fields_used_as_keys_or_compared_values() -> None:
        from casty import Collections

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
        from casty import Collections
        from tests.cluster import Harness

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
        from casty import Collections

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
            assert not await first.release()
            assert await second.release()
            assert not await second.release()
            assert await semaphore.available() == 2
            with pytest.raises(ValueError):
                await semaphore.acquire(3)
            with pytest.raises(ValueError):
                await semaphore.acquire(ttl=0)

    async def it_wakes_waiters_on_release_and_cleans_up_timeout_and_cancellation() -> None:
        from casty import Collections

        async with ActorSystem() as system:
            semaphore = Collections(system).semaphore("workers", capacity=1)
            first = await semaphore.acquire()
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.01):
                    await semaphore.acquire()
            async with asyncio.TaskGroup() as group:
                waiting = group.create_task(semaphore.acquire())
                await first.release()
            assert await semaphore.available() == 0
            held = waiting.result()
            task = asyncio.create_task(semaphore.acquire())
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await held.release()
            async with await semaphore.acquire():
                assert await semaphore.available() == 0
            assert await semaphore.available() == 1

    async def it_serializes_lock_holders_even_when_the_facade_is_shared() -> None:
        from casty import Collections

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
            await first.release()

    async def it_reuses_barrier_generations_and_withdraws_timed_out_arrivals() -> None:
        from casty import Collections

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
        from casty import Collections

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
        from casty import Collections
        from tests.support import eventually

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
                await lease.release()
            granted = pending.result()
            assert granted.token == lease.token + 1
            await granted.release()
            assert await semaphore.available() == 1

    async def it_withdraws_only_the_cancelled_barrier_participant() -> None:
        from casty import Collections
        from tests.support import eventually

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
        from uuid import uuid4

        from casty.collections import barrier, semaphore

        async with ActorSystem() as system:
            permits = system.ref(semaphore.actor, "raw")
            held = await permits.ask(semaphore.Request, uuid4(), 1, 30.0, 1, None)
            assert held is not None
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.005):
                    await permits.ask(semaphore.Request, uuid4(), 1, 30.0, 1, time.time() + 0.02)
            round = system.ref(barrier.actor, "raw")
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.005):
                    await round.ask(barrier.Arrive, uuid4(), 2, time.time() + 0.02)
            await asyncio.sleep(0.04)
            assert await permits.ask(semaphore.Release, held)
            assert await permits.ask(semaphore.Available, 1) == 1
            assert await round.ask(barrier.Waiting) == 0

    async def it_does_not_replay_queue_removals_after_a_local_timeout() -> None:
        from casty import Collections
        from casty.collections import MISSING

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
        from casty import Collections

        async with ActorSystem() as system:
            semaphore = Collections(system).semaphore("permits", capacity=1)
            assert await semaphore.available() == 1
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0):
                    await semaphore.acquire()
            assert await semaphore.available() == 1

    async def it_does_not_resubmit_a_wait_after_its_interest_expired(monkeypatch: pytest.MonkeyPatch) -> None:
        from casty import collections

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
            await collections._wait(stalled)  # pyright: ignore[reportPrivateUsage]
        assert attempts == 1
