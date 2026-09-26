import asyncio
import os
import time
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import is_dataclass
from datetime import timedelta
from functools import wraps
from types import SimpleNamespace

import pytest

from casty import ActorFailed, ActorSystem, Collections, Ref
from casty import collections as kinds
from casty.collections import (
    _PROBES,  # pyright: ignore[reportPrivateUsage]
    MISSING,
    Binding,
    ConfigurationError,
    Dict,
    Value,
    configured,
    entry,
    table,
    table_segment,
)
from tests.cluster import Harness
from tests.support import eventually


def binding(system: ActorSystem) -> Binding:
    return Binding(system, "dict", "entries", replicas=3, write="majority", shards=1)


def first_segment(bound: Binding) -> Ref[table_segment.Message]:
    """The segment every key of a one-shard index is in until the shard splits."""
    return bound.ref(table_segment.actor, "0.0")


async def pages(bound: Binding) -> list[Mapping[bytes, tuple[bytes, ...]]]:
    """What every segment of every shard lists, each asked from its own number, which is a hash it holds."""
    found: list[Mapping[bytes, tuple[bytes, ...]]] = []
    for shard in range(bound.shards):
        for at in range(await bound.ref(table.actor, shard).ask(table.Segments())):
            page = await bound.ref(table_segment.actor, f"{shard}.{at}").ask(table_segment.Scan(at))
            assert page is not None
            found.append(page[1])
    return found


async def listings(bound: Binding) -> Mapping[bytes, tuple[bytes, ...]]:
    """Every key the index lists, read from every segment of every shard."""
    return {raw: listed for page in await pages(bound) for raw, listed in page.items()}


def counting[**P, M](built: Counter[str], name: str, message: Callable[P, M]) -> Callable[P, M]:
    """`message`, counting under `name` each one it builds. `ask` still finds the reply type through `wraps`."""

    @wraps(message, updated=())
    def build(*args: P.args, **kwargs: P.kwargs) -> M:
        built[name] += 1
        return message(*args, **kwargs)

    return build


def counted(monkeypatch: pytest.MonkeyPatch, *names: str) -> Counter[str]:
    """How many messages of the collection kinds `names` the facades build from now on, by `kind.Message`."""
    built = Counter[str]()
    for name in names:
        namespace: object = getattr(kinds, name)
        kind: dict[str, object] = dict(vars(namespace))
        held: dict[str, object] = {}
        for field, value in kind.items():
            if field.startswith("__"):
                continue
            if isinstance(value, type) and is_dataclass(value):
                held[field] = counting(built, f"{name}.{field}", value)
            else:
                held[field] = value
        monkeypatch.setattr(kinds, name, SimpleNamespace(**held))
    return built


def describe_dict_entries() -> None:
    async def it_serializes_only_the_changed_value_even_with_one_index_shard() -> None:
        async with ActorSystem() as system:
            writes = system._writes()  # pyright: ignore[reportPrivateUsage]
            entries = Dict(binding(system), str, str)
            values = {str(i): f"value-{i}:" + "x" * 1000 for i in range(20)}
            for key, value in values.items():
                await entries.put(key, value)
            writes.payloads.clear()

            await Dict(binding(system), str, str).put("0", "updated")

            assert not any(value.encode() in payload for value in values.values() for payload in writes.payloads)
            assert await entries.get("0") == "updated"
            assert await entries.get("1") == values["1"]

    async def it_drops_a_listing_whose_value_was_never_saved_when_a_walk_finds_it() -> None:
        async with ActorSystem() as system:
            bound = binding(system)
            entries = Dict(bound, str, str)
            await bound.ready()
            raw = Value(str, bound.system).dump("unfinished")
            # A put that listed its key and stopped before it sent the value again.
            listed = await bound.ref(entry.actor, f"key:{raw.hex()}").ask(entry.Put(b"", 0))
            assert listed > 0
            await first_segment(bound).ask(table_segment.List(raw, listed))
            assert list(await listings(bound)) == [raw]

            assert await entries.get("unfinished") == MISSING
            assert await entries.items() == []
            assert await listings(bound) == {}
            await first_segment(bound).ask(table_segment.List(raw, listed))
            # `size` counts listings and asks no entry: the listing counts until a scan drops it.
            assert await entries.size() == 1
            assert [pair async for pair in entries.scan()] == []
            assert await listings(bound) == {}
            assert await entries.size() == 0
            await first_segment(bound).ask(table_segment.List(raw, listed))
            await entries.clear()
            assert await listings(bound) == {}
            await entries.put("unfinished", "complete")
            assert await entries.items() == [("unfinished", "complete")]

    async def it_unlists_a_removed_key_and_lists_it_again_when_it_is_put_back() -> None:
        async with ActorSystem() as system:
            bound = binding(system)
            entries = Dict(bound, str, str)
            raw = Value(str, bound.system).dump("key")
            await entries.put("key", "first")
            assert await entries.remove("key")
            assert not await entries.remove("key")
            assert await first_segment(bound).ask(table_segment.Get(raw)) == ()
            assert await entries.size() == 0
            assert not await entries.contains("key")
            await entries.put("key", "second")
            assert await entries.get("key") == "second"
            assert await entries.items() == [("key", "second")]

            async with asyncio.TaskGroup() as group:
                for i in range(10):
                    group.create_task(entries.put("key", str(i)))
                    group.create_task(entries.remove("key"))
            await entries.remove("key")
            assert await listings(bound) == {}
            await entries.put("key", "last")
            assert await entries.items() == [("key", "last")]
            assert list(await listings(bound)) == [raw]
            await entries.clear()
            assert await entries.items() == []
            assert await listings(bound) == {}
            await entries.put("key", "again")
            assert await entries.items() == [("key", "again")]

    async def it_unlists_every_removed_key_from_every_index_shard() -> None:
        async with ActorSystem() as system:
            # 750 keys a shard, which splits each of them.
            bound = Binding(system, "dict", "many", replicas=3, write="majority", shards=2)
            entries = Dict(bound, int, int)
            for i in range(1_500):
                await entries.put(i, i)
            assert await entries.size() == 1_500
            for i in range(1_500):
                assert await entries.remove(i)
            assert await listings(bound) == {}
            assert await entries.size() == 0
            await entries.put(7, 70)
            assert await entries.get(7) == 70
            assert await entries.items() == [(7, 70)]

    async def it_keeps_a_key_put_back_listed_when_the_unlisting_of_its_removal_arrives_late() -> None:
        async with ActorSystem() as system:
            bound = binding(system)
            entries = Dict(bound, str, str)
            raw = Value(str, bound.system).dump("key")
            await entries.put("key", "first")
            # The first half of a removal: the value is gone, and the unlisting has not reached the index yet.
            removed, listed = await bound.ref(entry.actor, f"key:{raw.hex()}").ask(entry.Remove())
            assert removed
            await entries.put("key", "second")
            # The unlisting arrives now, under the generation the first put listed the key with.
            assert await first_segment(bound).ask(table_segment.Unlist(raw, listed)) is False
            assert await entries.items() == [("key", "second")]
            assert await entries.remove("key")
            assert await listings(bound) == {}

    async def it_keeps_a_key_listed_when_an_earlier_life_of_it_listed_under_a_clock_that_ran_ahead() -> None:
        async with ActorSystem() as system:
            bound = binding(system)
            entries = Dict(bound, str, str)
            await bound.ready()
            raw = Value(str, bound.system).dump("key")
            # A life of the key on a node whose clock ran an hour ahead, removed with its unlisting still on the way.
            ahead = time.time_ns() // 1_000 + 3_600_000_000
            await first_segment(bound).ask(table_segment.List(raw, ahead))
            await entries.put("key", "value")
            assert await first_segment(bound).ask(table_segment.Unlist(raw, ahead)) is False
            assert await entries.items() == [("key", "value")]
            assert await entries.remove("key")
            assert await listings(bound) == {}

    async def it_leaves_no_visible_value_when_save_fails_after_registration() -> None:
        async with ActorSystem() as system:
            writes = system._writes()  # pyright: ignore[reportPrivateUsage]
            bound = binding(system)
            entries = Dict(bound, str, str)
            writes.fail_on = Value(str, bound.system).dump("uncommitted")
            with pytest.raises(ActorFailed, match="value save failed"):
                await entries.put("key", "uncommitted")

            raw = Value(str, bound.system).dump("key")
            assert list(await listings(bound)) == [raw]
            assert await entries.get("key") == MISSING
            assert await entries.items() == []
            assert await listings(bound) == {}
            assert await entries.size() == 0
            await entries.put("key", "committed")
            assert await entries.items() == [("key", "committed")]
            assert await entries.remove("key")
            assert await listings(bound) == {}

    async def it_splits_a_shard_into_segments_and_finds_every_key_after() -> None:
        async with ActorSystem() as system:
            bound = Binding(system, "dict", "split", replicas=3, write="majority", shards=1)
            entries = Dict(bound, int, int)
            for i in range(3_000):
                await entries.put(i, i * 2)
            assert await bound.ref(table.actor).ask(table.Segments()) > 4
            assert await entries.size() == 3_000
            assert dict(await entries.items()) == {i: i * 2 for i in range(3_000)}
            # A facade that has not seen the splits reaches every key through the directory.
            late = Dict(bound, int, int)
            for i in range(0, 3_000, 2):
                assert await late.remove(i)
            await late.put(-1, -2)
            assert await entries.size() == 1_501
            assert len(await listings(bound)) == 1_501
            assert dict(await entries.items()) == {i: i * 2 for i in range(-1, 3_000) if i % 2}

    async def it_keeps_every_key_when_writers_split_the_index_at_once() -> None:
        async with ActorSystem() as system:
            bound = Binding(system, "dict", "busy", replicas=3, write="majority", shards=1)
            writers = [Dict(bound, int, int) for _ in range(8)]
            keys = 4_000

            async def fill(first: int) -> None:
                facade = writers[first % len(writers)]
                for key in range(first, keys, 64):
                    await facade.put(key, key)

            async with asyncio.TaskGroup() as group:
                for first in range(64):
                    group.create_task(fill(first))
            assert len(await listings(bound)) == keys
            assert await writers[1].size() == keys
            assert dict(await writers[2].items()) == {key: key for key in range(keys)}

    async def it_counts_the_keys_with_one_ask_to_each_segment_and_none_to_the_entries(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async with ActorSystem() as system:
            bound = Binding(system, "dict", "counted", replicas=3, write="majority", shards=16)
            entries = Dict(bound, int, int)
            for i in range(10_000):
                await entries.put(i, i)
            segments = [await bound.ref(table.actor, shard).ask(table.Segments()) for shard in range(bound.shards)]
            asked = counted(monkeypatch, "entry", "table", "table_segment")

            assert await entries.size() == 10_000
            # Two reads of each directory and one ask to each segment, all at once, where there was one ask per key.
            assert asked == Counter({"table.Segments": 2 * bound.shards, "table_segment.Size": sum(segments)})

    async def it_reads_one_segment_at_a_time_and_the_values_of_its_keys_together(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async with ActorSystem() as system:
            bound = Binding(system, "dict", "paged", replicas=3, write="majority", shards=1)
            entries = Dict(bound, int, int)
            for i in range(3_000):
                await entries.put(i, -i)
            sizes = [len(page) for page in await pages(bound)]
            assert len(sizes) > 4
            probing = peak = 0
            value_of = entries._value_of  # pyright: ignore[reportPrivateUsage]

            async def watched(raw: bytes, listed: tuple[bytes, ...]) -> bytes | None:
                nonlocal probing, peak
                probing += 1
                peak = max(peak, probing)
                # Probes started at once all get here before any goes on.
                await asyncio.sleep(0)
                try:
                    return await value_of(raw, listed)
                finally:
                    probing -= 1

            monkeypatch.setattr(entries, "_value_of", watched)
            asked = counted(monkeypatch, "entry", "table_segment")
            scan = entries.scan()
            first = await anext(scan)
            # Only the first segment was read, and every key of it was asked for its value, `_PROBES` at a time.
            assert asked["table_segment.Scan"] == 1
            assert asked["entry.Get"] == sizes[0]
            assert peak == min(_PROBES, sizes[0])
            rest = [pair async for pair in scan]
            assert dict([first, *rest]) == {i: -i for i in range(3_000)}
            assert len(rest) == 2_999
            assert asked["table_segment.Scan"] == len(sizes)
            assert asked["entry.Get"] == 3_000
            assert peak == min(_PROBES, max(sizes))

            await entries.clear()
            assert await listings(bound) == {}
            assert await entries.size() == 0

    async def it_scans_each_key_there_throughout_once_while_splits_move_keys_around_it() -> None:
        async with ActorSystem() as system:
            bound = Binding(system, "dict", "moving", replicas=3, write="majority", shards=1)
            entries = Dict(bound, int, int)
            for i in range(600):
                await entries.put(i, i)
            assert await bound.ref(table.actor).ask(table.Segments()) == 2
            seen: list[int] = []
            async for key, value in entries.scan():
                assert value == key
                if not seen:
                    # The first segment was read: splits now move keys out of it and out of the one still to be read.
                    for i in range(600, 6_000):
                        await entries.put(i, i)
                seen.append(key)
            assert await bound.ref(table.actor).ask(table.Segments()) > 8
            assert len(seen) == len(set(seen))
            assert set(range(600)) <= set(seen) <= set(range(6_000))
            # Keys put where the scan had not been yet are found there.
            assert len(seen) > 600

    async def it_writes_as_much_to_add_a_key_to_a_large_dict_as_to_a_small_one() -> None:
        async with ActorSystem() as system:
            writes = system._writes()  # pyright: ignore[reportPrivateUsage]
            entries = Dict(Binding(system, "dict", "flat", replicas=3, write="majority", shards=1), int, int)

            async def written(keys: range) -> int:
                total = 0
                for key in keys:
                    await entries.put(key, key)
                    total += sum(map(len, writes.payloads))
                    writes.payloads.clear()
                return total

            small = await written(range(2_000))
            await written(range(2_000, 20_000))
            # With every key of the shard in one page, these would write about ten times what the first did.
            assert await written(range(20_000, 22_000)) < 2 * small

    @pytest.mark.skipif(not os.environ.get("CASTY_LARGE"), reason="a million keys take minutes: set CASTY_LARGE=1")
    async def it_builds_and_reads_back_a_million_keys() -> None:
        async with ActorSystem() as system:
            entries = Collections(system).dict("large", key=int, value=int)
            keys = 1_000_000
            workers = 256

            async def fill(first: int) -> None:
                for key in range(first, keys, workers):
                    await entries.put(key, -key)

            async with asyncio.TaskGroup() as group:
                for first in range(workers):
                    group.create_task(fill(first))
            assert await entries.size() == keys
            assert await entries.get(keys - 1) == 1 - keys
            assert dict(await entries.items()) == {key: -key for key in range(keys)}

    async def it_refuses_the_old_bucket_layout_instead_of_silently_hiding_its_values() -> None:
        async with ActorSystem() as system:
            old = Binding(
                system,
                "dict",
                "old",
                replicas=3,
                write="majority",
                shards=16,
                signature=(repr(str), repr(str)),
            )
            await old.ready()
            raw = Value(str, old.system).dump("key")
            value = Value(str, old.system).dump("saved")
            segment = old.ref(table_segment.actor, f"{old.shard(raw)}.0")
            await segment.ask(table_segment.Add(raw, value))

            with pytest.raises(ConfigurationError):
                await Collections(system).dict("old", key=str, value=str).get("key")

            assert await segment.ask(table_segment.Get(raw)) == (value,)

    async def it_keeps_dictionary_names_and_encoded_keys_separate() -> None:
        async with ActorSystem() as system:
            collections = Collections(system)
            first = collections.dict("a", key=str, value=str, index_shards=1)
            second = collections.dict("a:1", key=str, value=str, index_shards=1)
            for key in ("", "0", "key:0", "a:1:key:0", "ç"):
                await first.put(key, "first")
                await second.put(key, "second")
            assert {value for _, value in await first.items()} == {"first"}
            assert {value for _, value in await second.items()} == {"second"}
            with pytest.raises(ConfigurationError):
                await collections.dict("a", key=str, value=str, index_shards=2).size()


async def stored_entries(system: ActorSystem, bound: Binding) -> list[tuple[str, bool]]:
    """The keys of the entries of `bound` whose state `system` keeps, and whether it keeps only their tombstone."""
    actor = configured(entry.actor, bound.replicas, bound.write).name
    stored = await system._stored()  # pyright: ignore[reportPrivateUsage]
    return [(key, deleted) for held, key, deleted in stored if held == actor]


def describe_deleted_entries() -> None:
    async def it_keeps_no_state_of_the_keys_it_removed() -> None:
        async with ActorSystem() as system:
            bound = binding(system)
            entries = Dict(bound, int, int)
            for i in range(10_000):
                await entries.put(i, i)
            assert len(await stored_entries(system, bound)) == 10_000

            for i in range(10_000):
                assert await entries.remove(i)

            assert await stored_entries(system, bound) == []
            assert await entries.size() == 0
            # A key removed starts over when it is put back.
            assert await entries.get(3) == MISSING
            await entries.put(3, 30)
            assert await entries.items() == [(3, 30)]

    async def it_keeps_no_entry_for_a_key_that_was_only_read() -> None:
        async with ActorSystem(idle_after=timedelta(milliseconds=100)) as system:
            bound = binding(system)
            entries = Dict(bound, str, str)
            assert await entries.get("never") == MISSING
            assert not await entries.contains("never")

            async def gone() -> None:
                assert await stored_entries(system, bound) == []

            await eventually(gone)

    async def it_keeps_no_page_of_the_keys_it_removed_on_any_replica() -> None:
        async with Harness.start(3) as harness:
            system = harness.nodes[0].system
            bound = Binding(system, "dict", "removed", replicas=3, write="majority", shards=4)
            entries = Dict(bound, int, int)
            gate = asyncio.Semaphore(200)

            async def cycle(i: int) -> None:
                async with gate:
                    await entries.put(i, i)
                    assert await entries.remove(i)

            async with asyncio.TaskGroup() as group:
                for i in range(10_000):
                    group.create_task(cycle(i))
            assert await entries.size() == 0

            async def tombstones_only() -> None:
                for node in harness.nodes:
                    kept = await stored_entries(node.system, bound)
                    assert all(deleted for _, deleted in kept), f"{node.address} keeps the state of a removed key"

            async def nothing() -> None:
                for node in harness.nodes:
                    kept = await stored_entries(node.system, bound)
                    assert kept == [], f"{node.address} keeps {len(kept)} tombstones"

            await eventually(tombstones_only, timedelta(seconds=10))
            # A tombstone goes once it has lingered and every other replica has answered for it.
            await eventually(nothing, timedelta(seconds=60))
            await entries.put(7, 70)
            assert await entries.get(7) == 70
            assert await entries.items() == [(7, 70)]
