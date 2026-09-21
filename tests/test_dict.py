import asyncio

import pytest

from casty import ActorFailed, ActorSystem, Collections
from casty.collections import MISSING, Binding, ConfigurationError, Dict, Value, table


def binding(system: ActorSystem) -> Binding:
    return Binding(system, "dict", "entries", replicas=3, write="majority", shards=1)


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

    async def it_ignores_an_index_registration_whose_value_was_never_saved() -> None:
        async with ActorSystem() as system:
            bound = binding(system)
            entries = Dict(bound, str, str)
            await bound.ready()
            raw = Value(str, bound.system).dump("unfinished")
            await bound.ref(table.actor).ask(table.Add, raw, b"")

            assert await entries.get("unfinished") == MISSING
            assert await entries.items() == []
            assert await entries.size() == 0
            await entries.clear()
            await entries.put("unfinished", "complete")
            assert await entries.items() == [("unfinished", "complete")]

    async def it_keeps_registration_across_removal_and_reinsertion() -> None:
        async with ActorSystem() as system:
            bound = binding(system)
            entries = Dict(bound, str, str)
            await entries.put("key", "first")
            assert await entries.remove("key")
            assert not await entries.remove("key")
            raw = Value(str, bound.system).dump("key")
            assert await bound.ref(table.actor).ask(table.Get, raw) == (b"",)
            assert await entries.size() == 0
            assert not await entries.contains("key")

            async with asyncio.TaskGroup() as group:
                for i in range(10):
                    group.create_task(entries.put("key", str(i)))
                    group.create_task(entries.remove("key"))
            await entries.put("key", "last")
            assert await entries.items() == [("key", "last")]
            await entries.clear()
            assert await entries.items() == []
            await entries.put("key", "again")
            assert await entries.items() == [("key", "again")]

    async def it_leaves_no_visible_value_when_save_fails_after_registration() -> None:
        async with ActorSystem() as system:
            writes = system._writes()  # pyright: ignore[reportPrivateUsage]
            bound = binding(system)
            entries = Dict(bound, str, str)
            writes.fail_on = Value(str, bound.system).dump("uncommitted")
            with pytest.raises(ActorFailed, match="value save failed"):
                await entries.put("key", "uncommitted")

            raw = Value(str, bound.system).dump("key")
            assert await bound.ref(table.actor).ask(table.Get, raw) == (b"",)
            assert await entries.get("key") == MISSING
            assert await entries.items() == []
            assert await entries.size() == 0
            await entries.put("key", "committed")
            assert await entries.items() == [("key", "committed")]

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
            await old.ref(table.actor, old.shard(raw)).ask(table.Add, raw, value)

            with pytest.raises(ConfigurationError):
                await Collections(system).dict("old", key=str, value=str).get("key")

            assert await old.ref(table.actor, old.shard(raw)).ask(table.Get, raw) == (value,)

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
