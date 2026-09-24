"""The store of a system: the state of a durable type outlives the process that wrote it, and every replica."""

from dataclasses import dataclass
from datetime import timedelta
from itertools import count
from operator import methodcaller
from pathlib import Path
from typing import assert_never

import pytest

from casty import ActorSystem, Context, Ref, Store, Unavailable, actor
from casty.sqlite import SQLiteStore
from tests.app import Append, Entries, durable_ledger, ledger
from tests.cluster import WITHIN, Harness, Node
from tests.support import Records, eventually
from tests.traffic import kept


@dataclass(frozen=True)
class Add:
    reply_to: Ref[int]
    amount: int


@dataclass(frozen=True)
class Total:
    reply_to: Ref[int]


@dataclass(frozen=True)
class Forget:
    reply_to: Ref[bool]


type TallyMsg = Add | Total | Forget


async def _counted(ctx: Context[int, TallyMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Add(reply_to, amount):
                await ctx.state.set(ctx.state.value + amount)
                reply_to.tell(ctx.state.value)
            case Total(reply_to):
                reply_to.tell(ctx.state.value)
            case Forget(reply_to):
                await ctx.state.delete()
                reply_to.tell(True)
            case _:
                assert_never(msg)


# A type is named after its body, so each setting takes a body of its own.
@actor(initial=0, durable="write")
async def tally(ctx: Context[int, TallyMsg]) -> None:
    await _counted(ctx)


@actor(initial=0, durable=timedelta(milliseconds=200))
async def lazy(ctx: Context[int, TallyMsg]) -> None:
    await _counted(ctx)


@actor(initial=0)
async def forgetful(ctx: Context[int, TallyMsg]) -> None:
    await _counted(ctx)


def describe_a_durable_type() -> None:
    def it_says_when_the_store_keeps_its_writes() -> None:
        assert tally.durable == "write"
        assert lazy.durable == timedelta(milliseconds=200)
        assert forgetful.durable is None
        assert tally.configured("tests.storage.tally[r1]", 1, "one").durable == "write"

    def it_refuses_what_is_neither_write_nor_a_period() -> None:
        # Through `methodcaller`, because the checkers refuse these calls.
        with pytest.raises(ValueError, match="durable"):
            methodcaller("__call__", initial=0, durable="always")(actor)
        with pytest.raises(TypeError, match="durable"):
            methodcaller("__call__", initial=0, durable=5)(actor)
        with pytest.raises(ValueError, match="durable"):
            actor(initial=0, durable=timedelta(seconds=-1))

    def it_takes_as_a_store_any_object_with_the_three_methods_of_one() -> None:
        assert isinstance(Records(), Store)
        with pytest.raises(TypeError, match="store"):
            methodcaller("__call__", store=object())(ActorSystem)

    def when_a_system_alone_stops_and_another_starts_on_its_store() -> None:
        async def it_takes_every_key_back_as_the_one_before_left_it() -> None:
            store = Records()
            async with ActorSystem(store=store) as system:
                for key in ("a", "b"):
                    await system.ref(tally, key).ask(Add, 5)
                    await system.ref(tally, key).ask(Add, 7)

            async with ActorSystem(store=store) as system:
                assert await system.ref(tally, "a").ask(Total) == 12
                assert await system.ref(tally, "b").ask(Add, 1) == 13

            # What the second one wrote was kept over what the first one did.
            async with ActorSystem(store=store) as system:
                assert await system.ref(tally, "b").ask(Total) == 13

        async def it_starts_a_deleted_key_from_its_initial_state() -> None:
            store = Records()
            async with ActorSystem(store=store) as system:
                ref = system.ref(tally, "gone")
                await ref.ask(Add, 5)
                assert await ref.ask(Forget) is True

            async with ActorSystem(store=store) as system:
                assert await system.ref(tally, "gone").ask(Total) == 0

        async def it_keeps_nothing_of_a_type_that_is_not_durable() -> None:
            store = Records()
            async with ActorSystem(store=store) as system:
                await system.ref(forgetful, "f").ask(Add, 5)

            async with ActorSystem(store=store) as system:
                assert await system.ref(forgetful, "f").ask(Total) == 0
            assert store.records == {}

    def when_the_store_does_not_keep_a_write() -> None:
        async def it_fails_the_message_with_unavailable_and_the_next_write_goes_on() -> None:
            store = Records()
            async with ActorSystem(store=store) as system:
                ref = system.ref(tally, "t-1")
                assert await ref.ask(Add, 1) == 1
                store.failing = True
                with pytest.raises(Unavailable):
                    await ref.ask(Add, 1)
                store.failing = False
                assert await ref.ask(Add, 2) == 3

            async with ActorSystem(store=store) as system:
                assert await system.ref(tally, "t-1").ask(Total) == 3

    def when_a_type_saves_on_a_schedule() -> None:
        async def it_answers_at_once_and_saves_the_last_write_once_the_period_is_over() -> None:
            store = Records()
            async with ActorSystem(store=store) as system:
                ref = system.ref(lazy, "l-1")
                for _ in range(3):
                    await ref.ask(Add, 1)
                assert store.saves == 0

                async def saved_once() -> None:
                    assert store.saves == 1

                await eventually(saved_once)

            async with ActorSystem(store=store) as system:
                assert await system.ref(lazy, "l-1").ask(Total) == 3

    def when_the_system_has_no_store() -> None:
        async def it_does_not_activate_a_durable_type() -> None:
            async with ActorSystem() as system:
                with pytest.raises(Unavailable):
                    await system.ref(tally, "t-1").ask(Total)

    def when_the_nodes_of_a_cluster_share_a_store() -> None:
        async def it_keeps_every_write_their_replicas_confirmed_and_fails_the_ones_it_does_not() -> None:
            store = Records()
            async with Harness.start(3, store=store) as harness:
                ref = harness.nodes[0].system.ref(tally, "c-1")
                assert await ref.ask(Add, 4) == 4
                assert await ref.ask(Add, 5) == 9
                assert (tally.name, "c-1") in store.records
                saved = store.saves

                store.failing = True
                with pytest.raises(Unavailable):
                    await ref.ask(Add, 1)
                assert store.saves == saved

    def when_a_node_meets_it_while_it_joins() -> None:
        async def it_saves_the_writes_of_the_keys_the_node_owns_once_it_is_in() -> None:
            store = Records()
            async with Harness.start(1, store=store) as harness:
                [a] = harness.nodes
                # What a replication message or a command reaching the node before it has entered makes it do.
                b = await harness.add(joining=lambda system: system._resolve(tally.name))  # pyright: ignore[reportPrivateUsage]
                key = await _placed_on(b, a)
                assert await a.system.ref(tally, key).ask(Add, 4) == 4
                assert (tally.name, key) in store.records


def describe_the_sqlite_store() -> None:
    async def it_opens_its_file_only_once_entered(tmp_path: Path) -> None:
        path = tmp_path / "records.db"
        store = SQLiteStore(path)
        assert not path.exists()
        async with store:
            assert path.exists()

    def when_it_was_not_entered() -> None:
        async def it_refuses_every_statement(tmp_path: Path) -> None:
            store = SQLiteStore(tmp_path / "records.db")
            with pytest.raises(RuntimeError, match="async with"):
                await store.load("a", "k")
            async with store:
                assert await store.load("a", "k") is None

    async def it_keeps_of_the_saves_of_a_key_the_one_of_the_greatest_version(tmp_path: Path) -> None:
        async with SQLiteStore(tmp_path / "records.db") as store:
            assert await store.load("a", "k") is None
            await store.save("a", "k", _version(2), b"two")
            await store.save("a", "k", _version(1), b"one")
            assert await store.load("a", "k") == (_version(2), b"two")
            await store.save("a", "k", _version(3), None)
            assert await store.load("a", "k") == (_version(3), None)
            assert await store.load("a", "other") is None
            assert await store.load("b", "k") is None

    async def it_forgets_a_record_only_when_it_is_not_later_than_the_drop(tmp_path: Path) -> None:
        async with SQLiteStore(tmp_path / "records.db") as store:
            await store.save("a", "k", _version(2), None)
            await store.drop("a", "k", _version(1))
            assert await store.load("a", "k") == (_version(2), None)
            await store.drop("a", "k", _version(2))
            assert await store.load("a", "k") is None

    async def it_shares_its_records_with_every_store_on_the_same_file(tmp_path: Path) -> None:
        path = tmp_path / "records.db"
        async with SQLiteStore(path) as one, SQLiteStore(path) as other:
            assert isinstance(one, Store)
            await one.save("a", "k", _version(1), b"one")
            assert await other.load("a", "k") == (_version(1), b"one")
            await other.save("a", "k", _version(3), b"three")
            await one.save("a", "k", _version(2), b"late")
            assert await one.load("a", "k") == (_version(3), b"three")

        async with SQLiteStore(path) as reopened:
            assert await reopened.load("a", "k") == (_version(3), b"three")


def describe_a_cluster_on_a_sqlite_store() -> None:
    def when_every_node_stops_and_as_many_new_ones_start_on_its_file() -> None:
        async def it_reads_every_durable_key_back_as_last_confirmed(tmp_path: Path) -> None:
            path = tmp_path / "records.db"
            keys = {f"k-{index}": (index, index + 100, index + 200) for index in range(24)}
            async with SQLiteStore(path) as store, Harness.start(3, store=store) as harness:
                for index, (key, entries) in enumerate(keys.items()):
                    ref = harness.nodes[index % 3].system.ref(durable_ledger, key)
                    for entry in entries:
                        assert await ref.ask(Append, entry)
                assert await harness.nodes[0].system.ref(ledger, "memory").ask(Append, 1)

            # Leaving the harness stopped every node: no process holds a replica of any key any more.
            async with SQLiteStore(path) as store, Harness.start(3, store=store) as harness:
                for index, (key, entries) in enumerate(keys.items()):
                    listing = await harness.nodes[(index + 1) % 3].system.ref(durable_ledger, key).ask(Entries)
                    assert listing.entries == entries, key
                assert (await harness.nodes[0].system.ref(ledger, "memory").ask(Entries)).entries == ()

    def when_keys_were_written_while_a_node_was_down() -> None:
        async def it_reads_them_back_once_every_node_crashed_and_new_ones_started(tmp_path: Path) -> None:
            path = tmp_path / "records.db"
            keys = [f"k-{index}" for index in range(12)]
            entries = count(1)
            attempted: dict[str, set[int]] = {key: set() for key in keys}
            confirmed: dict[str, set[int]] = {key: set() for key in keys}

            async def append(system: ActorSystem, key: str) -> None:
                """Append to `key` until an append is confirmed. An attempt that failed may have been applied."""

                async def appended() -> None:
                    entry = next(entries)
                    attempted[key].add(entry)
                    assert await system.ref(durable_ledger, key).ask(Append, entry)
                    confirmed[key].add(entry)

                await eventually(appended, WITHIN)

            async with SQLiteStore(path) as store, Harness.start(3, store=store) as harness:
                a, b, c = harness.nodes
                for key in keys[:8]:
                    await append(a.system, key)
                await harness.crash(c)
                # Until the others remove it, the keys `c` owned answer `Unavailable`, and `append` tries again.
                for key in keys:
                    await append(b.system, key)
                await harness.crash(a)
                await harness.crash(b)

            async with SQLiteStore(path) as store, Harness.start(3, store=store) as harness:
                for key in keys:
                    listing = await harness.nodes[0].system.ref(durable_ledger, key).ask(Entries)
                    broken = kept(key, listing.entries, confirmed[key], attempted[key])
                    assert not broken, "; ".join(broken)


async def _placed_on(node: Node, by: Node) -> str:
    """The first of `t-0`, `t-1`, … that `by` places on `node`, once it sees `node` alive.

    Only `by` is asked: asking a node where a key of a type is makes it meet the type.
    """

    async def seen() -> None:
        assert node.system.node in {member.node for member in by.system.members if member.status == "alive"}

    await eventually(seen, WITHIN)
    for index in range(64):
        key = f"t-{index}"
        if (await by.system.placement(tally, key)).owner == node.system.node:
            return key
    raise AssertionError(f"none of the first 64 keys is placed on {node.address} by {by.address}")


def _version(order: int, /) -> bytes:
    """A version of 32 bytes, as casty writes them, which sorts as `order` does."""
    return order.to_bytes(32, "big")
