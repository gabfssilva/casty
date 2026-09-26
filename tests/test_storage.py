"""The store of a system: the state of a durable type outlives the process that wrote it, and every replica."""

import asyncio
import os
from dataclasses import dataclass
from datetime import timedelta
from itertools import count
from operator import methodcaller
from pathlib import Path
from random import Random
from typing import assert_never
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from casty import ActorSystem, Askable, Context, Store, Unavailable, actor
from casty.stores import SQL
from tests.app import Append, Entries, durable_ledger, ledger
from tests.cluster import WITHIN, Harness, Node
from tests.support import Records, eventually
from tests.traffic import kept

STORES = ["sqlite", *os.environ.get("CASTY_STORES", "").split()]
"""Where the tests of a store run: a SQLite file of each test, and each database whose URL `CASTY_STORES` lists."""


def _named(where: str, /) -> str:
    """`where` without the user and password its URL may carry."""
    parts = urlsplit(where)
    return where if not parts.scheme else f"{parts.scheme}-{parts.hostname}-{parts.port}"


_each_store = pytest.mark.parametrize("where", STORES, ids=_named)


@dataclass(frozen=True)
class Add(Askable[int]):
    amount: int


@dataclass(frozen=True)
class Total(Askable[int]):
    pass


@dataclass(frozen=True)
class Forget(Askable[bool]):
    pass


type TallyMsg = Add | Total | Forget


async def _counted(ctx: Context[int, TallyMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Add(amount, reply_to=reply_to):
                await ctx.state.set(ctx.state.value + amount)
                reply_to.tell(ctx.state.value)
            case Total(reply_to=reply_to):
                reply_to.tell(ctx.state.value)
            case Forget(reply_to=reply_to):
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
                    await system.ref(tally, key).ask(Add(5))
                    await system.ref(tally, key).ask(Add(7))

            async with ActorSystem(store=store) as system:
                assert await system.ref(tally, "a").ask(Total()) == 12
                assert await system.ref(tally, "b").ask(Add(1)) == 13

            # What the second one wrote was kept over what the first one did.
            async with ActorSystem(store=store) as system:
                assert await system.ref(tally, "b").ask(Total()) == 13

        async def it_starts_a_deleted_key_from_its_initial_state() -> None:
            store = Records()
            async with ActorSystem(store=store) as system:
                ref = system.ref(tally, "gone")
                await ref.ask(Add(5))
                assert await ref.ask(Forget()) is True

            async with ActorSystem(store=store) as system:
                assert await system.ref(tally, "gone").ask(Total()) == 0

        async def it_keeps_nothing_of_a_type_that_is_not_durable() -> None:
            store = Records()
            async with ActorSystem(store=store) as system:
                await system.ref(forgetful, "f").ask(Add(5))

            async with ActorSystem(store=store) as system:
                assert await system.ref(forgetful, "f").ask(Total()) == 0
            assert store.records == {}

    def when_the_store_does_not_keep_a_write() -> None:
        async def it_fails_the_message_with_unavailable_and_the_next_write_goes_on() -> None:
            store = Records()
            async with ActorSystem(store=store) as system:
                ref = system.ref(tally, "t-1")
                assert await ref.ask(Add(1)) == 1
                store.failing = True
                with pytest.raises(Unavailable):
                    await ref.ask(Add(1))
                store.failing = False
                assert await ref.ask(Add(2)) == 3

            async with ActorSystem(store=store) as system:
                assert await system.ref(tally, "t-1").ask(Total()) == 3

    def when_a_type_saves_on_a_schedule() -> None:
        async def it_answers_at_once_and_saves_the_last_write_once_the_period_is_over() -> None:
            store = Records()
            async with ActorSystem(store=store) as system:
                ref = system.ref(lazy, "l-1")
                for _ in range(3):
                    await ref.ask(Add(1))
                assert store.saves == 0

                async def saved_once() -> None:
                    assert store.saves == 1

                await eventually(saved_once)

            async with ActorSystem(store=store) as system:
                assert await system.ref(lazy, "l-1").ask(Total()) == 3

    def when_the_system_has_no_store() -> None:
        async def it_does_not_activate_a_durable_type() -> None:
            async with ActorSystem() as system:
                with pytest.raises(Unavailable):
                    await system.ref(tally, "t-1").ask(Total())

    def when_the_nodes_of_a_cluster_share_a_store() -> None:
        async def it_keeps_every_write_their_replicas_confirmed_and_fails_the_ones_it_does_not() -> None:
            store = Records()
            async with Harness.start(3, store=store) as harness:
                ref = harness.nodes[0].system.ref(tally, "c-1")
                assert await ref.ask(Add(4)) == 4
                assert await ref.ask(Add(5)) == 9
                assert (tally.name, "c-1") in store.records
                saved = store.saves

                store.failing = True
                with pytest.raises(Unavailable):
                    await ref.ask(Add(1))
                assert store.saves == saved

    def when_a_node_meets_it_while_it_joins() -> None:
        async def it_saves_the_writes_of_the_keys_the_node_owns_once_it_is_in() -> None:
            store = Records()
            async with Harness.start(1, store=store) as harness:
                [a] = harness.nodes
                # What a replication message or a command reaching the node before it has entered makes it do.
                b = await harness.add(joining=lambda system: system._resolve(tally.name))  # pyright: ignore[reportPrivateUsage]
                key = await _placed_on(b, a)
                assert await a.system.ref(tally, key).ask(Add(4)) == 4
                assert (tally.name, key) in store.records


def describe_a_store() -> None:
    @_each_store
    async def it_keeps_of_the_saves_of_a_key_the_one_of_the_greatest_version(where: str, tmp_path: Path) -> None:
        async with _opened(where, tmp_path) as store:
            actor = _actor()
            assert await store.load(actor, "k") is None
            await store.save(actor, "k", _version(2), b"two")
            await store.save(actor, "k", _version(1), b"one")
            assert await store.load(actor, "k") == (_version(2), b"two")
            await store.save(actor, "k", _version(3), None)
            assert await store.load(actor, "k") == (_version(3), None)
            assert await store.load(actor, "other") is None
            assert await store.load(_actor(), "k") is None

    @_each_store
    async def it_forgets_a_record_only_when_it_is_not_later_than_the_drop(where: str, tmp_path: Path) -> None:
        async with _opened(where, tmp_path) as store:
            actor = _actor()
            await store.save(actor, "k", _version(2), None)
            await store.drop(actor, "k", _version(1))
            assert await store.load(actor, "k") == (_version(2), None)
            await store.drop(actor, "k", _version(2))
            assert await store.load(actor, "k") is None
            await store.drop(actor, "k", _version(3))
            await store.save(actor, "k", _version(1), b"again")
            assert await store.load(actor, "k") == (_version(1), b"again")

    @_each_store
    async def it_orders_versions_as_unsigned_bytes(where: str, tmp_path: Path) -> None:
        async with _opened(where, tmp_path) as store:
            actor = _actor()
            low, high = b"\x7f" + b"\xff" * 31, b"\x80" + b"\x00" * 31
            await store.save(actor, "k", high, b"high")
            await store.save(actor, "k", low, b"low")
            assert await store.load(actor, "k") == (high, b"high")

    @_each_store
    async def it_keeps_apart_keys_a_collation_or_a_separator_would_confuse(where: str, tmp_path: Path) -> None:
        async with _opened(where, tmp_path) as store:
            actor = _actor()
            keys = [(actor, "key"), (actor, "Key"), (actor, "kéy"), (actor, "key ")]
            keys += [(f"{actor}/a", "b"), (actor, "a/b")]
            for order, (owner, key) in enumerate(keys, start=1):
                await store.save(owner, key, _version(order), key.encode())
            for order, (owner, key) in enumerate(keys, start=1):
                assert await store.load(owner, key) == (_version(order), key.encode()), (owner, key)

    @_each_store
    async def it_keeps_long_keys_and_large_states(where: str, tmp_path: Path) -> None:
        async with _opened(where, tmp_path) as store:
            actor, key, state = _actor(), "k" * 10_000, bytes(range(256)) * 4096
            await store.save(actor, key, _version(1), state)
            assert await store.load(actor, key) == (_version(1), state)

    @_each_store
    async def it_keeps_the_greatest_of_saves_racing_for_one_key(where: str, tmp_path: Path) -> None:
        async with _opened(where, tmp_path) as store, _opened(where, tmp_path) as other:
            actor, orders = _actor(), list(range(1, 41))
            Random(7).shuffle(orders)
            async with asyncio.TaskGroup() as saving:
                for index, order in enumerate(orders):
                    saving.create_task((store, other)[index % 2].save(actor, "k", _version(order), b"%d" % order))
            assert await store.load(actor, "k") == (_version(40), b"40")

    @_each_store
    async def it_shares_its_records_with_every_store_on_the_same_database(where: str, tmp_path: Path) -> None:
        actor = _actor()
        async with _opened(where, tmp_path) as one, _opened(where, tmp_path) as other:
            assert isinstance(one, Store)
            await one.save(actor, "k", _version(1), b"one")
            assert await other.load(actor, "k") == (_version(1), b"one")
            await other.save(actor, "k", _version(3), b"three")
            await one.save(actor, "k", _version(2), b"late")
            assert await one.load(actor, "k") == (_version(3), b"three")

        async with _opened(where, tmp_path) as reopened:
            assert await reopened.load(actor, "k") == (_version(3), b"three")


def describe_the_sql_store() -> None:
    async def it_opens_its_database_only_once_entered(tmp_path: Path) -> None:
        path = tmp_path / "records.db"
        store = SQL(f"sqlite://{path}?mode=rwc")
        assert not path.exists()
        async with store:
            assert path.exists()

    def when_it_was_not_entered_or_was_left() -> None:
        async def it_refuses_every_call(tmp_path: Path) -> None:
            store = _opened("sqlite", tmp_path)
            with pytest.raises(RuntimeError, match="async with"):
                await store.load("a", "k")
            async with store:
                assert await store.load("a", "k") is None
            with pytest.raises(RuntimeError, match="async with"):
                await store.save("a", "k", _version(1), None)

    def when_its_url_names_a_database_it_does_not_know() -> None:
        async def it_refuses_to_open() -> None:
            with pytest.raises(ValueError, match="redis"):
                async with SQL("redis://localhost"):
                    pass

    def when_its_database_cannot_be_reached() -> None:
        async def it_raises_connection_error(tmp_path: Path) -> None:
            with pytest.raises(ConnectionError, match="could not open"):
                async with SQL(f"sqlite://{tmp_path / 'missing' / 'records.db'}?mode=rwc"):
                    pass


def describe_a_system_alone_on_a_store() -> None:
    def when_it_stops_and_another_starts_on_the_same_database() -> None:
        @_each_store
        async def it_takes_every_key_back_as_the_one_before_left_it(where: str, tmp_path: Path) -> None:
            keys = [f"{uuid4().hex}-{index}" for index in range(4)]
            async with _opened(where, tmp_path) as store, ActorSystem(store=store) as system:
                for key in keys:
                    await system.ref(tally, key).ask(Add(5))
                assert await system.ref(tally, keys[0]).ask(Forget()) is True

            async with _opened(where, tmp_path) as store, ActorSystem(store=store) as system:
                assert await system.ref(tally, keys[0]).ask(Total()) == 0
                for key in keys[1:]:
                    assert await system.ref(tally, key).ask(Add(1)) == 6


def describe_a_cluster_on_a_store() -> None:
    def when_every_node_stops_and_as_many_new_ones_start_on_its_database() -> None:
        @_each_store
        async def it_reads_every_durable_key_back_as_last_confirmed(where: str, tmp_path: Path) -> None:
            run = uuid4().hex
            keys = {f"{run}-{index}": (index, index + 100, index + 200) for index in range(24)}
            async with _opened(where, tmp_path) as store, Harness.start(3, store=store) as harness:
                for index, (key, entries) in enumerate(keys.items()):
                    ref = harness.nodes[index % 3].system.ref(durable_ledger, key)
                    for entry in entries:
                        assert await ref.ask(Append(entry))
                assert await harness.nodes[0].system.ref(ledger, "memory").ask(Append(1))

            # Leaving the harness stopped every node: no process holds a replica of any key any more.
            async with _opened(where, tmp_path) as store, Harness.start(3, store=store) as harness:
                for index, (key, entries) in enumerate(keys.items()):
                    listing = await harness.nodes[(index + 1) % 3].system.ref(durable_ledger, key).ask(Entries())
                    assert listing.entries == entries, key
                assert (await harness.nodes[0].system.ref(ledger, "memory").ask(Entries())).entries == ()

    def when_keys_were_written_while_a_node_was_down() -> None:
        @_each_store
        async def it_reads_them_back_once_every_node_crashed_and_new_ones_started(where: str, tmp_path: Path) -> None:
            run = uuid4().hex
            keys = [f"{run}-{index}" for index in range(12)]
            entries = count(1)
            attempted: dict[str, set[int]] = {key: set() for key in keys}
            confirmed: dict[str, set[int]] = {key: set() for key in keys}

            async def append(system: ActorSystem, key: str) -> None:
                """Append to `key` until an append is confirmed. An attempt that failed may have been applied."""

                async def appended() -> None:
                    entry = next(entries)
                    attempted[key].add(entry)
                    assert await system.ref(durable_ledger, key).ask(Append(entry))
                    confirmed[key].add(entry)

                await eventually(appended, WITHIN)

            async with _opened(where, tmp_path) as store, Harness.start(3, store=store) as harness:
                a, b, c = harness.nodes
                for key in keys[:8]:
                    await append(a.system, key)
                await harness.crash(c)
                # Until the others remove it, the keys `c` owned answer `Unavailable`, and `append` tries again.
                for key in keys:
                    await append(b.system, key)
                await harness.crash(a)
                await harness.crash(b)

            async with _opened(where, tmp_path) as store, Harness.start(3, store=store) as harness:
                for key in keys:
                    listing = await harness.nodes[0].system.ref(durable_ledger, key).ask(Entries())
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


def _opened(where: str, tmp_path: Path, /) -> SQL:
    """A store over the database `where` names: a SQLite file of the test, or the URL of another database."""
    return SQL(f"sqlite://{tmp_path / 'records.db'}?mode=rwc" if where == "sqlite" else where)


def _actor() -> str:
    """An actor no other test and no earlier run wrote, since a database of `CASTY_STORES` outlives them."""
    return f"tests.storage:{uuid4().hex}"


def _version(order: int, /) -> bytes:
    """A version of 32 bytes, as casty writes them, which sorts as `order` does."""
    return order.to_bytes(32, "big")
