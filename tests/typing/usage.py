"""Correct use of the public API. Input for pyright, never executed."""

from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Annotated, Never, assert_never, assert_type

from casty import (
    Activation,
    Actor,
    ActorSystem,
    Backoff,
    Client,
    Cluster,
    Collections,
    Compression,
    Context,
    DefaultedActor,
    Durable,
    Event,
    Limits,
    MemberChanged,
    MessageDropped,
    MessageTooLarge,
    NodeId,
    OnFull,
    Opaque,
    Placement,
    Ref,
    State,
    Stats,
    Store,
    System,
    actor,
)
from casty.collections import (
    MISSING,
    Acquired,
    Barrier,
    Counter,
    Denied,
    Dict,
    Lease,
    Lock,
    Missing,
    MultiMap,
    Queue,
    Register,
    Semaphore,
    SemaphoreState,
    Set,
    Status,
    semaphore,
)


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class Withdraw:
    reply_to: Ref[bool]
    amount: int


type AccountMsg = Deposit | Withdraw


@dataclass(frozen=True)
class Account:
    balance: int = 0


@actor(initial=Account())
async def account(ctx: Context[Account, AccountMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Deposit(amount):
                await ctx.state.set(replace(ctx.state.value, balance=ctx.state.value.balance + amount))
            case Withdraw(reply_to, amount):
                if amount > ctx.state.value.balance:
                    reply_to.tell(False)
                    continue
                await ctx.state.set(replace(ctx.state.value, balance=ctx.state.value.balance - amount))
                reply_to.tell(True)
            case _:
                assert_never(msg)


@dataclass(frozen=True)
class Pending:
    pass


@dataclass(frozen=True)
class Paid:
    amount: int


@dataclass(frozen=True)
class Pay:
    reply_to: Ref[bool]
    amount: int


@actor
async def paid(ctx: Context[Paid, Pay]) -> None:
    async for msg in ctx.inbox:
        msg.reply_to.tell(False)


@actor(replicas=5, write="all")
async def order(ctx: Context[Pending, Pay]) -> None:
    async for msg in ctx.inbox:
        await ctx.become(paid, Paid(msg.amount))
        msg.reply_to.tell(True)


@dataclass(frozen=True)
class Offset:
    value: int = 0


@dataclass(frozen=True)
class Record:
    offset: int


class Broker:
    async def stream(self, key: str, offset: int) -> AsyncIterator[Record]:
        yield Record(offset)


broker = Broker()


async def process(record: Record) -> None: ...


async def advanced(offset: Offset) -> Offset:
    return Offset(offset.value + 1)


async def state_types(ctx: Context[Offset]) -> None:
    assert_type(ctx.state, State[Offset])
    assert_type(ctx.state.value, Offset)
    assert_type(await ctx.state.update(lambda offset: Offset(offset.value + 1)), Offset)
    assert_type(await ctx.state.update(advanced), Offset)


@actor(initial=Offset(), mailbox=1_000, on_full="wait")
async def consumer(ctx: Context[Offset]) -> None:
    async for record in broker.stream(ctx.key, ctx.state.value.value):
        await process(record)
        await ctx.state.set(Offset(record.offset + 1))


assert_type(consumer.on_full, OnFull)


@actor(
    initial=Offset(),
    idle_after=timedelta(minutes=10),
    ask_timeout=timedelta(minutes=2),
    write_timeout=timedelta(seconds=30),
    backoff=Backoff(limit=timedelta(minutes=1)),
)
async def indexer(ctx: Context[Offset]) -> None:
    async for record in broker.stream(ctx.key, ctx.state.value.value):
        await process(record)
        await ctx.state.set(Offset(record.offset + 1))


assert_type(indexer.ask_timeout, timedelta | None)
assert_type(indexer.backoff, Backoff | None)


@dataclass(frozen=True)
class Tick:
    count: int


@dataclass(frozen=True)
class Subscribe:
    subscriber: Ref[Tick]


async def ticks() -> AsyncIterator[Tick]:
    yield Tick(1)


@actor(initial=Offset())
async def clock(ctx: Context[Offset, Subscribe]) -> None:
    subscribers: list[Ref[Tick]] = []
    async for item in ctx.merge(ticks()):
        match item:
            case Subscribe(subscriber):
                subscribers.append(subscriber)
            case Tick():
                for subscriber in subscribers:
                    subscriber.tell(item)
            case _:
                assert_never(item)


@actor(initial=Account())
async def auditor(ctx: Context[Account, Tick]) -> None:
    ctx.system.ref(clock, "main").tell(Subscribe(ctx.self))
    async for tick in ctx.inbox:
        assert_type(await ctx.system.ref(account, ctx.key).ask(Withdraw, tick.count), bool)


@actor(initial=tuple[str, ...](), replicas=3, write="majority")
async def journal(ctx: Context[tuple[str, ...], AccountMsg]) -> None:
    async for _ in ctx.inbox:
        await ctx.state.set((*ctx.state.value, "entry"))


@actor(initial=Offset(), durable="write")
async def cursor(ctx: Context[Offset, Tick]) -> None:
    async for tick in ctx.inbox:
        await ctx.state.set(Offset(ctx.state.value.value + tick.count))


@actor(initial=Offset(), durable=timedelta(seconds=5))
async def gauge(ctx: Context[Offset, Tick]) -> None:
    async for tick in ctx.inbox:
        await ctx.state.set(Offset(tick.count))


assert_type(cursor.durable, Durable | None)


class Files:
    """A store as a caller writes one: three coroutines over records of a version and a state."""

    async def load(self, actor: str, key: str, /) -> tuple[bytes, bytes | None] | None:
        return None

    async def save(self, actor: str, key: str, version: bytes, state: bytes | None, /) -> None: ...

    async def drop(self, actor: str, key: str, version: bytes, /) -> None: ...


def keeper() -> Store:
    return Files()


async def stored_types() -> None:
    async with ActorSystem(store=keeper()) as system:
        system.ref(cursor, "c-1").tell(Tick(1))
        system.ref(gauge, "g-1").tell(Tick(2))


async def main() -> None:
    assert_type(account, DefaultedActor[Account, AccountMsg])
    assert_type(order, Actor[Pending, Pay])
    assert_type(consumer, DefaultedActor[Offset, Never])
    assert_type(journal, DefaultedActor[tuple[str, ...], AccountMsg])

    limits = Limits(message=32 * 1024 * 1024)
    compression = Compression(min_bytes=16 * 1024)
    cluster = Cluster(
        bind="0.0.0.0:7400",
        advertise="10.0.0.5:7400",
        seeds=("10.0.0.4:7400",),
        limits=limits,
        compression=compression,
    )

    async with ActorSystem(cluster=cluster) as system:
        assert_type(system.ref(journal, "diary"), Ref[AccountMsg])
        acc = system.ref(account, "acc-1")
        assert_type(acc, Ref[AccountMsg])
        assert_type(await acc.ask(Withdraw, 30), bool)
        assert_type(await acc.ask(Withdraw, amount=30), bool)
        acc.tell(Deposit(10))
        try:
            await acc.ask(Withdraw, 30)
        except MessageTooLarge as refused:
            assert_type(refused, MessageTooLarge)
        assert_type(system.ref(order, "o-1", initial=Pending()), Ref[Pay])
        assert_type(system.ref(consumer, "orders-0"), Ref[Never])


class Alarms:
    """An observer that takes only the kinds of event it raises an alarm on."""

    def wants(self, kind: type[Event], /) -> bool:
        return kind in (MemberChanged, MessageDropped)

    def __call__(self, event: Event, /) -> None: ...


async def observed_types() -> None:
    async with ActorSystem(observer=Alarms()) as system:
        stats = system.stats()
        assert_type(stats, Stats)
        assert_type(stats.actors[account.name].deepest, int)
        assert_type(stats.writes_confirmed, int)
        listed = system.activations()
        assert_type(listed, tuple[Activation, ...])
        assert_type(listed[0].since, datetime)
        placed = await system.placement(account, "acc-1")
        assert_type(placed, Placement)
        assert_type(placed.owner, NodeId | None)
        assert_type(await system.release(order, "o-1"), bool)
    async with Client(seeds=("10.0.0.4:7400",)) as client:
        assert_type(client.stats().bytes_received, int)
        assert_type((await client.placement(account, "acc-1")).replicas, tuple[NodeId, ...])


async def collection_types(system: System) -> None:
    collections = Collections(system)
    entries = collections.dict("accounts", key=str, value=Account)
    assert_type(entries, Dict[str, Account])
    assert_type(await entries.get("one"), Account | Missing)
    await entries.put("one", Account(1))
    assert_type(await entries.items(), list[tuple[str, Account]])
    assert_type(entries.scan(), AsyncIterator[tuple[str, Account]])
    assert_type(collections.set("names", value=str).scan(), AsyncIterator[str])
    assert_type(collections.multimap("owners", key=str, value=Account).scan(), AsyncIterator[tuple[str, Account]])
    assert_type(collections.counter("visits"), Counter)
    register = collections.register("account", value=Account)
    assert_type(register, Register[Account])
    assert_type(await register.compare_and_set(MISSING, Account(1)), bool)
    assert_type(collections.set("names", value=str), Set[str])
    assert_type(collections.multimap("accounts", key=str, value=Account), MultiMap[str, Account])
    queue = collections.queue("accounts", value=Account)
    assert_type(queue, Queue[Account])
    assert_type(await queue.poll(), Account | Missing)
    assert_type(await queue.drain(2), list[Account])
    assert_type(collections.queue("batches", value=tuple[str, ...]), Queue[tuple[str, ...]])
    semaphore = collections.semaphore("workers", capacity=3)
    assert_type(semaphore, Semaphore)
    assert_type(await semaphore.try_acquire(), Lease | None)
    lease = await semaphore.acquire()
    assert_type(lease, Lease)
    assert_type(lease.release(), None)
    assert_type(collections.lock("resource"), Lock)
    assert_type(collections.barrier("round", parties=3), Barrier)

    async with Client(seeds=("10.0.0.4:7400",), limits=Limits(message=32 * 1024 * 1024)) as client:
        assert_type(await client.ref(account, "acc-1").ask(Withdraw, 30), bool)


@actor(initial=0)
async def pooled(ctx: Context[int, Deposit | Acquired | Denied]) -> None:
    pool = ctx.system.ref(semaphore.actor, "pool", initial=SemaphoreState(capacity=4))
    async for msg in ctx.inbox:
        match msg:
            case Deposit():
                pool.tell(semaphore.Acquire(ctx.self, n=2, ttl=10.0, wait=5.0))
            case Acquired(lease_id, token):
                assert_type(token, int)
                pool.tell(semaphore.Release(lease_id))
            case Denied():
                pass
    assert_type(await pool.ask(semaphore.Acquire, lease_id="mine"), Acquired | Denied)
    assert_type(await pool.ask(semaphore.Renew, "mine", 10.0), bool)
    assert_type(await pool.ask(semaphore.Get), Status)


def packed(numbers: list[int]) -> bytes:
    return bytes(numbers)


def unpacked(data: bytes) -> list[int]:
    return list(data)


type Numbers = Annotated[list[int], Opaque(encode=packed, decode=unpacked)]


@dataclass(frozen=True)
class Sketch:
    strokes: Numbers


@dataclass(frozen=True)
class Stroke:
    reply_to: Ref[Numbers]
    value: int


@actor(initial=Sketch([]))
async def sketch(ctx: Context[Sketch, Stroke]) -> None:
    async for msg in ctx.inbox:
        assert_type(ctx.state.value.strokes, list[int])
        strokes = [*ctx.state.value.strokes, msg.value]
        await ctx.state.set(Sketch(strokes))
        msg.reply_to.tell(strokes)


async def opaque_types(system: System) -> None:
    assert_type(Opaque(encode=packed, decode=unpacked), Opaque[list[int]])
    assert_type(await system.ref(sketch, "s-1").ask(Stroke, 3), list[int])
