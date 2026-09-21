"""Correct use of the public API. Input for the type checkers, never executed."""

from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from typing import Never, assert_never, assert_type

from casty import Actor, ActorSystem, Client, Cluster, Collections, Context, DefaultedActor, Ref, State, System, actor
from casty.collections import (
    MISSING,
    Barrier,
    Counter,
    Dict,
    Lease,
    Lock,
    Missing,
    MultiMap,
    Queue,
    Register,
    Semaphore,
    Set,
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


@actor(initial=Offset(), mailbox=1_000)
async def consumer(ctx: Context[Offset]) -> None:
    async for record in broker.stream(ctx.key, ctx.state.value.value):
        await process(record)
        await ctx.state.set(Offset(record.offset + 1))


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


async def main() -> None:
    assert_type(account, DefaultedActor[Account, AccountMsg])
    assert_type(order, Actor[Pending, Pay])
    assert_type(consumer, DefaultedActor[Offset, Never])
    assert_type(journal, DefaultedActor[tuple[str, ...], AccountMsg])

    cluster = Cluster(bind="0.0.0.0:7400", advertise="10.0.0.5:7400", seeds=("10.0.0.4:7400",))

    async with ActorSystem(cluster=cluster) as system:
        assert_type(system.ref(journal, "diary"), Ref[AccountMsg])
        acc = system.ref(account, "acc-1")
        assert_type(acc, Ref[AccountMsg])
        assert_type(await acc.ask(Withdraw, 30), bool)
        assert_type(await acc.ask(Withdraw, amount=30), bool)
        acc.tell(Deposit(10))
        assert_type(system.ref(order, "o-1", initial=Pending()), Ref[Pay])
        assert_type(system.ref(consumer, "orders-0"), Ref[Never])


async def collection_types(system: System) -> None:
    collections = Collections(system)
    entries = collections.dict("accounts", key=str, value=Account)
    assert_type(entries, Dict[str, Account])
    assert_type(await entries.get("one"), Account | Missing)
    await entries.put("one", Account(1))
    assert_type(await entries.items(), list[tuple[str, Account]])
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
    assert_type(await semaphore.acquire(), Lease)
    assert_type(collections.lock("resource"), Lock)
    assert_type(collections.barrier("round", parties=3), Barrier)

    async with Client(seeds=("10.0.0.4:7400",)) as client:
        assert_type(await client.ref(account, "acc-1").ask(Withdraw, 30), bool)
