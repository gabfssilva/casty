"""Actors used by more than one test file."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Never, assert_never

from casty import Askable, Context, NodeId, actor


@dataclass(frozen=True)
class Account:
    balance: int = 0


@dataclass(frozen=True)
class Deposit(Askable[int]):
    amount: int


@dataclass(frozen=True)
class Balance(Askable[int]):
    pass


@dataclass(frozen=True)
class Location:
    balance: int
    node: NodeId


@dataclass(frozen=True)
class Where(Askable[Location]):
    pass


type AccountMsg = Deposit | Balance | Where


@actor(initial=Account())
async def account(ctx: Context[Account, AccountMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Deposit(amount, reply_to=reply_to):
                await ctx.state.set(Account(ctx.state.value.balance + amount))
                reply_to.tell(ctx.state.value.balance)
            case Balance(reply_to=reply_to):
                reply_to.tell(ctx.state.value.balance)
            case Where(reply_to=reply_to):
                reply_to.tell(Location(ctx.state.value.balance, ctx.system.node))
            case _:
                assert_never(msg)


@dataclass(frozen=True)
class Pending:
    pass


@dataclass(frozen=True)
class Paid:
    amount: int


type Order = Pending | Paid


@dataclass(frozen=True)
class Pay(Askable[bool]):
    amount: int


@actor
async def paid(ctx: Context[Paid, Pay]) -> None:
    async for msg in ctx.inbox:
        msg.reply_to.tell(False)


@actor
async def order(ctx: Context[Pending, Pay]) -> None:
    async for msg in ctx.inbox:
        await ctx.become(paid, Paid(msg.amount))
        msg.reply_to.tell(True)


@dataclass(frozen=True)
class Ledger:
    entries: tuple[int, ...] = ()


@dataclass(frozen=True)
class Append(Askable[bool]):
    entry: int


@dataclass(frozen=True)
class Listing:
    entries: tuple[int, ...]
    node: NodeId


@dataclass(frozen=True)
class Entries(Askable[Listing]):
    pass


type LedgerMsg = Append | Entries


async def _entries(ctx: Context[Ledger, LedgerMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Append(entry, reply_to=reply_to):
                await ctx.state.set(Ledger((*ctx.state.value.entries, entry)))
                reply_to.tell(True)
            case Entries(reply_to=reply_to):
                reply_to.tell(Listing(ctx.state.value.entries, ctx.system.node))
            case _:
                assert_never(msg)


# A type is named after its body, so two types take two bodies, even when they do the same.
@actor(initial=Ledger())
async def ledger(ctx: Context[Ledger, LedgerMsg]) -> None:
    await _entries(ctx)


@actor(initial=Ledger(), write="one")
async def loose(ctx: Context[Ledger, LedgerMsg]) -> None:
    """A ledger that confirms with one replica: it keeps writing on the minority side of a partition."""
    await _entries(ctx)


@actor(initial=Ledger(), durable="write")
async def durable_ledger(ctx: Context[Ledger, LedgerMsg]) -> None:
    """A ledger its store keeps too: an entry it confirmed outlives every replica of its key, and every node."""
    await _entries(ctx)


@dataclass(frozen=True)
class Notebook:
    notes: tuple[int, ...] = ()


@dataclass(frozen=True)
class Note:
    value: int


@dataclass(frozen=True)
class Written:
    notes: tuple[int, ...]
    node: NodeId


@dataclass(frozen=True)
class Notes(Askable[Written]):
    pass


type NotesMsg = Note | Notes


@actor(initial=Notebook())
async def notes(ctx: Context[Notebook, NotesMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Note(value):
                await ctx.state.set(Notebook((*ctx.state.value.notes, value)))
            case Notes(reply_to=reply_to):
                reply_to.tell(Written(ctx.state.value.notes, ctx.system.node))
            case _:
                assert_never(msg)


class Feed:
    """Records for the consumers to read, and where each record was processed.

    One feed for the process, shared by every node of a harness: the consumers read from it wherever they run, so what
    it kept is how a test sees a partition move from one machine to another.
    """

    def __init__(self) -> None:
        self._last = 0
        self._pace = 0.0
        self._log: list[tuple[str, int, NodeId]] = []

    @property
    def log(self) -> tuple[tuple[str, int, NodeId], ...]:
        """Every record processed, in order, as `(partition, offset, node)`."""
        return tuple(self._log)

    def reset(self, *, last: int, pace: timedelta) -> None:
        self._last = last
        self._pace = pace.total_seconds()
        self._log.clear()

    def processed(self, partition: str, offset: int, node: NodeId) -> None:
        self._log.append((partition, offset, node))

    async def records(self, start: int) -> AsyncIterator[int]:
        for offset in range(start, self._last + 1):
            await asyncio.sleep(self._pace)
            yield offset


FEED = Feed()


@dataclass(frozen=True)
class Cursor:
    offset: int = -1


@actor(initial=Cursor())
async def consumer(ctx: Context[Cursor, Never]) -> None:
    """Reads the feed from the offset after the last saved one, and takes no message at all.

    It records the offset before saving it, so a machine that dies in between makes the next one process that offset
    again. That is the at-least-once of a consumer, and it is never a gap.
    """
    async for offset in ctx.merge(FEED.records(ctx.state.value.offset + 1)):
        FEED.processed(ctx.key, offset, ctx.system.node)
        await ctx.state.set(Cursor(offset))


@dataclass(frozen=True)
class Gate:
    pass


@dataclass(frozen=True)
class Locate(Askable[NodeId]):
    pass


@dataclass(frozen=True)
class Hold(Askable[bool]):
    pass


type GateMsg = Locate | Hold


@actor(initial=Gate())
async def gate(ctx: Context[Gate, GateMsg]) -> None:
    """Says where it is, and never answers `Hold`: an `ask` that stays in flight until something else ends it."""
    async for msg in ctx.inbox:
        match msg:
            case Locate(reply_to=reply_to):
                reply_to.tell(ctx.system.node)
            case Hold():
                await asyncio.Event().wait()
            case _:
                assert_never(msg)


@dataclass(frozen=True)
class Idle:
    pass


@dataclass(frozen=True)
class Nap:
    length: timedelta


@actor(initial=Idle(), mailbox=1)
async def sleepy(ctx: Context[Idle, Nap]) -> None:
    """Sleeps as long as each message says, with room for one message behind the one it sleeps on."""
    async for msg in ctx.inbox:
        await asyncio.sleep(msg.length.total_seconds())


@dataclass(frozen=True)
class Touch(Askable[bool]):
    pass


@actor(initial=Idle())
async def touched(ctx: Context[Idle, Touch]) -> None:
    async for msg in ctx.inbox:
        msg.reply_to.tell(True)


@dataclass(frozen=True)
class Latch:
    """What a body of `gated` holds each message on: `held` once it took one, and it goes on once `released`."""

    held: asyncio.Event = field(default_factory=asyncio.Event)
    released: asyncio.Event = field(default_factory=asyncio.Event)


LATCHES: dict[str, Latch] = {}
"""The latch of each key of `gated`, which a test puts in place before it sends to the key."""


@dataclass(frozen=True)
class Bump(Askable[int]):
    pass


@actor(initial=0)
async def gated(ctx: Context[int, Bump]) -> None:
    """Counts its messages in its state, each one once the latch of its key is released, and answers the count."""
    async for msg in ctx.inbox:
        latch = LATCHES[ctx.key]
        latch.held.set()
        await latch.released.wait()
        await ctx.state.set(ctx.state.value + 1)
        msg.reply_to.tell(ctx.state.value)
