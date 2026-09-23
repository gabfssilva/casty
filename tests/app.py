"""Actors used by more than one test file."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Never, assert_never

from casty import Context, NodeId, Ref, actor


@dataclass(frozen=True)
class Account:
    balance: int = 0


@dataclass(frozen=True)
class Deposit:
    reply_to: Ref[int]
    amount: int


@dataclass(frozen=True)
class Balance:
    reply_to: Ref[int]


@dataclass(frozen=True)
class Location:
    balance: int
    node: NodeId


@dataclass(frozen=True)
class Where:
    reply_to: Ref[Location]


type AccountMsg = Deposit | Balance | Where


@actor(initial=Account())
async def account(ctx: Context[Account, AccountMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Deposit(reply_to, amount):
                await ctx.state.set(Account(ctx.state.value.balance + amount))
                reply_to.tell(ctx.state.value.balance)
            case Balance(reply_to):
                reply_to.tell(ctx.state.value.balance)
            case Where(reply_to):
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
class Pay:
    reply_to: Ref[bool]
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
class Append:
    reply_to: Ref[bool]
    entry: int


@dataclass(frozen=True)
class Listing:
    entries: tuple[int, ...]
    node: NodeId


@dataclass(frozen=True)
class Entries:
    reply_to: Ref[Listing]


type LedgerMsg = Append | Entries


async def _entries(ctx: Context[Ledger, LedgerMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Append(reply_to, entry):
                await ctx.state.set(Ledger((*ctx.state.value.entries, entry)))
                reply_to.tell(True)
            case Entries(reply_to):
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
class Notes:
    reply_to: Ref[Written]


type NotesMsg = Note | Notes


@actor(initial=Notebook())
async def notes(ctx: Context[Notebook, NotesMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Note(value):
                await ctx.state.set(Notebook((*ctx.state.value.notes, value)))
            case Notes(reply_to):
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
class Locate:
    reply_to: Ref[NodeId]


@dataclass(frozen=True)
class Hold:
    reply_to: Ref[bool]


type GateMsg = Locate | Hold


@actor(initial=Gate())
async def gate(ctx: Context[Gate, GateMsg]) -> None:
    """Says where it is, and never answers `Hold`: an `ask` that stays in flight until something else ends it."""
    async for msg in ctx.inbox:
        match msg:
            case Locate(reply_to):
                reply_to.tell(ctx.system.node)
            case Hold():
                await asyncio.Event().wait()
            case _:
                assert_never(msg)
