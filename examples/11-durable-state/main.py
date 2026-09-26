"""State that outlives every node. Run with `uv run main.py`.

A type declared with `durable="write"` is kept by the store of the system as well as by its replicas, and `state.set`
returns once the store has the write. The store here is `casty.sqlite.SQLiteStore`, one SQLite file shared by the
three nodes of this process; nodes on several machines would share a database they all reach instead. The nodes take
a few deposits and all stop, which loses every replica. Three new nodes start on the same file, and every account is
where the first three left it, while a type kept in memory only starts over.
"""

import asyncio
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import assert_never

from local_cluster import nodes

from casty import ActorSystem, Askable, Context, actor
from casty.sqlite import SQLiteStore

PORTS = (7451, 7452, 7453)


@dataclass(frozen=True)
class Deposit(Askable[int]):
    amount: int


@dataclass(frozen=True)
class Balance(Askable[int]):
    pass


type AccountMsg = Deposit | Balance


async def keep_balance(ctx: Context[int, AccountMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Deposit(amount, reply_to=reply_to):
                await ctx.state.set(ctx.state.value + amount)
                reply_to.tell(ctx.state.value)
            case Balance(reply_to=reply_to):
                reply_to.tell(ctx.state.value)
            case _:
                assert_never(msg)


# `durable` is per type. "write" saves every write before `state.set` returns, a store round trip per write; a
# `timedelta` saves the last write at most that late instead, and losing every replica loses at most that much.
@actor(initial=0, durable="write")
async def account(ctx: Context[int, AccountMsg]) -> None:
    await keep_balance(ctx)


# The same body kept in memory only, which is the default: a key goes with the last of its replicas.
@actor(initial=0)
async def tab(ctx: Context[int, AccountMsg]) -> None:
    await keep_balance(ctx)


async def cluster(store: SQLiteStore, work: Callable[[ActorSystem], Awaitable[None]], /) -> None:
    """Start three nodes on `store` together, hand the first one to `work`, then stop all three."""
    # Every node is given the store: a durable type does not activate on a node without one. The cluster is stopped
    # whole, with nobody left to take the keys of a leaving node, so the wait for someone to take them is kept short.
    async with nodes(PORTS, store=store, leave_timeout=timedelta(seconds=1)) as running:
        await work(running.systems[0])


async def deposit(system: ActorSystem, /) -> None:
    for name, amount in (("alice", 100), ("bob", 30), ("alice", -40)):
        print(f"account of {name}: {await system.ref(account, name).ask(Deposit(amount))}")
    print(f"tab of carol: {await system.ref(tab, 'carol').ask(Deposit(25))}")


async def balances(system: ActorSystem, /) -> None:
    for name in ("alice", "bob"):
        print(f"account of {name}: {await system.ref(account, name).ask(Balance())}")
    print(f"tab of carol: {await system.ref(tab, 'carol').ask(Balance())}")


async def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "state.db"
        async with SQLiteStore(path) as store:
            await cluster(store, deposit)
        print("every node stopped, and every replica with it; the file is what is left")
        # The file opened again, as new processes on this machine would.
        async with SQLiteStore(path) as store:
            await cluster(store, balances)


asyncio.run(main())
