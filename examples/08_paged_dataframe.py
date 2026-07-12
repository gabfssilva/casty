"""Paged state: a DataFrame of hundreds of megabytes, updated one cell at a time.

A replicated actor normally ships its whole state on every mutation — fine for a
few kilobytes, hopeless for a frame this size. `casty.paged(PandasPager())` swaps
that for a pager: it diffs the frame and hands the framework only the pages that
moved. A page is one Arrow block — one column, 16.384 rows.

Editing one cell of a 305 MiB frame therefore puts ~128 KiB on the wire. The pager
finds that cell with copy-on-write: pandas copies a block on the first write through
*any* path, because the check lives in the block manager — so a moved buffer is the
signal, and there is no write it can miss.

All five nodes run in this one process, so it holds the live frame plus the three
replicas' pages: budget ~3 GiB of RAM and half a minute.

Needs the pandas extra:
  uv run --extra pandas python examples/08_paged_dataframe.py
"""

import asyncio
import time
from typing import NamedTuple

import numpy as np
import pandas as pd

import casty
from casty.pagers import PandasPager

ROWS = 10_000_000
SENSORS = 4
ACTOR = "demo.Telemetry"
KEY = "site-1"
MIB = 1024 * 1024

# the *first* commit ships the whole frame and takes ~12s — longer than the 10s a caller
# waits for a handler by default. Every commit after it is a page or two, and instant.
CONFIG = casty.Config(call_timeout=60.0)


@casty.actor(name=ACTOR, replicas=3, write=casty.MAJORITY)
class Telemetry:
    readings: pd.DataFrame = casty.paged(PandasPager())
    revision: int = 0  # an ordinary integral field, committed alongside the frame

    async def seed(self, rows: int, sensors: int) -> None:
        rng = np.random.default_rng(0)  # random, so nothing compresses away
        self.readings = pd.DataFrame({f"s{i}": rng.random(rows) for i in range(sensors)})

    async def correct(self, row: int, sensor: str, value: float) -> None:
        self.readings.loc[row, sensor] = value
        self.revision += 1

    async def revisions(self) -> int:
        return self.revision


class Held(NamedTuple):
    """What one node's replica store holds, and how much of it the last commit wrote."""

    pages: int
    size: int
    fresh: int
    fresh_size: int


def held(system: casty.Node) -> Held | None:
    """Peek at the node's replica store. Internal API on purpose: the point of the
    example is to show what the framework actually shipped, page by page."""
    stored = system._replication.store.get(ACTOR, KEY)
    if stored is None:
        return None
    fresh = [page for page in stored.pages.values() if page.hlc == stored.hlc]
    return Held(
        pages=len(stored.pages),
        size=sum(len(page.data) for page in stored.pages.values()),
        fresh=len(fresh),
        fresh_size=sum(len(page.data) for page in fresh),
    )


def rebuild(system: casty.Node) -> pd.DataFrame:
    """The frame as this replica holds it — through the pager, the same way an
    activation on this node would rebuild it."""
    stored = system._replication.store.get(ACTOR, KEY)
    assert stored is not None
    return PandasPager().restore(
        {
            page_key.removeprefix("readings/"): page.data
            for page_key, page in stored.pages.items()
            if page_key.startswith("readings/")
        }
    )


async def main() -> None:
    systems = [await casty.start("127.0.0.1:7141", config=CONFIG)]
    for port in range(7142, 7146):
        systems.append(
            await casty.start(f"127.0.0.1:{port}", seeds=["127.0.0.1:7141"], config=CONFIG)
        )
    while any(len(s.membership.alive_members()) < len(systems) for s in systems):
        await asyncio.sleep(0.05)
    print(f"cluster up: {len(systems)} nodes\n")
    print(casty.explain(Telemetry), "\n")

    site = systems[0].actor(Telemetry, KEY)

    megabytes = ROWS * SENSORS * 8 / MIB
    print(f"seeding {ROWS:,} x {SENSORS} float64 ({megabytes:.0f} MiB)...")
    clock = time.perf_counter()
    await site.seed(ROWS, SENSORS)
    seeding = time.perf_counter() - clock

    holders = [(system, page_set) for system in systems if (page_set := held(system)) is not None]
    first = holders[0][1]
    print(
        f"  first commit: {first.size / MIB:>7.1f} MiB in {first.pages:>4} pages"
        f"  -> {len(holders)} of {len(systems)} nodes, {seeding:.1f}s\n"
    )

    row, sensor = ROWS // 2, "s0"
    print(f"correcting readings.loc[{row:,}, {sensor!r}]...")
    clock = time.perf_counter()
    await site.correct(row, sensor, 42.0)
    editing = time.perf_counter() - clock

    after = [page_set for system in systems if (page_set := held(system)) is not None]
    delta = after[0]
    print(
        f"  this commit:  {delta.fresh_size / MIB:>7.3f} MiB in {delta.fresh:>4} pages"
        f"  -> {len(after)} of {len(systems)} nodes, {editing:.3f}s\n"
    )
    print(
        f"  {first.size / delta.fresh_size:,.0f}x less data, {seeding / editing:,.0f}x"
        f" less time. The other {delta.pages - delta.fresh:,} pages kept their old HLC:"
        f" one 16.384-row block of {sensor}, plus the `revision` int, is all that moved."
    )
    assert await site.revisions() == 1

    # what a cold activation on another node pays: the pages are there, but the frame
    # still has to be rebuilt out of them
    ring = systems[0].ring
    assert ring is not None
    owner = ring.owner(f"{ACTOR}/{KEY}")
    replica = next(system for system, _ in holders if system.node_id != owner)
    clock = time.perf_counter()
    restored = rebuild(replica)
    print(
        f"\n  a replica rebuilds {restored.shape[0]:,} x {restored.shape[1]} from its pages"
        f" in {time.perf_counter() - clock:.1f}s, new cell included:"
        f" {float(restored[sensor].to_numpy()[row])}"
    )

    # a graceful leave hands the pages off to the ring without this node, so shutting the
    # holders down pushes the frame around one more time — most of this example's wall clock
    print("\n  shutting down (each holder hands its pages off before it leaves)...")
    for system in systems:
        await system.close()


if __name__ == "__main__":
    asyncio.run(main())
