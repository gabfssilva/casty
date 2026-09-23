"""Whether adding a key to a dict or a set costs the same however many keys it already holds.

The index of a dict, a set or a multimap keeps each shard in segments of about 512 keys, and a write rewrites one
segment, not the shard. This grows a dict and a set in one process and, each time one reaches a checkpoint (1k, 10k,
100k, ... keys), times the next `sample` additions and counts the bytes of state they wrote. Both stay flat once every
shard holds more than one segment's worth of keys; with the shard in one page they grew with it.

Run from the repository root:

    python -m benchmarks.index --keys 1000000 --output benchmarks/results/index.json
"""

import argparse
import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from casty import ActorSystem, Collections


@dataclass(frozen=True)
class Checkpoint:
    collection: str
    keys: int
    sample: int
    micros_per_add: float
    bytes_per_add: float


async def fill(
    add: Callable[[int], Awaitable[object]], keys: range, payloads: list[bytes], *, concurrency: int
) -> None:
    """Add every key of `keys`, `concurrency` at a time, forgetting the pages written meanwhile."""

    async def worker(first: int) -> None:
        for key in range(first, keys.stop, concurrency):
            await add(key)
            payloads.clear()

    async with asyncio.TaskGroup() as group:
        for first in range(keys.start, min(keys.start + concurrency, keys.stop)):
            group.create_task(worker(first))


async def grow(
    collection: str,
    add: Callable[[int], Awaitable[object]],
    payloads: list[bytes],
    *,
    keys: int,
    sample: int,
    concurrency: int,
) -> list[Checkpoint]:
    """Fill up to each checkpoint, then time the next `sample` additions one after the other."""
    checkpoints: list[Checkpoint] = []
    filled = 0
    at = 1_000
    while at <= keys:
        await fill(add, range(filled, at), payloads, concurrency=concurrency)
        payloads.clear()
        started = time.perf_counter()
        for key in range(at, at + sample):
            await add(key)
        elapsed = time.perf_counter() - started
        checkpoints.append(Checkpoint(collection, at, sample, elapsed / sample * 1e6, sum(map(len, payloads)) / sample))
        payloads.clear()
        filled = at + sample
        at *= 10
    return checkpoints


async def run(*, keys: int, sample: int, concurrency: int, shards: int) -> list[Checkpoint]:
    async with ActorSystem() as system:
        payloads = system._writes().payloads
        collections = Collections(system)
        entries = collections.dict("benchmark", key=int, value=int, index_shards=shards)
        members = collections.set("benchmark", value=int, shards=shards)

        async def put(key: int) -> None:
            await entries.put(key, key)

        return [
            *await grow("dict", put, payloads, keys=keys, sample=sample, concurrency=concurrency),
            *await grow("set", members.add, payloads, keys=keys, sample=sample, concurrency=concurrency),
        ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--keys", type=int, default=100_000, help="The last checkpoint the collections grow to")
    parser.add_argument("--sample", type=int, default=1_000, help="Additions timed at each checkpoint")
    parser.add_argument("--concurrency", type=int, default=256, help="Additions in flight while filling")
    parser.add_argument("--shards", type=int, default=16, help="Index shards of each collection")
    parser.add_argument("--output", type=Path, default=None, help="Where to write the checkpoints as JSON")
    options = parser.parse_args()
    checkpoints = asyncio.run(
        run(keys=options.keys, sample=options.sample, concurrency=options.concurrency, shards=options.shards)
    )
    for checkpoint in checkpoints:
        print(
            f"{checkpoint.collection} at {checkpoint.keys:,} keys: {checkpoint.micros_per_add:,.0f} us "
            f"and {checkpoint.bytes_per_add:,.0f} bytes written per addition"
        )
    if options.output is not None:
        options.output.parent.mkdir(parents=True, exist_ok=True)
        options.output.write_text(
            json.dumps(
                {
                    "at": datetime.now(UTC).isoformat(),
                    "shards": options.shards,
                    "checkpoints": [asdict(checkpoint) for checkpoint in checkpoints],
                },
                indent=2,
            )
            + "\n"
        )
        print(f"written to {options.output}")


if __name__ == "__main__":
    main()
