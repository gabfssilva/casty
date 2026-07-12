"""Streaming RPC: actor methods that receive and/or return AsyncIterator (spec 07).

Server-streaming yields many elements from one call; client-streaming folds an
uploaded iterator into state and returns a scalar; duplex does both. Elements
cross the network lazily with end-to-end backpressure — a slow consumer throttles
the producer. A streaming method holds its actor for the stream's lifetime and
commits its state once at the end, exactly like a unary handler.

Run: uv run python examples/06_streaming.py
"""

import asyncio
import dataclasses
from collections.abc import AsyncIterator

import casty


@casty.actor
class Log:
    entries: list[str] = dataclasses.field(default_factory=list)

    async def append(self, line: str) -> int:
        self.entries.append(line)
        return len(self.entries)

    async def tail(self, since: int) -> AsyncIterator[str]:  # server-streaming
        for entry in self.entries[since:]:
            yield entry

    async def ingest(self, lines: AsyncIterator[str]) -> int:  # client-streaming
        count = 0
        async for line in lines:
            self.entries.append(line)
            count += 1
        return count

    async def lengths(self, lines: AsyncIterator[str]) -> AsyncIterator[int]:  # duplex
        async for line in lines:
            yield len(line)


async def _source(lines: list[str]) -> AsyncIterator[str]:
    for line in lines:
        yield line


async def main() -> None:
    async with casty.local() as system:
        log = system.actor(Log, "app:1")

        # client-streaming: upload an iterator, get a scalar back
        added = await log.ingest(_source(["boot", "ready", "serving"]))
        print(f"ingested {added} lines")

        # server-streaming: consume with `async for`
        print("tail from 1:")
        async for entry in log.tail(1):
            print(f"  {entry}")

        # duplex: iterator in, iterator out
        print("lengths:")
        async for n in log.lengths(_source(["a", "bb", "ccc"])):
            await asyncio.sleep(1)
            print(f"  {n}")


if __name__ == "__main__":
    asyncio.run(main())
