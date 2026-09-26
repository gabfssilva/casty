"""The two costs the macro benchmark hides: turning a value into bytes, and crossing the event loop.

The performance run (`reliability/performance.py`) times a whole `ask`, where serialization and scheduling are mixed
with the transport, the replication and the network. These two are what the core changes on its own, so they are
measured apart and compared before and after the port. A whole `ask` on one node, with neither transport nor replicas,
is measured too: it is what the bookkeeping of the core around every message, such as the chain of an `ask`, adds to.
So is an `ask` that starts and ends an activation, which is what the reports of a node to its observer are frequent
enough to weigh on.

Run from the repository root:

    python -m benchmarks.micro --output benchmarks/results/micro.json
"""

import argparse
import asyncio
import json
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from benchmarks.actors import Increment, Read, counter
from casty import ActorSystem, Ref


@dataclass(frozen=True)
class Measurement:
    name: str
    operations: int
    elapsed: float
    per_second: float
    nanos_per_operation: float

    @classmethod
    def of(cls, name: str, operations: int, elapsed: float, /) -> "Measurement":
        return cls(name, operations, elapsed, operations / elapsed, elapsed / operations * 1e9)


def measure(name: str, once: Callable[[], object], /, *, duration: float, batch: int = 1_000) -> Measurement:
    """Run `once` in batches until `duration` has passed, so that the clock is read once per batch."""
    started = time.perf_counter()
    deadline = started + duration
    operations = 0
    while time.perf_counter() < deadline:
        for _ in range(batch):
            once()
        operations += batch
    return Measurement.of(name, operations, time.perf_counter() - started)


async def measure_hop(name: str, /, *, duration: float) -> Measurement:
    """What one message costs the core before the body sees it: waking the loop that owns a future from another thread.

    The core resolves every future from its own runtime thread, which is alive for the life of the node, so the thread
    here is started once and the measurement is the round trip, not the thread.
    """
    loop = asyncio.get_running_loop()
    handed: list[asyncio.Future[None]] = []
    ready = threading.Semaphore(0)
    done = threading.Event()

    def resolve() -> None:
        while True:
            ready.acquire()
            if done.is_set():
                return
            loop.call_soon_threadsafe(handed.pop().set_result, None)

    worker = threading.Thread(target=resolve)
    worker.start()
    try:
        started = time.perf_counter()
        deadline = started + duration
        hops = 0
        while time.perf_counter() < deadline:
            pending = loop.create_future()
            handed.append(pending)
            ready.release()
            await pending
            hops += 1
        elapsed = time.perf_counter() - started
    finally:
        done.set()
        ready.release()
        worker.join()
    return Measurement.of(name, hops, elapsed)


async def measure_ask(name: str, ref: Ref[Read], /, *, duration: float, batch: int = 100) -> Measurement:
    """A whole `ask` of a key on this node from outside any body, one after the other."""
    started = time.perf_counter()
    deadline = started + duration
    asks = 0
    while time.perf_counter() < deadline:
        for _ in range(batch):
            await ref.ask(Read(b""))
        asks += batch
    return Measurement.of(name, asks, time.perf_counter() - started)


async def measure_activation(name: str, /, *, duration: float, batch: int = 100) -> Measurement:
    """A whole `ask` of a key that never ran, on a system whose keys idle out at once, with the default observer.

    Every one starts an activation and ends it: the two events a node reports most often. The default observer takes
    them only when the `casty` logger takes debug records, which it does not here.
    """
    async with ActorSystem(idle_after=timedelta(0)) as system:
        started = time.perf_counter()
        deadline = started + duration
        asks = 0
        while time.perf_counter() < deadline:
            for index in range(asks, asks + batch):
                await system.ref(counter, f"k-{index}").ask(Read(b""))
            asks += batch
        return Measurement.of(name, asks, time.perf_counter() - started)


async def run(*, duration: float) -> tuple[Measurement, ...]:
    async with ActorSystem() as system:
        # A ref of a running node, which is what a message that carries a `reply_to` holds.
        ref = system.ref(counter, "k")
        encode = system._encode
        decode = system._decode
        messages = system._schema(Increment)
        states = system._schema(int)

        small = Increment(b"", reply_to=ref)
        large = Increment(b"x" * 1024, reply_to=ref)
        wire = {
            "message": encode(messages, small),
            "message-1kib": encode(messages, large),
            "state": encode(states, 7),
        }

        return (
            measure("encode-message", lambda: encode(messages, small), duration=duration),
            measure("encode-message-1kib", lambda: encode(messages, large), duration=duration),
            measure("encode-state", lambda: encode(states, 7), duration=duration),
            measure("decode-message", lambda: decode(messages, wire["message"]), duration=duration),
            measure("decode-message-1kib", lambda: decode(messages, wire["message-1kib"]), duration=duration),
            measure("decode-state", lambda: decode(states, wire["state"]), duration=duration),
            await measure_hop("loop-hop", duration=duration),
            await measure_ask("ask", ref, duration=duration),
            await measure_activation("activate", duration=duration),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=1.0, help="Measured seconds per case")
    parser.add_argument("--output", type=Path, default=None, help="Where to write the measurements as JSON")
    options = parser.parse_args()
    measurements = asyncio.run(run(duration=options.duration))
    for measurement in measurements:
        print(f"{measurement.name}: {measurement.per_second:,.0f}/s, {measurement.nanos_per_operation:,.0f} ns")
    if options.output is not None:
        options.output.parent.mkdir(parents=True, exist_ok=True)
        options.output.write_text(
            json.dumps(
                {
                    "at": datetime.now(UTC).isoformat(),
                    "duration": options.duration,
                    "measurements": [asdict(measurement) for measurement in measurements],
                },
                indent=2,
            )
            + "\n"
        )
        print(f"written to {options.output}")


if __name__ == "__main__":
    main()
