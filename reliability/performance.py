"""Throughput and latency of a cluster on Kubernetes, under closed-loop load.

    make performance

A scenario is `clients x servers`: `servers` nodes, each in a pod of its own with the periods casty has by default, and
`clients` pods of load, each a `Client` of the nodes. For each actor type, concurrency, operation and repeat, a round
warms every client up and then has them all call for `duration` seconds at once, each with `concurrency` calls in
flight: a call starts when the one before it on its lane answers. A round reports the calls per second of its clients
together, counting the calls started before the end that answer after it, and the percentiles of the latency of the
calls that succeeded; a call that ends in `Unavailable`, `TimeoutError` or `MailboxFull` is an error. `counter` keeps
its state in memory, on 3 replicas with majority writes; `durable` also writes every increment to the PostgreSQL of the
run before it answers.

`PERFORMANCE_SCENARIOS`, `PERFORMANCE_ACTORS`, `PERFORMANCE_OPERATIONS` and `PERFORMANCE_CONCURRENCY`, lists separated
by commas, and `PERFORMANCE_DURATION`, `PERFORMANCE_WARMUP`, `PERFORMANCE_REPEATS` and `PERFORMANCE_KEYS` shape the run
(`Settings.environment`). The output directory keeps `settings.json`, `summary.jsonl`, a row per round, and the log of
every pod.

    python -m reliability.performance '["slot-0.casty:7400", ...]'

is the process of a pod of load: a `Client` of the seeds it is given, which says `{"ready": null}` once it is connected
and is then driven on its control port. A round is one connection: `{"prepare": <load>}` warms the client up and is
answered with `{}`, and `{"go": null}` then runs the load and is answered with its `Sample`.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import signal
import statistics
import sys
import time
from collections import Counter as Tally
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, replace
from datetime import timedelta
from functools import partial
from itertools import count, product
from pathlib import Path
from typing import Literal

from casty import Client, DefaultedActor, MailboxFull, Ref, Unavailable
from reliability import records
from reliability.actors import CounterMsg, Increment, Read, counter, durable_counter
from reliability.node import CONTROL, Launch, View, say
from reliability.site import Kubernetes, Pod, Stuck
from reliability.support import eventually

SCENARIOS: Mapping[str, tuple[int, int]] = {
    "1x1": (1, 1),
    "1x3": (1, 3),
    "1x10": (1, 10),
    "3x10": (3, 10),
    "5x50": (5, 50),
}
"""The scenarios by name, `clients x servers`, as the number of each."""

ACTORS: Mapping[str, DefaultedActor[int, CounterMsg]] = {"counter": counter, "durable": durable_counter}

type Operation = Literal["read", "increment"]


def operation(raw: object) -> Operation:
    match records.text(raw):
        case "read":
            return "read"
        case "increment":
            return "increment"
        case other:
            raise ValueError(f"unknown operation {other!r}")


@dataclass(frozen=True)
class Settings:
    """What a performance run is made of: every scenario, actor type, concurrency, operation and repeat in turn, a
    round of `duration` seconds after `warmup` seconds of the same load, over `keys` keys."""

    scenarios: tuple[str, ...] = tuple(SCENARIOS)
    actors: tuple[str, ...] = tuple(ACTORS)
    operations: tuple[Operation, ...] = ("read", "increment")
    concurrency: tuple[int, ...] = (1, 8, 32)
    duration: float = 5.0
    warmup: float = 2.0
    repeats: int = 3
    keys: int = 1000

    def __post_init__(self) -> None:
        if unknown := set(self.scenarios) - SCENARIOS.keys():
            raise ValueError(f"unknown scenarios {sorted(unknown)}; there are {', '.join(SCENARIOS)}")
        if unknown := set(self.actors) - ACTORS.keys():
            raise ValueError(f"unknown actors {sorted(unknown)}; there are {', '.join(ACTORS)}")
        if min(self.duration, self.warmup, self.repeats, self.keys, *self.concurrency) <= 0:
            raise ValueError("duration, warmup, repeats, keys and concurrency must be positive")

    @property
    def slots(self) -> int:
        """The most servers a scenario of the run starts."""
        return max(SCENARIOS[scenario][1] for scenario in self.scenarios)

    @classmethod
    def environment(cls, environ: Mapping[str, str], /) -> Settings:
        """The settings the `PERFORMANCE_*` variables ask for, and the defaults for the rest."""
        default = cls()
        return cls(
            scenarios=_names(environ.get("PERFORMANCE_SCENARIOS"), default.scenarios),
            actors=_names(environ.get("PERFORMANCE_ACTORS"), default.actors),
            operations=tuple(
                operation(name) for name in _names(environ.get("PERFORMANCE_OPERATIONS"), default.operations)
            ),
            concurrency=tuple(
                int(n) for n in _names(environ.get("PERFORMANCE_CONCURRENCY"), tuple(map(str, default.concurrency)))
            ),
            duration=float(environ.get("PERFORMANCE_DURATION", default.duration)),
            warmup=float(environ.get("PERFORMANCE_WARMUP", default.warmup)),
            repeats=int(environ.get("PERFORMANCE_REPEATS", default.repeats)),
            keys=int(environ.get("PERFORMANCE_KEYS", default.keys)),
        )

    def encoded(self) -> dict[str, object]:
        return {
            "scenarios": list(self.scenarios),
            "actors": list(self.actors),
            "operations": list(self.operations),
            "concurrency": list(self.concurrency),
            "duration": self.duration,
            "warmup": self.warmup,
            "repeats": self.repeats,
            "keys": self.keys,
        }

    @classmethod
    def decoded(cls, raw: object, /) -> Settings:
        held = records.record(raw)
        return cls(
            scenarios=records.texts(held["scenarios"]),
            actors=records.texts(held["actors"]),
            operations=tuple(operation(name) for name in records.items(held["operations"])),
            concurrency=tuple(records.integer(n) for n in records.items(held["concurrency"])),
            duration=records.number(held["duration"]),
            warmup=records.number(held["warmup"]),
            repeats=records.integer(held["repeats"]),
            keys=records.integer(held["keys"]),
        )


@dataclass(frozen=True)
class Load:
    """What one client does in a round: `concurrency` lanes of calls of `operation` on the keys of `actor`, the lane
    `n` starting at the key `first + n` and moving on by `concurrency` keys at each call."""

    actor: str
    operation: Operation
    keys: int
    concurrency: int
    warmup: float
    duration: float
    first: int = 0

    def encoded(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def decoded(cls, raw: object, /) -> Load:
        held = records.record(raw)
        return cls(
            records.text(held["actor"]),
            operation(held["operation"]),
            records.integer(held["keys"]),
            records.integer(held["concurrency"]),
            records.number(held["warmup"]),
            records.number(held["duration"]),
            records.integer(held["first"]),
        )


@dataclass(frozen=True)
class Sample:
    """What the round of one client came to: how many calls succeeded in each microsecond of latency, how many ended
    in an error, and the seconds from its start to the end of its last call."""

    latencies: Mapping[int, int]
    errors: int
    elapsed: float

    def encoded(self) -> dict[str, object]:
        latencies = [[micros, n] for micros, n in self.latencies.items()]
        return {"latencies": latencies, "errors": self.errors, "elapsed": self.elapsed}

    @classmethod
    def decoded(cls, raw: object, /) -> Sample:
        held = records.record(raw)
        pairs = (records.items(pair) for pair in records.items(held["latencies"]))
        return cls(
            {records.integer(micros): records.integer(n) for micros, n in pairs},
            records.integer(held["errors"]),
            records.number(held["elapsed"]),
        )


@dataclass(frozen=True)
class Summary:
    """A round, over the samples of all its clients."""

    operations: int
    errors: int
    elapsed: float
    ops_per_second: float
    error_rate: float
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None


def summarize(samples: Sequence[Sample], /) -> Summary:
    latencies: Tally[int] = Tally()
    for sample in samples:
        latencies.update(sample.latencies)
    operations = latencies.total()
    errors = sum(sample.errors for sample in samples)
    elapsed = max(sample.elapsed for sample in samples)
    ordered = sorted(latencies.items())

    def percentile(fraction: float) -> float | None:
        rank = math.ceil(operations * fraction)
        seen = 0
        for micros, n in ordered:
            seen += n
            if seen >= rank:
                return micros / 1000
        return None

    return Summary(
        operations,
        errors,
        elapsed,
        operations / elapsed,
        errors / (operations + errors) if operations + errors else 0.0,
        percentile(0.5),
        percentile(0.95),
        percentile(0.99),
    )


async def measure(settings: Settings, output: Path, site: Kubernetes, /) -> str | None:
    """Run every round of `settings` on `site`, a row each in `summary.jsonl`; answer what failed, if anything did.

    A round whose calls end in errors has not failed: its errors are in its row.
    """
    (output / "settings.json").write_text(json.dumps(settings.encoded(), indent=1) + "\n")
    summaries: dict[str, list[Summary]] = {}
    for scenario in settings.scenarios:
        clients, servers = SCENARIOS[scenario]
        _say(f"{scenario}: starting {servers} servers and {clients} clients")
        try:
            async with _started(site, scenario, servers, clients, output) as loads:
                rounds = product(settings.actors, settings.concurrency, settings.operations, range(settings.repeats))
                for actor, concurrency, called, repeat in rounds:
                    load = Load(actor, called, settings.keys, concurrency, settings.warmup, settings.duration)
                    summary = summarize(await _round(loads, load))
                    name = f"{scenario} {actor} {called} c{concurrency}"
                    summaries.setdefault(name, []).append(summary)
                    row = {
                        "scenario": scenario,
                        "actor": actor,
                        "operation": called,
                        "concurrency": concurrency,
                        "repeat": repeat + 1,
                        **asdict(summary),
                    }
                    with (output / "summary.jsonl").open("a") as rows:
                        rows.write(json.dumps(row) + "\n")
                    _say(
                        f"{name} r{repeat + 1}: {summary.ops_per_second:.0f} ops/s, p50 {_ms(summary.p50_ms)}, "
                        f"p99 {_ms(summary.p99_ms)}, {summary.errors} errors"
                    )
        except Stuck as stuck:
            return f"{scenario}: {stuck}"
    print(_table(summaries), flush=True)
    return None


@asynccontextmanager
async def _started(
    site: Kubernetes, scenario: str, servers: int, clients: int, output: Path, /
) -> AsyncGenerator[tuple[Pod, ...]]:
    """The pods of a scenario: its servers, formed into a cluster, and its clients, connected to them; all deleted
    when it ends."""
    pods: list[Pod] = []
    starting = asyncio.Semaphore(_STARTING)
    seeds = site.addresses[:_SEEDS]

    async def started(pod: Pod) -> Pod:
        pods.append(pod)
        await pod.ready(_JOIN)
        return pod

    async def server(slot: int) -> Pod:
        others = tuple(seed for seed in seeds[:servers] if seed != site.addresses[slot])
        launch = Launch(site.addresses[slot], others, None, site.store)
        async with starting:
            return await started(await site.node(slot, launch, output / f"{scenario}-slot-{slot:03d}.log"))

    async def client(index: int) -> Pod:
        arguments = ["-m", "reliability.performance", json.dumps(seeds[:servers])]
        async with starting:
            return await started(await site.load(index, arguments, output / f"{scenario}-load-{index:03d}.log"))

    try:
        nodes = await asyncio.gather(*(server(slot) for slot in range(servers)))
        await _formed(nodes, timedelta(seconds=_JOIN + servers))
        yield tuple(await asyncio.gather(*(client(index) for index in range(clients))))
    finally:
        await asyncio.gather(*(pod.kill() for pod in pods))


async def _formed(nodes: Sequence[Pod], within: timedelta, /) -> None:
    """Wait until every node sees every other one alive."""
    questions = count(1)

    async def everyone_sees_everyone() -> None:
        await asyncio.sleep(_POLL)
        answers = await asyncio.gather(*(node.ask({"members": next(questions)}, _ASKING) for node in nodes))
        for node, answer in zip(nodes, answers, strict=True):
            alive = sum(status == "alive" for _, status in View.decoded(answer).table)
            assert alive == len(nodes), f"{node.name} sees {alive} of {len(nodes)} nodes alive"

    try:
        await eventually(everyone_sees_everyone, within)
    except (AssertionError, TimeoutError) as failure:
        raise Stuck(f"the cluster did not form within {within.total_seconds():.0f}s: {failure}") from None


async def _round(loads: Sequence[Pod], load: Load, /) -> list[Sample]:
    """Prepare every client for `load`, then start them all at once, and answer their samples."""
    connections = await asyncio.gather(*(pod.connect() for pod in loads))

    async def said(within: float) -> list[object]:
        for _, writer in connections:
            await writer.drain()
        try:
            async with asyncio.timeout(within):
                return await asyncio.gather(
                    *(_answer(pod, reader) for pod, (reader, _) in zip(loads, connections, strict=True))
                )
        except TimeoutError:
            raise Stuck(f"the clients did not answer within {within:.0f}s") from None

    try:
        for index, (_, writer) in enumerate(connections):
            writer.write(_line({"prepare": replace(load, first=index * _SPREAD).encoded()}))
        await said(load.warmup + _PREPARING)
        for _, writer in connections:
            writer.write(_line({"go": None}))
        return [Sample.decoded(answer) for answer in await said(load.duration + _FINISHING)]
    finally:
        for _, writer in connections:
            writer.close()


async def _answer(pod: Pod, reader: asyncio.StreamReader, /) -> object:
    line = await reader.readline()
    if not line:
        raise Stuck(f"{pod.name} hung up; see {pod.log}")
    return json.loads(line)


async def exercise(client: Client, load: Load, duration: float, /) -> Sample:
    """Call for `duration` seconds as `load` says, and answer what came of it."""
    began = time.monotonic()
    deadline = began + duration
    latencies: Tally[int] = Tally()
    errors = 0
    refs = _refs(client, load)

    async def lane(index: int) -> None:
        nonlocal errors
        cursor = load.first + index
        while time.monotonic() < deadline:
            ref = refs[cursor % load.keys]
            cursor += load.concurrency
            before = time.monotonic()
            try:
                await _call(ref, load.operation)
            except (Unavailable, TimeoutError, MailboxFull):
                errors += 1
            else:
                latencies[round((time.monotonic() - before) * 1_000_000)] += 1

    async with asyncio.TaskGroup() as lanes:
        for index in range(load.concurrency):
            lanes.create_task(lane(index))
    return Sample(dict(latencies), errors, time.monotonic() - began)


async def warm(client: Client, load: Load, /) -> None:
    """Activate every key of `load`, and then run it for its `warmup` seconds."""
    for ref in _refs(client, load):
        await ref.ask(Read(_PAYLOAD))
    await exercise(client, load, load.warmup)


async def serve(seeds: tuple[str, ...], /) -> None:
    """Be a pod of load for the nodes of `seeds` until SIGTERM."""
    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    async with Client(seeds=seeds) as client:
        control = await asyncio.start_server(partial(_driven, client), port=CONTROL, limit=_LINE)
        say("ready", None)
        await stop.wait()
        control.close()


async def _driven(client: Client, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, /) -> None:
    prepared: Load | None = None
    try:
        while line := await reader.readline():
            command = records.record(json.loads(line))
            answer: dict[str, object]
            if "prepare" in command:
                prepared = Load.decoded(command["prepare"])
                await warm(client, prepared)
                answer = {}
            elif "go" in command:
                if prepared is None:
                    raise ValueError("go before prepare")
                answer = (await exercise(client, prepared, prepared.duration)).encoded()
            elif "crash" in command:
                os._exit(137)
            else:
                raise ValueError(f"unknown command {command!r}")
            writer.write(_line(answer))
            await writer.drain()
    finally:
        writer.close()


def _refs(client: Client, load: Load, /) -> tuple[Ref[CounterMsg], ...]:
    return tuple(client.ref(ACTORS[load.actor], f"key-{index}") for index in range(load.keys))


async def _call(ref: Ref[CounterMsg], called: Operation, /) -> None:
    match called:
        case "read":
            await ref.ask(Read(_PAYLOAD))
        case "increment":
            await ref.ask(Increment(_PAYLOAD))


def _table(summaries: Mapping[str, Sequence[Summary]], /) -> str:
    """Each round of a run, the medians of its repeats and the errors of all of them."""
    lines = [f"{'round':<32} {'ops/s':>10} {'p50':>9} {'p99':>9} {'errors':>7}"]
    for name, repeats in summaries.items():
        rate = statistics.median(summary.ops_per_second for summary in repeats)
        p50 = _median([summary.p50_ms for summary in repeats])
        p99 = _median([summary.p99_ms for summary in repeats])
        errors = sum(summary.errors for summary in repeats)
        lines.append(f"{name:<32} {rate:>10.0f} {_ms(p50):>9} {_ms(p99):>9} {errors:>7}")
    return "\n".join(lines)


def _median(values: Sequence[float | None], /) -> float | None:
    known = [value for value in values if value is not None]
    return statistics.median(known) if known else None


def _ms(value: float | None, /) -> str:
    return "-" if value is None else f"{value:.2f}ms"


def _line(body: object, /) -> bytes:
    return (json.dumps(body) + "\n").encode()


def _names(raw: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
    if raw is None:
        return default
    return tuple(name.strip() for name in raw.split(",") if name.strip())


def _say(text: str, /) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {text}", flush=True)


_PAYLOAD = b"x" * 64
_SEEDS = 3
_STARTING = 16
"""How many pods start at once, which bounds the load of a large boot."""
_JOIN = 60.0
"""Seconds a pod has to be ready."""
_ASKING = 5.0
"""Seconds a node has to answer on its control port."""
_POLL = 0.25
_SPREAD = 997
"""How far apart the first keys of two clients are, so that their lanes call different keys."""
_PREPARING = 120.0
"""Seconds past the warmup a client has to activate every key."""
_FINISHING = 60.0
"""Seconds past the duration a client has to end its last calls and answer."""
_LINE = 1 << 24
"""The longest line a pod of load reads."""


if __name__ == "__main__":
    asyncio.run(serve(records.texts(json.loads(sys.argv[1]))))
