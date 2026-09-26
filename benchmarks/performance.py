"""Compare actor read/increment throughput in local and multiprocess TCP topologies.

Run from the repository root. Each client uses closed-loop concurrency over the same keys.
Latencies cover successful asks; throughput includes draining requests started before the deadline.
Servers are counted including seeds. Replication uses up to three copies and majority writes.
"""

import argparse
import asyncio
import csv
import json
import math
import os
import platform
import signal
import socket
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from benchmarks.actors import Increment, Read, counter
from casty import ActorSystem, Client, Cluster, MailboxFull, Unavailable

type Operation = Literal["read", "increment"]
SCENARIOS = {"local": (1, 0), "1x1": (1, 1), "1x3": (1, 3), "1x10": (1, 10), "3x10": (3, 10), "5x50": (5, 50)}


@dataclass(frozen=True)
class Sample:
    latencies: tuple[float, ...]
    errors: int
    elapsed: float


@dataclass(frozen=True)
class Summary:
    operations: int
    errors: int
    elapsed: float
    ops_per_second: float
    error_rate: float
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None


def summarize(samples: tuple[Sample, ...]) -> Summary:
    latencies = sorted(value for sample in samples for value in sample.latencies)
    errors = sum(sample.errors for sample in samples)
    elapsed = max(sample.elapsed for sample in samples)
    count = len(latencies)

    def percentile(fraction: float) -> float | None:
        return latencies[math.ceil(count * fraction) - 1] * 1000 if count else None

    return Summary(
        count,
        errors,
        elapsed,
        count / elapsed,
        errors / (count + errors) if count + errors else 0,
        percentile(0.5),
        percentile(0.95),
        percentile(0.99),
    )


async def exercise(
    system: ActorSystem | Client,
    operation: Operation,
    namespace: str,
    *,
    keys: int,
    concurrency: int,
    duration: float,
    start: float | None = None,
    offset: int = 0,
) -> Sample:
    beginning = time.monotonic() if start is None else start
    await asyncio.sleep(max(0, beginning - time.monotonic()))
    deadline = beginning + duration
    latencies: list[float] = []
    errors = 0
    refs = tuple(system.ref(counter, f"{namespace}:{i}") for i in range(keys))
    payload = b"x" * 64

    async def lane(index: int) -> None:
        nonlocal errors
        cursor = offset + index
        while time.monotonic() < deadline:
            ref = refs[cursor % keys]
            cursor += concurrency
            before = time.monotonic()
            try:
                if operation == "read":
                    await ref.ask(Read(payload))
                else:
                    await ref.ask(Increment(payload))
            except (Unavailable, TimeoutError, MailboxFull):
                errors += 1
            else:
                latencies.append(time.monotonic() - before)

    async with asyncio.TaskGroup() as group:
        for index in range(concurrency):
            group.create_task(lane(index))
    return Sample(tuple(latencies), errors, time.monotonic() - beginning)


@dataclass
class Options:
    scenarios: list[str] | None = None
    concurrency: list[int] | None = None
    duration: float = 5
    warmup: float = 2
    repeats: int = 3
    keys: int = 1000
    output: str = "benchmarks/results"
    role: str = "run"
    port: int = 0
    servers: int = 0
    seed: str = ""
    operation: Operation = "read"
    client: int = 0


async def worker(options: Options) -> None:
    if options.role == "server":
        stop = asyncio.Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(signum, stop.set)
        cluster = Cluster(
            bind=f"127.0.0.1:{options.port}",
            seeds=(options.seed,),
            suspect_after=timedelta(seconds=60),
            anti_entropy=timedelta(seconds=1),
        )
        async with ActorSystem(cluster=cluster, leave_timeout=timedelta(seconds=1)) as system:
            system.ref(counter, "bench:0")
            await formed(system, options.servers)
            print("READY", flush=True)
            await stop.wait()
        return
    target = Client(seeds=(options.seed,)) if options.servers else ActorSystem()
    async with target as system:
        for index in range(options.keys):
            await system.ref(counter, f"bench:{index}").ask(Read(b"x" * 64))
        await exercise(
            system,
            options.operation,
            "bench",
            keys=options.keys,
            concurrency=(options.concurrency or [1])[0],
            duration=options.warmup,
            offset=options.client * 997,
        )
        print("READY", flush=True)
        start = float(await asyncio.to_thread(sys.stdin.readline))
        sample = await exercise(
            system,
            options.operation,
            "bench",
            keys=options.keys,
            concurrency=(options.concurrency or [1])[0],
            duration=options.duration,
            start=start,
            offset=options.client * 997,
        )
        await asyncio.to_thread(write_sample, Path(options.output), sample)


async def formed(system: ActorSystem, count: int) -> None:
    complete = asyncio.Event()
    loop = asyncio.get_running_loop()
    timer: asyncio.TimerHandle | None = None

    def check() -> None:
        nonlocal timer
        if sum(member.status == "alive" for member in system.members) == count:
            complete.set()
        else:
            timer = loop.call_later(0.1, check)

    check()
    try:
        async with asyncio.timeout(120):
            await complete.wait()
    finally:
        if timer is not None:
            timer.cancel()


def write_sample(path: Path, sample: Sample) -> None:
    with path.open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow((sample.elapsed, sample.errors))
        writer.writerows((latency,) for latency in sample.latencies)


def read_sample(path: Path) -> Sample:
    with path.open(newline="") as file:
        rows = csv.reader(file)
        elapsed, errors = next(rows)
        return Sample(tuple(float(row[0]) for row in rows), int(errors), float(elapsed))


async def ready(process: asyncio.subprocess.Process) -> None:
    assert process.stdout is not None
    async with asyncio.timeout(150):
        line = await process.stdout.readline()
    if line != b"READY\n":
        raise RuntimeError(f"worker {process.pid} did not become ready: {line!r}; inspect worker logs")


async def stop(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        process.terminate()
    try:
        async with asyncio.timeout(10):
            await process.wait()
    except TimeoutError:
        process.kill()
        await process.wait()


def ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            sockets.append(listener)
        return [int(listener.getsockname()[1]) for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()


async def run(options: Options) -> None:
    output = Path(options.output) / datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    output.mkdir(parents=True)
    metadata = {
        "options": asdict(options),
        "python": sys.version,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "load_average_at_start": os.getloadavg(),
        "topologies_clients_servers": SCENARIOS,
        "replicas": 3,
        "write": "majority",
        "payload_bytes": 64,
        "load": "closed-loop; concurrency per client; success latencies; elapsed includes drain",
        "cluster": {"suspect_after_seconds": 60, "anti_entropy_seconds": 1},
    }
    (output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"Results: {output}", flush=True)

    async def launch(label: str, arguments: list[str]) -> asyncio.subprocess.Process:
        with (output / f"{label}.log").open("w") as log:
            return await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "benchmarks.performance",
                *arguments,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=log,
            )

    for scenario in options.scenarios or list(SCENARIOS):
        clients, servers = SCENARIOS[scenario]
        addresses = ports(servers)
        seed = f"127.0.0.1:{addresses[0]}" if addresses else ""
        hosts: list[asyncio.subprocess.Process] = []
        print(f"{scenario}: starting {servers} servers, {clients} clients", flush=True)
        try:
            for index, port in enumerate(addresses):
                hosts.append(
                    await launch(
                        f"{scenario}-server-{index}",
                        [
                            "--role",
                            "server",
                            "--port",
                            str(port),
                            "--seed",
                            seed,
                            "--servers",
                            str(servers),
                        ],
                    )
                )
            await asyncio.gather(*(ready(host) for host in hosts))
            for concurrency in options.concurrency or [1, 8, 32]:
                for operation in ("read", "increment"):
                    for repeat in range(1, options.repeats + 1):
                        label = f"{scenario}-{operation}-c{concurrency}-r{repeat}"
                        print(f"{label}: warmup", flush=True)
                        workers: list[asyncio.subprocess.Process] = []
                        paths = [output / f"{label}-client-{index}.csv" for index in range(clients)]
                        try:
                            for index, path in enumerate(paths):
                                workers.append(
                                    await launch(
                                        f"{label}-client-{index}",
                                        [
                                            "--role",
                                            "client",
                                            "--seed",
                                            seed,
                                            "--servers",
                                            str(servers),
                                            "--client",
                                            str(index),
                                            "--operation",
                                            operation,
                                            "--keys",
                                            str(options.keys),
                                            "--concurrency",
                                            str(concurrency),
                                            "--warmup",
                                            str(options.warmup),
                                            "--duration",
                                            str(options.duration),
                                            "--output",
                                            str(path),
                                        ],
                                    )
                                )
                            await asyncio.gather(*(ready(client) for client in workers))
                            start = time.monotonic() + 0.5
                            for client in workers:
                                assert client.stdin is not None
                                client.stdin.write(f"{start}\n".encode())
                                await client.stdin.drain()
                            async with asyncio.timeout(options.duration + 30):
                                codes = await asyncio.gather(*(client.wait() for client in workers))
                            if any(codes):
                                raise RuntimeError(f"{label}: worker exit codes {codes}; inspect worker logs")
                            result = summarize(tuple(read_sample(path) for path in paths))
                            row = {
                                "scenario": scenario,
                                "operation": operation,
                                "concurrency": concurrency,
                                "repeat": repeat,
                                **asdict(result),
                            }
                            with (output / "summary.jsonl").open("a") as file:
                                file.write(json.dumps(row) + "\n")
                            print(
                                f"{label}: {result.ops_per_second:.0f} ops/s, p99={result.p99_ms} ms, "
                                f"errors={result.errors}",
                                flush=True,
                            )
                        finally:
                            await asyncio.gather(*(stop(client) for client in workers))
        finally:
            await asyncio.gather(*(stop(host) for host in hosts))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--scenarios", nargs="+", choices=list(SCENARIOS))
    parser.add_argument("--concurrency", nargs="+", type=int, help="In-flight requests per client (default: 1 8 32)")
    parser.add_argument("--duration", type=float, default=5, help="Measured seconds per repetition")
    parser.add_argument("--warmup", type=float, default=2, help="Warmup seconds, excluded from measurements")
    parser.add_argument("--repeats", type=int, default=3, help="Repetitions per operation and concurrency")
    parser.add_argument("--keys", type=int, default=1000, help="Total shared actor keys")
    parser.add_argument("--output", default="benchmarks/results", help="Parent for timestamped results and worker logs")
    parser.add_argument("--role", choices=["run", "server", "client"], default="run", help=argparse.SUPPRESS)
    parser.add_argument("--port", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--servers", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--seed", default="", help=argparse.SUPPRESS)
    parser.add_argument("--operation", choices=["read", "increment"], default="read", help=argparse.SUPPRESS)
    parser.add_argument("--client", type=int, default=0, help=argparse.SUPPRESS)
    options = parser.parse_args(namespace=Options())
    if min(options.duration, options.warmup, options.repeats, options.keys, *(options.concurrency or [1])) <= 0:
        parser.error("duration, warmup, repeats, keys and concurrency must be positive")
    asyncio.run(run(options) if options.role == "run" else worker(options))


if __name__ == "__main__":
    main()
