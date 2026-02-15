"""Load + chaos test via Docker Compose using ClusterClient.

Sends ShardEnvelope messages directly via TCP — zero HTTP overhead,
testing the actual actor-level routing path.

Usage:
    docker compose -f tests/load/docker-compose.yml up --build -d
    sleep 10
    docker compose -f tests/load/docker-compose.yml --profile loadtest run --rm loadtest
    docker compose -f tests/load/docker-compose.yml down
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import subprocess
import time
import urllib.request
from dataclasses import dataclass, field

from casty.client.client import ClusterClient
from casty.cluster import ShardEnvelope

from app.entities import Get, GetCounter, Increment, Put

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("casty").setLevel(logging.WARNING)
log = logging.getLogger("load")

CONTACT_HOST = os.environ.get("CONTACT_HOST", "node-0")
CASTY_PORT = int(os.environ.get("CASTY_PORT", "25520"))
CLIENT_HOST = os.environ.get("CLIENT_HOST", "127.0.0.1")
CLIENT_PORT = int(os.environ.get("CLIENT_PORT", "0"))
NODE_COUNT = int(os.environ.get("NODE_COUNT", "5"))
DURATION = int(os.environ.get("DURATION", "180"))
WARMUP = int(os.environ.get("WARMUP", "15"))
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "20"))

CONTAINER_NAMES = [f"loadtest-node-{i}" for i in range(NODE_COUNT)]


@dataclass
class Stats:
    requests: int = 0
    errors: int = 0
    latencies: list[float] = field(default_factory=list)
    faults: list[tuple[float, str]] = field(default_factory=list)
    start: float = field(default_factory=time.monotonic)

    def record(self, latency_ms: float, error: bool) -> None:
        self.requests += 1
        if error:
            self.errors += 1
        self.latencies.append(latency_ms)

    def record_fault(self, kind: str) -> None:
        self.faults.append((time.monotonic() - self.start, kind))

    def elapsed(self) -> float:
        return time.monotonic() - self.start


async def worker(
    client: ClusterClient,
    counter: object,
    kv: object,
    stats: Stats,
    stop: asyncio.Event,
) -> None:
    ops = ["counter_increment", "counter_read", "kv_put", "kv_get"]
    weights = [0.4, 0.3, 0.15, 0.15]

    while not stop.is_set():
        op = random.choices(ops, weights)[0]
        entity = f"entity-{random.randint(0, 99)}"
        start = time.monotonic()
        error = False

        try:
            match op:
                case "counter_increment":
                    counter.tell(ShardEnvelope(entity, Increment(amount=random.randint(1, 5))))  # type: ignore[union-attr]
                case "counter_read":
                    await client.ask(
                        counter,  # type: ignore[arg-type]
                        lambda r, eid=entity: ShardEnvelope(eid, GetCounter(reply_to=r)),
                        timeout=2.0,
                    )
                case "kv_put":
                    key = f"key-{random.randint(0, 9)}"
                    kv.tell(ShardEnvelope(entity, Put(key=key, value=f"val-{random.randint(0, 999)}")))  # type: ignore[union-attr]
                case "kv_get":
                    key = f"key-{random.randint(0, 9)}"
                    await client.ask(
                        kv,  # type: ignore[arg-type]
                        lambda r, eid=entity, k=key: ShardEnvelope(eid, Get(key=k, reply_to=r)),
                        timeout=2.0,
                    )
        except Exception as exc:  # noqa: BLE001
            error = True
            if stats.requests % 100 == 0:
                log.debug("Worker error on %s(%s): %s", op, entity, exc)

        latency_ms = (time.monotonic() - start) * 1000
        stats.record(latency_ms, error)


def docker_kill(container: str) -> None:
    subprocess.run(
        ["docker", "kill", container],
        capture_output=True,
    )


def docker_start(container: str) -> None:
    subprocess.run(
        ["docker", "start", container],
        capture_output=True,
    )


HTTP_PORT = int(os.environ.get("HTTP_PORT", "8000"))


def query_cluster_state_sync(node_index: int = 0) -> str:
    """Query cluster state from a node's HTTP API and return a summary."""
    try:
        url = f"http://node-{node_index}:{HTTP_PORT}/cluster/status"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
        members = data.get("members", [])
        unreachable = data.get("unreachable", [])
        lines = [f"  {m['address']} status={m['status']}" for m in members]
        summary = "\n".join(lines)
        if unreachable:
            summary += f"\n  unreachable: {unreachable}"
        return summary
    except Exception as exc:  # noqa: BLE001
        return f"  (query failed: {exc})"


async def query_cluster_state(node_index: int = 0) -> str:
    """Async wrapper around the sync HTTP query."""
    return await asyncio.to_thread(query_cluster_state_sync, node_index)



async def chaos_monkey(stats: Stats, stop: asyncio.Event) -> None:
    """Inject faults on a schedule, independent of load."""
    await asyncio.sleep(WARMUP)

    scenarios = [
        ("kill_non_leader", 30),
        ("kill_leader", 40),
        ("kill_two", 30),
    ]

    for scenario, duration in scenarios:
        if stop.is_set():
            return

        cluster_before = await query_cluster_state()
        log.info("CHAOS: cluster state before '%s':\n%s", scenario, cluster_before)

        match scenario:
            case "kill_non_leader":
                victim = CONTAINER_NAMES[2]
                log.info("CHAOS: killing %s", victim)
                stats.record_fault(f"kill {victim}")
                docker_kill(victim)
                await asyncio.sleep(10)
                cluster_after_kill = await query_cluster_state()
                log.info("CHAOS: cluster state 10s after kill:\n%s", cluster_after_kill)
                await asyncio.sleep(5)
                log.info("CHAOS: restarting %s", victim)
                stats.record_fault(f"restart {victim}")
                docker_start(victim)
                await asyncio.sleep(duration - 15)

            case "kill_leader":
                victim = CONTAINER_NAMES[0]
                log.info("CHAOS: killing leader %s", victim)
                stats.record_fault(f"kill {victim}")
                docker_kill(victim)
                await asyncio.sleep(10)
                cluster_after_kill = await query_cluster_state()
                log.info("CHAOS: cluster state 10s after leader kill:\n%s", cluster_after_kill)
                await asyncio.sleep(10)
                log.info("CHAOS: restarting leader %s", victim)
                stats.record_fault(f"restart {victim}")
                docker_start(victim)
                await asyncio.sleep(duration - 20)

            case "kill_two":
                v1, v2 = CONTAINER_NAMES[1], CONTAINER_NAMES[3]
                log.info("CHAOS: killing %s and %s", v1, v2)
                stats.record_fault(f"kill {v1}+{v2}")
                docker_kill(v1)
                docker_kill(v2)
                await asyncio.sleep(10)
                cluster_after_kill = await query_cluster_state()
                log.info("CHAOS: cluster state 10s after double kill:\n%s", cluster_after_kill)
                await asyncio.sleep(5)
                log.info("CHAOS: restarting %s and %s", v1, v2)
                stats.record_fault(f"restart {v1}+{v2}")
                docker_start(v1)
                docker_start(v2)
                await asyncio.sleep(duration - 15)

    log.info("CHAOS: all scenarios done")
    final_state = await query_cluster_state()
    log.info("CHAOS: final cluster state:\n%s", final_state)


async def printer(stats: Stats, stop: asyncio.Event) -> None:
    last_reqs = 0
    last_errs = 0
    while not stop.is_set():
        await asyncio.sleep(5)
        elapsed = stats.elapsed()
        delta = stats.requests - last_reqs
        delta_errs = stats.errors - last_errs
        rps = delta / 5
        last_reqs = stats.requests
        last_errs = stats.errors
        err_pct = delta_errs / max(delta, 1) * 100

        recent = stats.latencies[-500:]
        p50 = sorted(recent)[len(recent) // 2] if recent else 0
        log.info(
            "[%5.0fs] %3.0f rps | %d reqs (%d err, %.1f%% window) | p50=%.1fms",
            elapsed, rps, stats.requests, stats.errors, err_pct, p50,
        )


async def main() -> None:
    stats = Stats()
    stop = asyncio.Event()

    log.info("=== Load + Chaos Test (ClusterClient) ===")
    log.info("Contact: %s:%d | Workers: %d | Duration: %ds", CONTACT_HOST, CASTY_PORT, NUM_WORKERS, DURATION)

    async with ClusterClient(
        contact_points=[(CONTACT_HOST, CASTY_PORT)],
        system_name="loadtest-cluster",
        client_host=CLIENT_HOST,
        client_port=CLIENT_PORT,
    ) as client:
        counter = client.entity_ref("counters", num_shards=20)
        kv = client.entity_ref("kv", num_shards=20)

        log.info("Warming up for %ds...", WARMUP)
        initial_state = await query_cluster_state()
        log.info("Cluster state at startup:\n%s", initial_state)
        await asyncio.sleep(WARMUP)
        log.info("Starting load")

        workers = [
            asyncio.create_task(worker(client, counter, kv, stats, stop))
            for _ in range(NUM_WORKERS)
        ]
        printer_task = asyncio.create_task(printer(stats, stop))
        chaos_task = asyncio.create_task(chaos_monkey(stats, stop))

        await asyncio.sleep(DURATION)
        stop.set()

        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await printer_task
        await chaos_task

    log.info("=== Results ===")
    log.info("Total: %d reqs | %d errors (%.1f%%)",
             stats.requests, stats.errors,
             stats.errors / max(stats.requests, 1) * 100)

    if stats.latencies:
        s = sorted(stats.latencies)
        log.info("Latency: p50=%.1fms p95=%.1fms p99=%.1fms",
                 s[len(s) // 2], s[int(len(s) * 0.95)], s[int(len(s) * 0.99)])

    if stats.faults:
        log.info("Faults:")
        for ts, kind in stats.faults:
            log.info("  [%5.0fs] %s", ts, kind)


if __name__ == "__main__":
    asyncio.run(main())
