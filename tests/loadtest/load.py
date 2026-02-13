from __future__ import annotations

import asyncio
import random
import time

import httpx

from .metrics import MetricsCollector, RequestMetric

OPERATIONS = [
    ("counter_increment", 0.40),
    ("counter_read", 0.30),
    ("kv_put", 0.15),
    ("kv_get", 0.15),
]

COUNTER_ENTITIES = [f"entity-{i}" for i in range(1000)]
KV_ENTITIES = [f"store-{i}" for i in range(100)]
KV_KEYS = [f"key-{i}" for i in range(20)]


def pick_operation() -> str:
    r = random.random()
    cumulative = 0.0
    for op, weight in OPERATIONS:
        cumulative += weight
        if r < cumulative:
            return op
    return OPERATIONS[-1][0]


async def run_worker(
    client: httpx.AsyncClient,
    node_urls: list[str],
    collector: MetricsCollector,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        op = pick_operation()
        url = random.choice(node_urls)
        start = time.monotonic()
        status_code = 0
        node = ""
        error: str | None = None

        try:
            match op:
                case "counter_increment":
                    entity = random.choice(COUNTER_ENTITIES)
                    amount = random.randint(1, 10)
                    resp = await client.post(
                        f"{url}/counter/{entity}/increment",
                        json={"amount": amount},
                        timeout=10.0,
                    )
                    status_code = resp.status_code
                    node = resp.headers.get("X-Casty-Node", "")

                case "counter_read":
                    entity = random.choice(COUNTER_ENTITIES)
                    resp = await client.get(
                        f"{url}/counter/{entity}",
                        timeout=10.0,
                    )
                    status_code = resp.status_code
                    node = resp.headers.get("X-Casty-Node", "")

                case "kv_put":
                    entity = random.choice(KV_ENTITIES)
                    key = random.choice(KV_KEYS)
                    value = f"val-{random.randint(0, 99999)}"
                    resp = await client.put(
                        f"{url}/kv/{entity}/{key}",
                        json={"value": value},
                        timeout=10.0,
                    )
                    status_code = resp.status_code
                    node = resp.headers.get("X-Casty-Node", "")

                case "kv_get":
                    entity = random.choice(KV_ENTITIES)
                    key = random.choice(KV_KEYS)
                    resp = await client.get(
                        f"{url}/kv/{entity}/{key}",
                        timeout=10.0,
                    )
                    status_code = resp.status_code
                    node = resp.headers.get("X-Casty-Node", "")

                case _:
                    pass

        except httpx.HTTPError as exc:
            status_code = 0
            error = str(exc)
        except asyncio.CancelledError:
            return

        latency_ms = (time.monotonic() - start) * 1000.0
        metric = RequestMetric(
            timestamp=collector.now(),
            operation=op,
            status_code=status_code,
            latency_ms=latency_ms,
            node=node,
            error=error,
        )
        await collector.record_request(metric)


async def run_load(
    node_urls: list[str],
    collector: MetricsCollector,
    num_workers: int,
    stop_event: asyncio.Event,
) -> None:
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=100)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [
            asyncio.create_task(
                run_worker(client, node_urls, collector, stop_event)
            )
            for _ in range(num_workers)
        ]
        await stop_event.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
