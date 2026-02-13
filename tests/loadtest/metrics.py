from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

log = logging.getLogger("loadtest.metrics")


@dataclass
class RequestMetric:
    timestamp: float
    operation: str
    status_code: int
    latency_ms: float
    node: str
    error: str | None = None


@dataclass
class FaultEvent:
    timestamp: float
    kind: str
    target: str


class MetricsCollector:
    def __init__(self) -> None:
        self._start = time.monotonic()
        self._requests: list[RequestMetric] = []
        self._faults: list[FaultEvent] = []
        self._lock = asyncio.Lock()
        self._printer_task: asyncio.Task[None] | None = None

    def now(self) -> float:
        return time.monotonic() - self._start

    async def record_request(self, metric: RequestMetric) -> None:
        async with self._lock:
            self._requests.append(metric)

    def record_fault(self, kind: str, target: str) -> None:
        self._faults.append(FaultEvent(timestamp=self.now(), kind=kind, target=target))

    async def snapshot(self) -> tuple[list[RequestMetric], list[FaultEvent]]:
        async with self._lock:
            return list(self._requests), list(self._faults)

    def start_printer(self, interval: float = 5.0) -> None:
        self._printer_task = asyncio.create_task(self._print_loop(interval))

    def stop_printer(self) -> None:
        if self._printer_task:
            self._printer_task.cancel()

    async def _print_loop(self, interval: float) -> None:
        last_count = 0
        while True:
            await asyncio.sleep(interval)
            requests, _ = await self.snapshot()
            total = len(requests)
            new = total - last_count
            errors = sum(1 for r in requests[last_count:] if r.status_code >= 400)
            if new > 0:
                latencies = sorted(r.latency_ms for r in requests[last_count:])
                p50 = latencies[len(latencies) // 2]
                rps = new / interval
                elapsed = self.now()
                log.info(
                    "[%6.1fs] %d rps | %d reqs (%d err) | p50=%.1fms",
                    elapsed, rps, new, errors, p50,
                )
            last_count = total
