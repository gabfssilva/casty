"""Benchmark fixtures — simple timing helpers, no external deps."""
from __future__ import annotations

import statistics
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
import pytest


@dataclass
class BenchmarkResult:
    """Collects individual timings and reports stats."""

    label: str
    timings_ns: list[int] = field(default_factory=list)
    total_ns: int = 0
    ops: int = 0

    @contextmanager
    def measure(self) -> Generator[None]:
        start = time.perf_counter_ns()
        yield
        self.timings_ns.append(time.perf_counter_ns() - start)

    def report(self, ops: int | None = None) -> None:
        ops = ops or self.ops or len(self.timings_ns)
        total_s = self.total_ns / 1e9
        ops_per_sec = ops / total_s if total_s > 0 else float("inf")
        lines = [
            f"\n--- {self.label} ---",
            f"  total: {total_s:.4f}s",
            f"  ops:   {ops}",
            f"  ops/s: {ops_per_sec:,.0f}",
        ]
        if self.timings_ns:
            sorted_ns = sorted(self.timings_ns)
            p50 = sorted_ns[len(sorted_ns) // 2]
            p99_idx = min(int(len(sorted_ns) * 0.99), len(sorted_ns) - 1)
            p99 = sorted_ns[p99_idx]
            mean = statistics.mean(sorted_ns)
            lines.extend([
                f"  mean:  {mean / 1e6:.3f}ms",
                f"  p50:   {p50 / 1e6:.3f}ms",
                f"  p99:   {p99 / 1e6:.3f}ms",
            ])
        print("\n".join(lines))


@pytest.fixture
def bench() -> Generator[BenchmarkResult]:
    b = BenchmarkResult(label="benchmark")
    yield b
    if b.total_ns == 0 and b.timings_ns:
        b.total_ns = sum(b.timings_ns)
    b.report()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Auto-apply benchmark marker to all tests in this directory."""
    for item in items:
        if "/benchmarks/" in str(item.fspath):
            item.add_marker(pytest.mark.benchmark)
