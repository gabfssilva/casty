"""Benchmark fixtures — timing, persistence, and regression detection."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import statistics
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pytest

BENCHMARKS_DIR = Path(__file__).resolve().parents[2] / ".benchmarks"
REGRESSION_THRESHOLD = float(os.environ.get("BENCH_REGRESSION_THRESHOLD", "0.20"))


def machine_id() -> str:
    raw = f"{platform.node()}-{platform.system()}-{platform.machine()}-{platform.processor()}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


@dataclass
class BenchmarkResult:
    """Collects individual timings and reports stats."""

    label: str
    test_name: str = ""
    timings_ns: list[int] = field(default_factory=list)
    total_ns: int = 0
    ops: int = 0

    @contextmanager
    def measure(self) -> Generator[None]:
        start = time.perf_counter_ns()
        yield
        self.timings_ns.append(time.perf_counter_ns() - start)

    def stats(self) -> dict[str, float]:
        ops = self.ops or len(self.timings_ns)
        total_s = self.total_ns / 1e9
        ops_per_sec = ops / total_s if total_s > 0 else float("inf")
        result: dict[str, float] = {
            "total_s": total_s,
            "ops": ops,
            "ops_per_sec": ops_per_sec,
        }
        if self.timings_ns:
            sorted_ns = sorted(self.timings_ns)
            result["mean_ms"] = statistics.mean(sorted_ns) / 1e6
            result["p50_ms"] = sorted_ns[len(sorted_ns) // 2] / 1e6
            p99_idx = min(int(len(sorted_ns) * 0.99), len(sorted_ns) - 1)
            result["p99_ms"] = sorted_ns[p99_idx] / 1e6
        return result

    def report(self) -> None:
        s = self.stats()
        lines = [
            f"\n--- {self.label} ---",
            f"  total: {s['total_s']:.4f}s",
            f"  ops:   {int(s['ops'])}",
            f"  ops/s: {s['ops_per_sec']:,.0f}",
        ]
        if "mean_ms" in s:
            lines.extend(
                [
                    f"  mean:  {s['mean_ms']:.3f}ms",
                    f"  p50:   {s['p50_ms']:.3f}ms",
                    f"  p99:   {s['p99_ms']:.3f}ms",
                ]
            )
        print("\n".join(lines))

    def save(self) -> None:
        mid = machine_id()
        out_dir = BENCHMARKS_DIR / mid
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{self.test_name}.json"
        path.write_text(
            json.dumps(
                {
                    "label": self.label,
                    "machine_id": mid,
                    "platform": platform.platform(),
                    "node": platform.node(),
                    "stats": self.stats(),
                },
                indent=2,
            )
        )

    def load_baseline(self) -> dict[str, float] | None:
        path = BENCHMARKS_DIR / machine_id() / f"{self.test_name}.json"
        if not path.exists():
            return None
        data = json.loads(path.read_text())
        return data.get("stats")

    def check_regression(self) -> None:
        baseline = self.load_baseline()
        if baseline is None:
            print(
                f"  [baseline] no previous run for {self.test_name}, saving as baseline"
            )
            return

        current = self.stats()
        regressions: list[str] = []

        # ops/sec regression = current is LOWER than baseline
        if baseline.get("ops_per_sec", 0) > 0:
            ratio = current["ops_per_sec"] / baseline["ops_per_sec"]
            delta = 1.0 - ratio
            if delta > REGRESSION_THRESHOLD:
                regressions.append(
                    f"ops/sec regressed {delta:.0%}: "
                    f"{baseline['ops_per_sec']:,.0f} -> {current['ops_per_sec']:,.0f}"
                )
            else:
                symbol = "+" if ratio >= 1.0 else ""
                print(f"  [vs baseline] ops/s: {symbol}{(ratio - 1.0):.1%}")

        # latency regression = current is HIGHER than baseline
        for metric in ("p50_ms", "p99_ms"):
            if metric in baseline and metric in current and baseline[metric] > 0:
                ratio = current[metric] / baseline[metric]
                delta = ratio - 1.0
                if delta > REGRESSION_THRESHOLD:
                    regressions.append(
                        f"{metric} regressed {delta:.0%}: "
                        f"{baseline[metric]:.3f}ms -> {current[metric]:.3f}ms"
                    )
                else:
                    symbol = "+" if delta >= 0 else ""
                    print(f"  [vs baseline] {metric}: {symbol}{delta:.1%}")

        if regressions:
            msg = (
                f"Performance regression detected (threshold: {REGRESSION_THRESHOLD:.0%}):\n"
                + "\n".join(f"  - {r}" for r in regressions)
            )
            pytest.fail(msg)


@pytest.fixture
def bench(request: pytest.FixtureRequest) -> Generator[BenchmarkResult]:
    b = BenchmarkResult(label="benchmark", test_name=request.node.name)
    yield b
    if b.total_ns == 0 and b.timings_ns:
        b.total_ns = sum(b.timings_ns)
    b.report()

    if request.config.getoption("--benchmark-compare"):
        b.check_regression()

    if request.config.getoption("--benchmark-save"):
        b.save()
        print(f"  [saved] .benchmarks/{machine_id()}/{b.test_name}.json")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Auto-apply benchmark marker to all tests in this directory."""
    for item in items:
        if "/benchmarks/" in str(item.fspath):
            item.add_marker(pytest.mark.benchmark)
