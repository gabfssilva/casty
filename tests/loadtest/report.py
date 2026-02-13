from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict

from .metrics import FaultEvent, MetricsCollector, RequestMetric

log = logging.getLogger("loadtest.report")


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(values):
        return values[f]
    return values[f] + (k - f) * (values[c] - values[f])


def fault_impact(
    requests: list[RequestMetric],
    fault: FaultEvent,
    window: float = 10.0,
) -> dict[str, object]:
    before = [
        r for r in requests
        if fault.timestamp - window <= r.timestamp < fault.timestamp
    ]
    after = [
        r for r in requests
        if fault.timestamp <= r.timestamp < fault.timestamp + window
    ]

    before_errs = sum(1 for r in before if r.status_code >= 400 or r.error)
    after_errs = sum(1 for r in after if r.status_code >= 400 or r.error)
    before_p50 = percentile([r.latency_ms for r in before], 50)
    after_p50 = percentile([r.latency_ms for r in after], 50)
    before_rps = len(before) / window if window > 0 else 0
    after_rps = len(after) / window if window > 0 else 0

    return {
        "fault": fault.kind,
        "target": fault.target,
        "timestamp": round(fault.timestamp, 1),
        "err_delta": after_errs - before_errs,
        "p50_before_ms": round(before_p50, 1),
        "p50_after_ms": round(after_p50, 1),
        "rps_before": round(before_rps, 0),
        "rps_after": round(after_rps, 0),
    }


async def generate_report(collector: MetricsCollector) -> dict[str, object]:
    requests, faults = await collector.snapshot()
    duration = collector.now()
    total = len(requests)
    successes = sum(1 for r in requests if 200 <= r.status_code < 400)
    errors = total - successes

    latencies = [r.latency_ms for r in requests]
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)
    max_lat = max(latencies) if latencies else 0.0
    throughput = total / duration if duration > 0 else 0

    by_op: dict[str, list[float]] = {}
    for r in requests:
        by_op.setdefault(r.operation, []).append(r.latency_ms)

    op_stats = {}
    for op, lats in sorted(by_op.items()):
        op_stats[op] = {
            "count": len(lats),
            "p50_ms": round(percentile(lats, 50), 1),
            "p99_ms": round(percentile(lats, 99), 1),
        }

    impacts = [fault_impact(requests, f) for f in faults]

    report = {
        "duration_s": round(duration, 1),
        "total_requests": total,
        "throughput_rps": round(throughput, 1),
        "successes": successes,
        "errors": errors,
        "success_rate_pct": round(successes / total * 100, 1) if total else 0,
        "latency": {
            "p50_ms": round(p50, 1),
            "p95_ms": round(p95, 1),
            "p99_ms": round(p99, 1),
            "max_ms": round(max_lat, 1),
        },
        "by_operation": op_stats,
        "fault_impacts": impacts,
    }

    print_report(report)
    save_report(report, requests, faults)
    return report


def print_report(report: dict[str, object]) -> None:
    lat = report["latency"]
    log.info("=== Casty Load + Resilience Report ===")
    log.info(
        "Duration: %ss | Requests: %s | Throughput: %s rps",
        report["duration_s"], f"{report['total_requests']:,}", report["throughput_rps"],
    )
    log.info(
        "Success: %s (%s%%) | Errors: %s",
        f"{report['successes']:,}", report["success_rate_pct"], f"{report['errors']:,}",
    )
    log.info(
        "Latency: p50=%sms  p95=%sms  p99=%sms  max=%sms",
        lat["p50_ms"], lat["p95_ms"], lat["p99_ms"], lat["max_ms"],  # type: ignore[index]
    )

    log.info("By Operation:")
    for op, stats in report["by_operation"].items():  # type: ignore[union-attr]
        log.info(
            "  %-25s %7s reqs  p50=%sms  p99=%sms",
            op, f"{stats['count']:,}", stats["p50_ms"], stats["p99_ms"],
        )

    if report["fault_impacts"]:
        log.info("Fault Impact (10s window before vs after):")
        for impact in report["fault_impacts"]:  # type: ignore[union-attr]
            log.info(
                "  [%6.1fs] %-18s %s -> err +%s, p50 %s->%sms, rps %.0f->%.0f",
                impact["timestamp"], impact["fault"], impact["target"],
                impact["err_delta"], impact["p50_before_ms"], impact["p50_after_ms"],
                impact["rps_before"], impact["rps_after"],
            )


def save_report(
    report: dict[str, object],
    requests: list[RequestMetric],
    faults: list[FaultEvent],
) -> None:
    ts = int(time.time())
    filename = f"report-{ts}.json"
    data = {
        "summary": report,
        "requests": [asdict(r) for r in requests],
        "faults": [asdict(f) for f in faults],
    }
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    log.info("Full report saved to %s", filename)
