"""Read sweep_results.jsonl and print pivot tables (throughput, p99, err%)
grouped by cluster size, with actors on rows and writers on columns."""

from __future__ import annotations

import json
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "sweep_results.jsonl"
rows = [json.loads(line) for line in open(path) if line.strip()]

nodes = sorted({r["nodes"] for r in rows})
writers = sorted({r["writers"] for r in rows})
actors = sorted({r["actors"] for r in rows})
by = {(r["nodes"], r["writers"], r["actors"]): r for r in rows}


def grid(metric: str, fmt: str) -> None:
    for n in nodes:
        print(f"\n### nodes={n} — {metric}")
        head = "actors".rjust(8) + "".join(f"{'w=' + str(w):>14}" for w in writers)
        print(head)
        for a in actors:
            cells = []
            for w in writers:
                r = by.get((n, w, a))
                cells.append(format(r[metric], fmt).rjust(14) if r else "-".rjust(14))
            print(f"{a:>8}" + "".join(cells))


grid("ops_per_sec", ".0f")
grid("p99_ms", ".1f")
grid("err_pct", ".2f")

print(f"\n{len(rows)} points | nodes={nodes} writers={writers} actors={actors}")
