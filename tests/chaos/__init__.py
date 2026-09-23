"""The chaos suite: many nodes, each in a process of its own, under seeded faults and traffic for as long as asked.

    CASTY_CHAOS=1 uv run pytest tests/chaos -s

runs it with the defaults: 20 nodes, 5 minutes of faults, every workload, a random seed. Without `CASTY_CHAOS=1` it is
not even collected. `CHAOS_NODES`, `CHAOS_MINUTES`, `CHAOS_SEED` and the rest of `run.Settings.environment` shape the
run, and `CHAOS_REPLAY=<output>/run.json` replays one.

- `node`: the process of one node, and the proxies its links go through, driven over its stdin and stdout.
- `fleet`: the runner's side of those processes, and the network between them.
- `schedule`: the faults, planned from the seed before the run starts.
- `journal` and `workloads`: the traffic, the record of it, and the invariants it is checked against; `actors`, the
  types only this suite writes to.
- `run`: all of it together, and the report.
"""
