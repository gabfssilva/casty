"""The chaos run: nodes in pods under seeded faults and traffic for as long as asked, and the invariants they must keep.

    make chaos

runs it with the defaults: 20 nodes, 5 minutes of faults, every workload, a random seed. `CHAOS_NODES`,
`CHAOS_MINUTES`, `CHAOS_SEED` and the rest of `run.Settings.environment` shape the run, and
`CHAOS_REPLAY=<output>/run.json` replays one.

- `fleet`: the runner's side of the nodes, and the network between them.
- `schedule`: the faults, planned from the seed before the run starts.
- `journal` and `workloads`: the traffic, the record of it, and the invariants it is checked against.
- `run`: all of it together, and the report.
"""
