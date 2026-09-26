"""The runs of casty on Kubernetes: the chaos run, which checks the invariants of a cluster under seeded faults, and the
performance run, which measures its throughput and latency. Nothing here is imported by the suite in `tests/`, and
nothing here imports it.

    make chaos            # shaped by the CHAOS_* variables of `chaos.run`
    make performance      # shaped by the PERFORMANCE_* variables of `performance`

Each brings up the cluster of the Pulumi stack STACK (`local`, a kind cluster, by default) and destroys it when the run
ends, however it ends.

- `__main__`: the driver, on this machine: plans the run, brings the cluster up, builds the image of this checkout
  (`Dockerfile`), starts the runner, copies back what the run leaves, and destroys the cluster and the image.
- `runner`: the runner, in its pod: `chaos.run` or `performance.measure` on the `site`.
- `site`: the pods of a run, and the network between its nodes as Chaos Mesh objects.
- `node`: one node, in its pod; `actors` and `deploy`, the types the runs call.
- `chaos`: the fleet, the schedule of faults, the traffic and its invariants.
- `performance`: the rounds of load, and the program of a pod of load.
- `infra`: the Pulumi project of the cluster.
"""
