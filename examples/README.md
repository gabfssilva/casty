# Examples

Each directory is a program that demonstrates one part of casty. They all use the `uv` project of this directory, which installs the local version of casty from the root of the repository.

Requires `uv`, Python 3.12 or later and a Rust toolchain: on the first run, `uv` compiles casty. The commands start from the root of the repository, and each example runs with `uv run` inside its directory:

```sh
cd examples/00-hello-world
uv run main.py
```

The examples with several nodes start them all in the same process, each on a local TCP port, and stop them when they finish. `09-docker-cluster` is the exception: it runs in containers and needs only Docker.

- [`00-hello-world`](00-hello-world): an actor with an initial state, asked with `ask`, and one state per key.
- [`01-state-machine`](01-state-machine): an order as a state machine, where `ctx.become` changes the behavior without changing the reference.
- [`02-actors-talking`](02-actors-talking): an actor that coordinates transfers by asking other actors through `ctx.system.ref`.
- [`03-streams`](03-streams): `ctx.merge` joining the mailbox and an async generator in the same loop of the actor.
- [`04-failures`](04-failures): `ActorFailed`, `MailboxFull` and the state an actor keeps after a failure.
- [`05-replication`](05-replication): a state with three replicas that survives the loss of the node running its key.
- [`06-distribution`](06-distribution): keys spread over the hash ring while nodes join and leave the cluster.
- [`07-client`](07-client): a `Client` that uses the cluster without being part of it, with each node in its own process.
- [`08-consumers`](08-consumers): one consumer per partition, which the cluster resumes on its own when its node goes down.
- [`09-docker-cluster`](09-docker-cluster): a cluster of thirty-three nodes in Docker Compose, resized while it runs.
- [`10-agent-per-node`](10-agent-per-node): one agent per node, declared with `pinned=True` and reached by the address of the node.
- [`11-durable-state`](11-durable-state): a state with `durable="write"` in a SQLite store, which survives the stop of every node.
