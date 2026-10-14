# casty

<p align="center">
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/v/casty.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/pyversions/casty.svg" alt="Python"></a>
  <a href="https://github.com/gabfssilva/casty/actions"><img src="https://img.shields.io/github/actions/workflow/status/gabfssilva/casty/ci.yml" alt="Tests"></a>
  <a href="https://github.com/gabfssilva/casty/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
</p>

casty is a typed virtual actor framework for concurrent and distributed programming in Python.

As [Hewitt](https://arxiv.org/pdf/1008.1459) describes, an actor is a primitive of concurrent computation that communicates with other actors through asynchronous messages. Upon receiving a message, an actor may:

1. Send messages to actors whose addresses it knows.
2. Create new actors.
3. Determine how it will handle the next message it receives.

In casty, actors are defined as async Python functions decorated with `@actor`. Each function defines an actor type, and each `(actor type, key)` identifies a virtual actor with its own state and mailbox.

An actor processes one message at a time. The runtime manages activation and deactivation, and the same actor code can run in a single process or across a cluster with state replication.

[Documentation](https://gabfssilva.github.io/casty/) · [Examples](examples) · [Issues](https://github.com/gabfssilva/casty/issues)

## Installation

```sh
pip install casty
```

Or add it to a project with uv:

```sh
uv add casty
```

Requires Python 3.12 or later. The runtime is written in Rust. Installing from source requires Rust 1.90 or later.

## A first actor

This counter updates its state and replies with the new total:

```python
import asyncio
from dataclasses import dataclass

from casty import ActorSystem, Askable, Context, actor


@dataclass(frozen=True)
class Add:
    amount: int
    
@dataclass(frozen=True)
class Get(Askable[int]): pass


@actor(initial=0)
async def counter(ctx: Context[int, Add | Get]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Add(amount):
                await ctx.state.set(ctx.state.value + amount)
            case Get():
                msg.reply_to.tell(ctx.state.value)


async def main() -> None:
    async with ActorSystem() as system:
        views = system.ref(counter, "page-views")
        views.tell(Add(1))
        print(await views.ask(Get()))
        views.tell(Add(2))
        print(await views.ask(Get()))


asyncio.run(main())
```

Output:

```text
1
3
```

`Context[int, Add | Get]` declares the state and message types. `Askable[int]` declares the reply type, and `initial=0` provides the initial state of each counter.

`system.ref(counter, "page-views")` addresses one counter by its key. A different key has independent state. `ask` waits for a reply; `tell` sends a message without waiting for one.

State, messages and replies are serialized even within one process. Use immutable values, such as frozen dataclasses, tuples and frozensets.

An `await` inside the message loop delays the next message for that actor. To handle other asynchronous work while continuing to receive messages, use `ctx.to_self` or `ctx.ask`. Blocking calls can run through `asyncio.to_thread`.

## Running a cluster

To join a cluster, configure the node's listening address, advertised address and seed nodes. In the example above, import `Cluster` and replace `main` with:

```python
from casty import Cluster


async def main() -> None:
    cluster = Cluster(
        bind="0.0.0.0:7400",
        advertise="10.0.0.5:7400",
        seeds=("10.0.0.4:7400",),
    )
    async with ActorSystem(cluster=cluster) as system:
        views = system.ref(counter, "page-views")
        print(await views.ask(Add(1)))
```

Every node must have the same actor definitions, at module scope. The runtime routes messages to the node that owns each key and moves actors as nodes join, leave or fail. Placement uses consistent hashing, and membership is maintained through gossip.

By default, each key has up to three replicas, limited by the cluster size, and state writes wait for a majority of replicas to confirm. Both the replica count and the write level are configurable per actor type.

A `Client` sends messages to the cluster without hosting actors or participating in membership. See the [cluster example](examples/06-distribution) and [client example](examples/07-client) for runnable programs.

## State and persistence

`ctx.state.value` reads the last saved state. `await ctx.state.set(value)` replaces it, and `await ctx.state.update(change)` applies a function and returns the saved result. Both wait for the configured write level to confirm before updating the local value.

State is kept in memory on its replicas. To preserve it across a full cluster restart, configure a store and declare the actor type with `durable="write"`. A confirmed write then also waits for the store.

`casty.stores.SQL` supports PostgreSQL, MySQL, MariaDB and SQLite. Every node must reach the same stored records. The [durable state example](examples/11-durable-state) demonstrates recovery after stopping the entire cluster.

## Schedules and collections

Actors can schedule messages with `ctx.schedule`. Schedules are saved with actor state and follow the actor when its owner changes.

`Collections` provides replicated counters, registers, dicts, sets, multimaps, queues, semaphores, locks and barriers over an `ActorSystem` or a `Client`. Collections are not durable.

The [documentation](docs/index.md) covers the programming model, runtime implementation and public APIs in numbered chapters. The [examples](examples) also cover state machines, messages between actors, streams and failure recovery.

## Guarantees and limits

- Message delivery is at most once. Messages from one node to one key arrive in order while the owner remains the same.
- With majority writes, confirmed state survives the loss of a minority of replicas. The minority side of a network partition cannot write. With `write="one"`, an ownership change can lose confirmed writes.
- A failed or timed-out operation may have committed.
- Reads are not linearizable. During ownership changes or after a long process pause, an actor may answer from stale state.
- Side effects can repeat after an actor restarts or changes owner.
- Losing every replica loses in-memory state. Use a durable store when state must survive that loss.
- Schedules and leases depend on synchronized node clocks. Schedules can be missed or delivered twice during ownership changes; leases can expire while their holder is still running.

## Contributing

Bug reports and pull requests are welcome. For a bug report, include a small reproducer and your Python and casty versions.

To set up the development environment and run the checks:

```sh
uv sync
make check
```

`make check` runs linting, type checks, the Rust and Python test suites, and documentation checks. After changing Rust code, rebuild the extension with `make build`. Run `make help` for the other development commands.

## License

[MIT](LICENSE).
