# Introduction

casty is a typed virtual actor framework for concurrent and distributed programming in Python. An actor represents an entity, such as a user, a device or a job, with its own identity, state and behavior. Applications communicate with that entity through messages, independently of the node that currently executes it.

The same actor definition runs in one process or in a cluster. The runtime manages activation, message routing and state replication. Durable storage is configured separately from replication.

## Installation

```sh
pip install casty
```

For a uv project:

```sh
uv add casty
```

casty requires Python 3.12 or later. The runtime is a Rust extension; installing from source requires Rust 1.90 or later. Free-threaded Python (3.14t and 3.15t) is also supported.

## A first program

A message that expects an integer reply subclasses `Askable[int]`. The actor updates its state before replying:

```python
import asyncio
from dataclasses import dataclass

from casty import ActorSystem, Askable, Context, actor


@dataclass(frozen=True)
class Add(Askable[int]):
    amount: int


@actor(initial=0)
async def counter(ctx: Context[int, Add]) -> None:
    async for msg in ctx.inbox:
        total = await ctx.state.update(lambda value, amount=msg.amount: value + amount)
        msg.reply_to.tell(total)


async def main() -> None:
    async with ActorSystem() as system:
        views = system.ref(counter, "page-views")
        print(await views.ask(Add(1)))
        print(await views.ask(Add(2)))


asyncio.run(main())
```

Save the program as `main.py` and run `python main.py`. It prints `1`, then `3`.

`counter` is an actor type. `"page-views"` is the identity of one counter of that type. Another key has independent state and a mailbox of its own. `Context[int, Add]` declares the state and message types; `Askable[int]` declares the reply type. Messages, state and replies are serialized, including in local mode.

## Implementation layers

| Component | Responsibility |
|---|---|
| [`casty-core`](https://github.com/gabfssilva/casty/tree/main/crates/casty-core/src) | Protocol state machines, placement, mailboxes, schemas and state pages |
| [`casty-net`](https://github.com/gabfssilva/casty/tree/main/crates/casty-net/src) | Connections, framing, stream multiplexing, TLS and compression |
| [`casty-node`](https://github.com/gabfssilva/casty/tree/main/crates/casty-node/src) | Membership, routing, replication and handoff over the transport |
| [`casty-py`](https://github.com/gabfssilva/casty/tree/main/crates/casty-py/src) | PyO3 bindings, Python schemas, activations and the asyncio bridge |
| [`casty-store`](https://github.com/gabfssilva/casty/tree/main/crates/casty-store/src) | Versioned records in SQL databases |

The core protocols consume inputs and produce decisions and outgoing messages. The node layer performs the I/O those decisions require. Actor bodies execute on the Python event loop; clustering and transport execute on Rust runtime threads. The [actor model chapter](02-model-actor.md#execution-and-threading) explains the boundary.

## Reading the documentation

Start with the [actor model](02-model-actor.md) and [state](03-state.md). For distributed execution, continue with [clustering](04-clustering.md), [replication](05-replication.md) and [handoff](06-handoff.md).

The remaining chapters cover [messaging](07-messaging.md), [schedules](08-schedulers.md), [collections](09-collections.md), [failures and guarantees](10-failures.md) and [development](11-development.md). The [overview](index.md#api-reference) links to the generated API reference.
