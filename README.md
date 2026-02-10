<p align="center">
  <img src="docs/logo_bw.png" alt="Casty" width="512">
</p>

<p align="center">
  <strong>Typed, clustered actor framework for Python</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/v/casty.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/pyversions/casty.svg" alt="Python"></a>
  <a href="https://github.com/gabfssilva/casty/actions"><img src="https://img.shields.io/github/actions/workflow/status/gabfssilva/casty/python-package.yml" alt="Tests"></a>
  <a href="https://github.com/gabfssilva/casty/blob/main/LICENSE"><img src="https://img.shields.io/github/license/gabfssilva.svg" alt="License"></a>
</p>

<p align="center">
  <a href="https://gabfssilva.github.io/casty/">Documentation</a> · <a href="https://gabfssilva.github.io/casty/getting-started/">Getting Started</a> · <a href="https://pypi.org/project/casty/">PyPI</a>
</p>

---

Casty is a typed, clustered actor framework for Python built on asyncio. Instead of threads, locks, and shared mutable state, you model your system as independent actors that communicate exclusively through immutable messages, from a single process to a distributed cluster.

## Quick Start

```
pip install casty
```

```python
import asyncio
from dataclasses import dataclass
from casty import ActorContext, ActorSystem, Behavior, Behaviors

@dataclass(frozen=True)
class Greet:
    name: str

def greeter() -> Behavior[Greet]:
    async def receive(ctx: ActorContext[Greet], msg: Greet) -> Behavior[Greet]:
        print(f"Hello, {msg.name}!")
        return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(greeter(), "greeter")
        ref.tell(Greet("Alice"))
        ref.tell(Greet("Bob"))
        await asyncio.sleep(0.1)

asyncio.run(main())
# Hello, Alice!
# Hello, Bob!
```

## Features

- **Behaviors as values, not classes** — No `Actor` base class. Behaviors are frozen dataclasses composed through factory functions.
- **State via closures** — Actor state is captured in closures. State transitions happen by returning a new behavior with new closed-over values. No mutable fields, no `nonlocal`.
- **Immutability by default** — All messages, behaviors, events, and configurations are frozen dataclasses.
- **Type-safe end-to-end** — `ActorRef[M]`, `Behavior[M]`, and PEP 695 type aliases ensure message type mismatches are caught at development time.
- **Zero external dependencies** — Pure Python, stdlib only.
- **Supervision** — "Let it crash" philosophy with configurable restart strategies, inspired by Erlang/OTP.
- **Event sourcing** — Persist actor state as a sequence of events with automatic recovery on restart.
- **Cluster sharding** — Distribute actors across nodes with gossip-based membership, phi accrual failure detection, and automatic shard rebalancing.
- **Shard replication** — Passive replicas with automatic promotion on node failure.
- **Cluster broadcast** — Fan-out messages to all nodes with typesafe aggregated responses.
- **Distributed data structures** — Counter, Dict, Set, Queue, Lock, Semaphore, and Barrier built on sharded actors.
- **TOML configuration** — Optional `casty.toml` for environment-specific tuning with per-actor overrides.

## Documentation

Full documentation is available at **[gabfssilva.github.io/casty](https://gabfssilva.github.io/casty/)**.

| Section | Description |
|---------|-------------|
| [Getting Started](https://gabfssilva.github.io/casty/getting-started/) | Installation and first steps |
| [Concepts](https://gabfssilva.github.io/casty/concepts/actors-and-messages/) | Actors, behaviors, state, request-reply, hierarchies, supervision, state machines |
| [Persistence](https://gabfssilva.github.io/casty/persistence/event-sourcing/) | Event sourcing with journals and snapshots |
| [Clustering](https://gabfssilva.github.io/casty/clustering/cluster-sharding/) | Sharding, broadcast, replication, distributed data structures |
| [Configuration](https://gabfssilva.github.io/casty/configuration/) | TOML-based configuration with per-actor overrides |
| [API Reference](https://gabfssilva.github.io/casty/reference/behaviors/) | Complete API documentation |

## Acknowledgments

Casty builds on the actor model (Hewitt, 1973), Erlang/OTP's supervision philosophy, Akka Typed's functional behavior API, and distributed systems research including phi accrual failure detection, gossip protocols, CRDTs, and vector clocks. See the full [Acknowledgments](https://gabfssilva.github.io/casty/acknowledgments/) page for details and references.

## Contributing

```bash
git clone https://github.com/gabfssilva/casty
cd casty
uv sync
uv run pytest                    # run tests
uv run pyright src/casty/        # type checking (strict mode)
uv run ruff check src/ tests/    # lint
uv run ruff format src/ tests/   # format
```

## License

MIT — see [LICENSE](LICENSE) for details.
