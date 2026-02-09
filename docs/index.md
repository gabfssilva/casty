<p align="center">
  <img src="logo.png" alt="Casty" width="400">
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

---

Casty is a typed, clustered actor framework for Python built on asyncio. Pure Python, zero dependencies. Actors communicate exclusively through immutable messages, from a single process to a distributed cluster.

- **Behaviors are values, not classes.** No `Actor` base class. Behaviors are frozen dataclasses produced by factory functions, composed like any other value.
- **State lives in closures.** State transitions happen by returning a new behavior that closes over the new state. No mutable fields, no `self.balance = ...`.
- **Immutability by default.** All messages, behaviors, events, and configurations are frozen dataclasses.
- **Zero external dependencies.** Pure Python, stdlib only.
- **Type-safe end-to-end.** `ActorRef[M]`, `Behavior[M]`, and PEP 695 type aliases catch message mismatches at development time.
- **Cluster-ready.** Distribute actors across nodes with gossip-based membership, phi accrual failure detection, and automatic shard rebalancing.

## Quick Example

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
        await asyncio.sleep(0.1)

asyncio.run(main())
# Hello, Alice!
```

## Next Steps

<div class="grid cards" markdown>

- :material-rocket-launch: **[Getting Started](getting-started/index.md)** — Installation, first actor, core concepts
- :material-book-open-variant: **[Concepts](concepts/actors-and-messages.md)** — Actors, behaviors, state, supervision, and more
- :material-database: **[Persistence](persistence/event-sourcing.md)** — Event sourcing for durable actor state
- :material-server-network: **[Clustering](clustering/cluster-sharding.md)** — Distribute actors across multiple nodes
- :material-cog: **[Configuration](configuration/index.md)** — TOML-based configuration for all parameters
- :material-api: **[API Reference](reference/behaviors.md)** — Full autodoc of all public types

</div>
