# Development

casty has a Python facade, a typed contract and a Rust extension. Changes to protocol behavior belong with the corresponding Rust state machine; changes to actor execution and Python value handling belong in the PyO3 layer.

## Local setup

Use Python 3.12 or later, Rust 1.90 or later and uv:

```sh
uv sync
```

This installs development dependencies and builds the extension. After changing Rust code, rebuild it:

```sh
make build
```

Run the checks used by CI:

```sh
make check
```

`make check` runs Ruff, rustfmt, clippy, strict Pyright, the Rust and Python suites, and documentation checks. `make test-ft` uses a separate environment for free-threaded Python, 3.15t by default and `FT=3.14t` for the other. `make help` lists the remaining targets.

## Finding the implementation

| Change | Starting point |
|---|---|
| Actor annotations and serialization | `crates/casty-py/src/schema/` and `crates/casty-core/src/schema/` |
| Body execution, cancellation and behavior changes | `crates/casty-py/src/node/activation.rs` |
| The event-loop bridge | `crates/casty-py/src/node/inbox.rs` |
| Membership merge and failure detection | `crates/casty-core/src/membership/` |
| Ring placement and member changes | `crates/casty-core/src/placement.rs` and `crates/casty-node/src/placement/` |
| Recovery, quorum writes and tombstones | `crates/casty-core/src/replication/` |
| Range transfer and handover | `crates/casty-node/src/handoff/` |
| Framing, connection negotiation and flow control | `crates/casty-net/src/` |
| Collection facade and native behaviors | `src/casty/collections.py` and `crates/casty-py/src/collections/` |
| Durable SQL records | `crates/casty-store/src/lib.rs` |
| Public signatures and API docstrings | `src/casty/__init__.pyi` and the Python modules |

The [introduction](01-intro.md#implementation-layers) describes the crate boundaries. Keep the protocol core independent of Python and transport I/O so its decisions can be exercised directly.

## Protocol and integration tests

Rust unit tests drive protocol state machines with explicit inputs and inspect their decisions. They cover properties such as epoch fencing, partial writes, tombstone propagation and deterministic placement changes. Seeded cases exercise several member arrangements and transition orders.

Python tests run actual systems over TCP on loopback. Proxies introduce disconnections and partitions while assertions inspect replies, saved state and actor ownership. Useful starting points include [replication tests](https://github.com/gabfssilva/casty/blob/main/tests/test_replication.py), [handoff tests](https://github.com/gabfssilva/casty/blob/main/tests/test_handoff.py), [schema tests](https://github.com/gabfssilva/casty/blob/main/tests/test_schema.py) and [storage tests](https://github.com/gabfssilva/casty/blob/main/tests/test_storage.py).

`make test-stores` also exercises SQL stores against database containers. Its databases are removed when the target ends. `make chaos` runs seeded faults on Kubernetes and checks invariants; `make performance` measures cluster throughput and latency. These workloads use the [reliability infrastructure](https://github.com/gabfssilva/casty/tree/main/reliability), whose default local stack creates and destroys a kind cluster.

For a change to a guarantee, test the relevant failure sequence as well as normal execution. A completed request is not the same observation as a confirmed write, and a timeout is not proof that the request had no effect.

## Documentation

All Markdown pages live directly in `docs/`. Numbered chapters explain the model and runtime; the module pages contain the API reference. `mkdocs.yml` orders both in the navigation.

The reference is generated from Python sources and `src/casty/__init__.pyi`, without importing the compiled extension. The Griffe extension preserves the docstrings and signatures of overloads when it reads that contract.

Build and validate the documentation:

```sh
make docs
```

The target runs a strict MkDocs build and `docs/check_reference.py`. The latter checks that every name in `casty.__all__` has a docstring and appears in a rendering directive on a documentation page. The strict build checks resolvable directives, navigation and links.

Use relative links between Markdown pages and ordinary anchors for chapter sections. Code links can point to the corresponding source file in the repository. Keep paragraphs on one Markdown line; code blocks, tables and lists use their required line structure.

The [runnable examples](https://github.com/gabfssilva/casty/tree/main/examples) are also checked by the project's Python tooling. A documentation example should match the current typed API.

Implementation: [Makefile](https://github.com/gabfssilva/casty/blob/main/Makefile), [typed contract](https://github.com/gabfssilva/casty/blob/main/src/casty/__init__.pyi), [documentation coverage](https://github.com/gabfssilva/casty/blob/main/docs/check_reference.py) and [overload rendering](https://github.com/gabfssilva/casty/blob/main/docs/griffe_overloads.py).
