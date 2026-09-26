# casty

A minimalist, type-safe actor framework for Python 3.12+ with built-in distributed clustering.

This is the reference of the public API, one entry per name, generated from the docstrings of the typed contract
(`src/casty/__init__.pyi`) and of the Python modules. The guide, the cluster setup and the internals are in the
[README](https://github.com/gabfssilva/casty#readme).

| Module | Contents |
|---|---|
| [`casty`](casty.md) | Actor types, bodies, systems and clients, cluster settings, stores, errors |
| [`casty.collections`](collections.md) | Replicated counters, registers, dicts, sets, multimaps, queues, semaphores, locks and barriers |
| [`casty.observer`](observer.md) | The events a system reports, its observers, and the counts `stats()` reads |
| [`casty.sqlite`](sqlite.md) | A `Store` in one SQLite file |
