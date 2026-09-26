# Durable state

Shows a type declared with `durable="write"`, whose state is also kept in the store of the system: each `state.set` returns after the store has kept the write. Three nodes in the same process share a store over a SQLite file (`casty.stores.SQL`), take deposits and all stop together, which loses every replica. Three new nodes start on the same file and each account comes back with its last confirmed balance, while a type kept only in memory starts over from zero. The same store takes the URL of a PostgreSQL, MySQL or MariaDB database, which nodes on several machines reach.

Needs local TCP ports `7451` to `7453` to be free. The store file is kept in a temporary directory.

```sh
cd examples/11-durable-state
uv run main.py
```
