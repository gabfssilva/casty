# Durable state

Shows a type declared with `durable="write"`, whose state is also kept in the store of the system: each `state.set` returns after the store has kept the write. Three nodes in the same process share a store in a SQLite file (`casty.sqlite.SQLiteStore`), take deposits and all stop together, which loses every replica. Three new nodes start on the same file and each account comes back with its last confirmed balance, while a type kept only in memory starts over from zero. The code of `SQLiteStore`, in `src/casty/sqlite.py`, is also the shape of a store over any other database: each method is a single SQL statement.

Needs local TCP ports `7451` to `7453` to be free. The store file is kept in a temporary directory.

```sh
cd examples/11-durable-state
uv run main.py
```
