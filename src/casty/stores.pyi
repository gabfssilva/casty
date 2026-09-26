"""The stores casty carries, written in Rust.

Each is a `casty.Store`, which the nodes of a cluster call from their own threads, without the event loop.
"""

from typing import Self

__all__ = ["SQL"]

class SQL:
    """`casty.Store` over a SQL database: PostgreSQL, MySQL, MariaDB, SQLite, and the databases that speak the protocol
    of one of them.

    The records are the rows of one table, `casty_records`, which the store makes when the database has none. A row is
    found by a digest of its actor and its key, so no collation, limit on the length of a key or reserved word of the
    database changes which row a key has. A save keeps its record only when its version is greater than the version of
    the record kept, compared as bytes.

    The store is used inside `async with`, which connects on entering and closes the connections on leaving, once the
    calls under way are done. The calls run on a thread of the store, and each answers on the event loop the store was
    entered on. The nodes of a cluster call it from their own threads, without the event loop.

    Entering raises `ValueError` for a URL that names no database the store knows, and `ConnectionError` when the
    database cannot be reached or refuses the table. A call that fails raises `Unavailable`, and one made outside
    `async with` raises `RuntimeError`.

    Parameters
    ----------
    url
        The database, by the scheme of its driver: `postgres://user:password@host/database`,
        `mysql://user:password@host/database`, or `sqlite://path?mode=rwc` for a file made when it does not exist.
        Not `sqlite::memory:`: each connection of the store would open a database of its own.
    """

    def __init__(self, url: str, /) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...
    async def load(self, actor: str, key: str, /) -> tuple[bytes, bytes | None] | None:
        """The record of `(actor, key)` as `(version, state)`, or `None` when there is none."""
        ...

    async def save(self, actor: str, key: str, version: bytes, state: bytes | None, /) -> None:
        """Keep `(version, state)` as the record of `(actor, key)`, unless its record has a greater version."""
        ...

    async def drop(self, actor: str, key: str, version: bytes, /) -> None:
        """Forget the record of `(actor, key)` if its version is not greater than `version`."""
        ...
