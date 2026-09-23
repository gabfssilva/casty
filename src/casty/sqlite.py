"""A store for the durable types in one SQLite file, with nothing but the standard library.

`SQLiteStore` is `casty.Store` over one table of a row per key. Each of its methods is one statement, so it is also the
shape of a store over any database that compares two blobs byte by byte.
"""

from __future__ import annotations

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from os import PathLike
from typing import Self


class SQLiteStore:
    """`casty.Store` in one SQLite file, which every process of one machine can open at once.

    Every system given a store on the same file shares its records, whatever process it runs in: SQLite locks the file
    for each write, so writers take turns. That makes it the store of a system alone, or of the nodes of one machine. A
    cluster across machines needs a database they all reach, since SQLite does not lock a file over a network file
    system.

    A save keeps its record only when its version is greater than the version of the record kept, compared as bytes,
    which is how SQLite compares blobs. A method returns once its statement committed. The statements run on a thread
    of the store, one at a time, so the event loop never waits for the disk.

    Parameters
    ----------
    path
        The file of the database. It is created, with its table, when it does not exist.
    """

    def __init__(self, path: str | PathLike[str], /) -> None:
        self._db = sqlite3.connect(path, autocommit=True, check_same_thread=False)
        # A write-ahead log lets the processes sharing the file read while one of them writes. Processes opening a new
        # file at once race to switch it, and SQLite refuses the switches that lose without waiting: the file keeps the
        # log the winner set, which every connection to it then uses.
        with suppress(sqlite3.OperationalError):
            self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = FULL")
        self._db.execute(_TABLE)
        self._worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="casty-sqlite")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Wait for the statements under way, then close the file."""
        self._worker.shutdown()
        self._db.close()

    async def load(self, actor: str, key: str, /) -> tuple[bytes, bytes | None] | None:
        """The record of `(actor, key)` as `(version, state)`, or `None` when there is none."""
        return await asyncio.get_running_loop().run_in_executor(self._worker, self._load, actor, key)

    async def save(self, actor: str, key: str, version: bytes, state: bytes | None, /) -> None:
        """Keep `(version, state)` as the record of `(actor, key)`, unless its record has a greater version."""
        await asyncio.get_running_loop().run_in_executor(self._worker, self._run, _SAVE, (actor, key, version, state))

    async def drop(self, actor: str, key: str, version: bytes, /) -> None:
        """Forget the record of `(actor, key)` if its version is not greater than `version`."""
        await asyncio.get_running_loop().run_in_executor(self._worker, self._run, _DROP, (actor, key, version))

    def _load(self, actor: str, key: str, /) -> tuple[bytes, bytes | None] | None:
        row: object = self._db.execute(_LOAD, (actor, key)).fetchone()
        match row:
            case None:
                return None
            case (bytes() as version, bytes() | None as state):
                return version, state
            case _:
                raise TypeError(f"the record of {actor}/{key} is {row!r}, which casty did not write")

    def _run(self, statement: str, parameters: tuple[str | bytes | None, ...], /) -> None:
        self._db.execute(statement, parameters)


_TABLE = """
CREATE TABLE IF NOT EXISTS records (
    actor TEXT NOT NULL,
    key TEXT NOT NULL,
    version BLOB NOT NULL,
    state BLOB,
    PRIMARY KEY (actor, key)
) WITHOUT ROWID
"""
_LOAD = "SELECT version, state FROM records WHERE actor = ? AND key = ?"
_SAVE = """
INSERT INTO records (actor, key, version, state) VALUES (?, ?, ?, ?)
ON CONFLICT (actor, key) DO UPDATE SET version = excluded.version, state = excluded.state
WHERE excluded.version > records.version
"""
_DROP = "DELETE FROM records WHERE actor = ? AND key = ? AND version <= ?"
