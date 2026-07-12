"""Pagers for types casty cannot encode on its own (spec 09 §5). Optional: importing
this module needs the `pandas` extra.

The pandas pager diffs a DataFrame against a copy-on-write shadow by *buffer address*.
Interception was rejected: `.loc`, `.iloc` and C code all reach the same buffers, and a
write pandas hides from would be silent divergence. Copy-on-write cannot hide one — the
check lives in the block manager, so the first write through any path moves the block to
a fresh buffer, and a moved address is the signal.
"""

from __future__ import annotations

import base64
import typing
from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt
import pandas as pd
import pyarrow as pa

from casty.actors.registry import PageSet
from casty.errors import SerializationError, SerializationSchemaError
from casty.serde import codec
from casty.serde.registry import message

# below 3.0 copy-on-write can be switched off and `.to_numpy()` hands out a writable
# view of the block: both make a missed mutation possible
_MIN_PANDAS = (3, 0)

# fixed, not configurable: a page key names a row range, so moving the boundaries
# would silently orphan every page already committed
_BLOCK_ROWS = 16_384

_SCHEMA = "schema"


@message(name="casty.pandas.Frame")
class _Schema:
    columns: list[str]
    rows: int
    index: tuple[int, int, int] | None  # a RangeIndex needs no pages: (start, stop, step)
    # the axis names: a page carries values, and these ride nowhere else
    index_name: str | None = None
    columns_name: str | None = None


@dataclass(frozen=True, slots=True)
class _Shadow:
    frame: pd.DataFrame  # copy-on-write: shares every buffer, so it costs nothing
    buffers: dict[str, object]  # column -> where its data lived at snapshot time


# `DataFrame._mgr` is pandas-internal and carries no annotations; these say exactly what
# this module needs from it, so an upgrade that changes the shape fails here and loudly
class _Locs(typing.Protocol):
    @property
    def as_array(self) -> npt.NDArray[np.intp]: ...


class _Block(typing.Protocol):
    @property
    def values(self) -> object: ...

    @property
    def mgr_locs(self) -> _Locs: ...


class _Manager(typing.Protocol):
    @property
    def blocks(self) -> tuple[_Block, ...]: ...


class PandasPager:
    """Pages a DataFrame as one Arrow IPC page per column per row block; use
    with `casty.paged(PandasPager())`.

    A commit compares buffer addresses against the shadow (~30 µs for 2M x 5) and then,
    for each column that moved, compares row blocks by value. That second pass is what
    keeps block consolidation honest: writing one column of a consolidated block moves
    its neighbours' buffers too, and only the value comparison stops them reaching the
    wire.

    Limits, all loud rather than lossy: string column names, no MultiIndex, and any
    change to the row count re-pages the frame (the blocks no longer line up).

    Raises
    ------
    SerializationSchemaError
        On construction, if pandas is older than 3.0 — copy-on-write is the
        diff, and before 3.0 it can be switched off.
    """

    def __init__(self) -> None:
        version = tuple(int(part) for part in pd.__version__.split(".")[:2])
        if version < _MIN_PANDAS:
            raise SerializationSchemaError(
                f"PandasPager needs pandas >= 3.0 (copy-on-write is the diff, and before "
                f"3.0 it can be switched off); found {pd.__version__}"
            )

    def snapshot(self, value: object) -> object:
        frame = _frame(value)
        return _Shadow(frame=frame.copy(deep=False), buffers=_buffers(frame))

    def rollback(self, previous: object) -> pd.DataFrame:
        return _shadow(previous).frame  # the shadow *is* the value the handler started on

    def pages(self, value: object, previous: object) -> PageSet:
        frame, shadow = _frame(value), _shadow(previous)
        old = shadow.frame
        schema, before_schema = _schema_of(frame), _schema_of(old)
        rows, old_rows = schema.rows, before_schema.rows
        blocks, old_blocks = _block_count(rows), _block_count(old_rows)
        # the row count moved: not one committed block lines up with the new frame
        moved = rows != old_rows
        reindexed = moved or not _same_index(frame, old)

        changed: dict[str, bytes] = {}
        if schema != before_schema:
            changed[_SCHEMA] = codec.encode(schema)
        if reindexed and schema.index is None:
            changed.update(
                (f"index/{block}", _ipc(pd.Series(frame.index[start:stop])))
                for block, (start, stop) in enumerate(_spans(rows))
            )
        addresses = _buffers(frame)
        for column in schema.columns:
            known = not moved and column in before_schema.columns
            if known and addresses[column] == shadow.buffers[column]:
                continue  # the buffer never moved, so copy-on-write never wrote to it
            series = frame[column]
            was = old[column] if known else None
            for block, (start, stop) in enumerate(_spans(rows)):
                chunk = series.iloc[start:stop]
                if was is not None and chunk.equals(was.iloc[start:stop]):
                    continue  # flagged by a neighbour in its consolidated block
                changed[f"col/{_key(column)}/{block}"] = _ipc(chunk)

        dropped: list[str] = []
        for column in before_schema.columns:
            first = blocks if column in schema.columns else 0
            dropped.extend(f"col/{_key(column)}/{block}" for block in range(first, old_blocks))
        if before_schema.index is None:
            first = blocks if schema.index is None else 0
            dropped.extend(f"index/{block}" for block in range(first, old_blocks))
        return PageSet(changed=changed, dropped=dropped)

    def restore(self, pages: Mapping[str, bytes]) -> pd.DataFrame:
        encoded = pages.get(_SCHEMA)
        if encoded is None:
            return pd.DataFrame()  # nothing was ever committed: the field's default
        schema = codec.decode(encoded)
        if not isinstance(schema, _Schema):
            raise SerializationError(f"{_SCHEMA}: not a paged DataFrame")
        blocks = range(_block_count(schema.rows))
        frame = pd.DataFrame(
            {column: _concat(pages, f"col/{_key(column)}", blocks) for column in schema.columns},
            columns=schema.columns,
        )
        frame.index = (
            pd.RangeIndex(*schema.index, name=schema.index_name)
            if schema.index is not None
            else pd.Index(_concat(pages, "index", blocks), name=schema.index_name)
        )
        frame.columns.name = schema.columns_name
        return frame


def _frame(value: object) -> pd.DataFrame:
    if not isinstance(value, pd.DataFrame):
        raise SerializationError(
            f"PandasPager pages DataFrame fields, not {type(value).__qualname__}"
        )
    if isinstance(value.index, pd.MultiIndex):
        raise SerializationError("PandasPager cannot page a MultiIndex")
    names = list(value.columns)
    if not all(isinstance(name, str) for name in names):
        raise SerializationError(f"PandasPager needs string column names, got {names}")
    if len(set(names)) != len(names):
        raise SerializationError(f"PandasPager needs unique column names, got {names}")
    return value


def _shadow(previous: object) -> _Shadow:
    if isinstance(previous, _Shadow):
        return previous
    raise SerializationError(
        f"expected a PandasPager snapshot, got {type(previous).__qualname__}"
    )


def _columns(frame: pd.DataFrame) -> list[str]:
    return [str(name) for name in frame.columns]


def _schema_of(frame: pd.DataFrame) -> _Schema:
    """Everything about the frame that no page carries."""
    return _Schema(
        columns=_columns(frame),
        rows=len(frame),
        index=_range_of(frame.index),
        index_name=_axis_name(frame.index),
        columns_name=_axis_name(frame.columns),
    )


def _axis_name(axis: pd.Index) -> str | None:
    return str(axis.name) if axis.name is not None else None


def _buffers(frame: pd.DataFrame) -> dict[str, object]:
    """Where each column's data currently lives. Blocks are consolidated, so several
    columns can report the same address — and all of them move when one is written."""
    columns = _columns(frame)
    manager: _Manager = frame._mgr
    addresses: dict[str, object] = {}
    for block in manager.blocks:
        address = _address(block.values)
        for position in block.mgr_locs.as_array:
            addresses[columns[int(position)]] = address
    return addresses


def _address(values: object) -> object:
    """The buffer behind a block's array. An array shape this does not recognise gets a
    fresh object, which never compares equal — the column then always looks dirty, which
    is the safe way to be wrong."""
    if isinstance(values, np.ndarray):
        return values.__array_interface__["data"][0]
    arrow = getattr(values, "_pa_array", None)
    if arrow is not None:
        return tuple(
            buffer.address
            for chunk in arrow.chunks
            for buffer in chunk.buffers()
            if buffer is not None
        )
    backing = getattr(values, "_ndarray", None)
    if isinstance(backing, np.ndarray):
        return backing.__array_interface__["data"][0]
    return object()


def _same_index(frame: pd.DataFrame, old: pd.DataFrame) -> bool:
    # identity hits whenever a handler only wrote cells: the index object is untouched
    return frame.index is old.index or bool(frame.index.equals(old.index))


def _range_of(index: pd.Index) -> tuple[int, int, int] | None:
    if isinstance(index, pd.RangeIndex):
        return (index.start, index.stop, index.step)
    return None


def _spans(rows: int) -> list[tuple[int, int]]:
    if rows == 0:
        return [(0, 0)]  # an empty block still carries the column's dtype
    return [(start, min(start + _BLOCK_ROWS, rows)) for start in range(0, rows, _BLOCK_ROWS)]


def _block_count(rows: int) -> int:
    return max(1, -(-rows // _BLOCK_ROWS))


def _key(column: str) -> str:
    return base64.urlsafe_b64encode(column.encode()).decode("ascii")


def _ipc(values: pd.Series) -> bytes:
    # renamed: the page must not depend on the column name, which is already in the key
    table = pa.Table.from_pandas(values.to_frame(name="v"), preserve_index=False)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return bytes(sink.getvalue().to_pybytes())


def _read(data: bytes) -> pd.Series:
    with pa.ipc.open_stream(pa.py_buffer(data)) as reader:
        column: pd.Series = reader.read_all().to_pandas()["v"]
    return column


def _concat(pages: Mapping[str, bytes], prefix: str, blocks: range) -> pd.Series:
    parts = [_read(pages[f"{prefix}/{block}"]) for block in blocks]
    if len(parts) == 1:
        return parts[0]
    combined: pd.Series = pd.concat(parts, ignore_index=True)
    return combined
