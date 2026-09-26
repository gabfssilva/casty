"""JSON read back into typed values: what the runner and a node tell each other, and what a dump holds.

Each reader takes what `json.loads` gave and either answers the type it names or raises `ValueError` saying what it
found instead, so a malformed line or dump fails where it is read.
"""

from __future__ import annotations

from typing import TypeGuard


def record(raw: object) -> dict[str, object]:
    if not _mapping(raw):
        raise ValueError(f"expected an object, found {raw!r}")
    return {key: value for key, value in raw.items() if isinstance(key, str)}


def items(raw: object) -> list[object]:
    if not _sequence(raw):
        raise ValueError(f"expected a list, found {raw!r}")
    return raw


def text(raw: object) -> str:
    if not isinstance(raw, str):
        raise ValueError(f"expected a string, found {raw!r}")
    return raw


def integer(raw: object) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError(f"expected an integer, found {raw!r}")
    return raw


def number(raw: object) -> float:
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise ValueError(f"expected a number, found {raw!r}")
    return float(raw)


def texts(raw: object) -> tuple[str, ...]:
    return tuple(text(item) for item in items(raw))


def _mapping(raw: object, /) -> TypeGuard[dict[object, object]]:
    return isinstance(raw, dict)


def _sequence(raw: object, /) -> TypeGuard[list[object]]:
    return isinstance(raw, list)
