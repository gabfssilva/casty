"""Internal discriminated result for distributed collections.

Distinguishes "no value present" from "the value is None" on the wire, so
that ``dequeue`` / ``get`` can raise on absence while still returning a real
``None`` payload. Never exposed in the public API — clients translate these
into a value or a raised ``QueueEmpty`` / ``KeyError``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final


@dataclass(frozen=True)
class Some:
    value: Any


class _Empty:
    __slots__ = ()

    def __repr__(self) -> str:
        return "EMPTY"


EMPTY: Final[_Empty] = _Empty()

type Maybe = Some | _Empty
