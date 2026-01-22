from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class Merge(Generic[T]):
    base: T | None
    local: T
    remote: T
