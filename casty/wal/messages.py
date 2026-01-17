from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.wal.version import VectorClock


@dataclass(frozen=True)
class Append:
    delta: dict[str, Any]


@dataclass(frozen=True)
class Snapshot:
    state: dict[str, Any]


@dataclass(frozen=True)
class SyncTo:
    version: "VectorClock"
    state: dict[str, Any]


@dataclass(frozen=True)
class AppendMerged:
    version: "VectorClock"
    state: dict[str, Any]


@dataclass(frozen=True)
class Close:
    pass


@dataclass(frozen=True)
class Recover:
    pass


@dataclass(frozen=True)
class GetCurrentVersion:
    pass


@dataclass(frozen=True)
class GetCurrentState:
    pass


@dataclass(frozen=True)
class GetStateAt:
    version: "VectorClock"


@dataclass(frozen=True)
class FindBase:
    their_version: "VectorClock"


@dataclass(frozen=True)
class Merge:
    their_version: "VectorClock"
    their_state: dict[str, Any]
    my_state: dict[str, Any]
    actor: Any


@dataclass(frozen=True)
class MergeResult:
    version: "VectorClock"
    merged_state: dict[str, Any]
    success: bool
