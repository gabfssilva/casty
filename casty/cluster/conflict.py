from __future__ import annotations

from enum import Enum

from .vector_clock import VectorClock


class ConflictResult(Enum):
    ACCEPT_REMOTE = "accept_remote"
    IGNORE = "ignore"
    MERGE = "merge"


def detect_conflict(local: VectorClock, remote: VectorClock) -> ConflictResult:
    comparison = local.compare(remote)

    match comparison:
        case "before":
            return ConflictResult.ACCEPT_REMOTE
        case "after":
            return ConflictResult.IGNORE
        case "concurrent":
            if local.versions == remote.versions:
                return ConflictResult.IGNORE
            return ConflictResult.MERGE
