from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable
from enum import Enum, auto
from typing import Protocol


class Directive(Enum):
    restart = auto()
    stop = auto()
    escalate = auto()


class SupervisionStrategy(Protocol):
    def decide(self, exception: Exception, **kwargs: object) -> Directive: ...


class OneForOneStrategy(SupervisionStrategy):
    def __init__(
        self,
        max_restarts: int = 3,
        within: float = 60.0,
        decider: Callable[[Exception], Directive] | None = None,
    ) -> None:
        self._max_restarts = max_restarts
        self._within = within
        self._decider = decider
        self._restart_timestamps: dict[str, list[float]] = defaultdict(list)

    def decide(
        self, exception: Exception, *, child_id: str = "__default__", **kwargs: object
    ) -> Directive:
        if self._decider:
            directive = self._decider(exception)
            if directive != Directive.restart:
                return directive

        now = time.monotonic()
        timestamps = self._restart_timestamps[child_id]

        # Prune timestamps outside the window
        cutoff = now - self._within
        timestamps[:] = [t for t in timestamps if t > cutoff]

        if len(timestamps) >= self._max_restarts:
            return Directive.stop

        timestamps.append(now)
        return Directive.restart
