"""Supervision strategies for actor failure handling.

Provides the ``SupervisionStrategy`` protocol and a default
``OneForOneStrategy`` that decides per-child restart/stop/escalate
directives based on failure frequency.
"""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable
from enum import Enum, auto
from typing import Protocol


class Directive(Enum):
    """Action to take when a supervised actor fails.

    Examples
    --------
    >>> from casty import Directive
    >>> Directive.restart
    <Directive.restart: 1>
    """

    restart = auto()
    stop = auto()
    escalate = auto()


class SupervisionStrategy(Protocol):
    """Protocol for deciding how to handle actor failures.

    Implementations receive the exception raised by a child actor and
    return a ``Directive`` indicating the recovery action.

    Examples
    --------
    >>> class AlwaysRestart:
    ...     def decide(self, exception, *, child_id="", **kw):
    ...         return Directive.restart
    """

    def decide(self, exception: Exception, *, child_id: str = ..., **kwargs: object) -> Directive: ...


class OneForOneStrategy(SupervisionStrategy):
    """Supervision strategy that handles each child failure independently.

    Restarts a failing child up to ``max_restarts`` times within a
    sliding time window. If the limit is exceeded the child is stopped.

    Parameters
    ----------
    max_restarts : int
        Maximum number of restarts allowed within the time window.
    within : float
        Length of the sliding time window in seconds.
    decider : Callable[[Exception], Directive] | None
        Optional function to override the directive for specific
        exceptions. If it returns ``Directive.restart``, the rate-limit
        logic still applies.

    Examples
    --------
    >>> from casty import OneForOneStrategy, Directive
    >>> strategy = OneForOneStrategy(max_restarts=5, within=30.0)
    """

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
        """Decide the recovery action for a failed child actor.

        Parameters
        ----------
        exception : Exception
            The exception that caused the child to fail.
        child_id : str
            Identifier for the child actor, used to track per-child
            restart frequency.

        Returns
        -------
        Directive
            The action to take: restart, stop, or escalate.

        Examples
        --------
        >>> strategy = OneForOneStrategy(max_restarts=1, within=60.0)
        >>> strategy.decide(ValueError("bad"), child_id="a")
        <Directive.restart: 1>
        """
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
