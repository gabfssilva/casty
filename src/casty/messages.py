"""Actor lifecycle signal messages.

Provides the ``Terminated`` message delivered to watchers when a
monitored actor stops.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty.ref import ActorRef


@dataclass(frozen=True)
class Terminated:
    """Signal sent when a watched actor is stopped.

    Delivered to actors that called ``ctx.watch()`` on the terminated
    actor's reference.

    Parameters
    ----------
    ref : ActorRef[Any]
        Reference to the actor that terminated.

    Examples
    --------
    >>> from casty import Terminated
    >>> event = Terminated(ref=watched_ref)
    """

    ref: ActorRef[Any]
