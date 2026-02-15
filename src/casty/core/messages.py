"""Actor lifecycle signal messages.

Provides the ``Terminated`` message delivered to watchers when a
monitored actor stops.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.core.ref import ActorRef


@dataclass(frozen=True)
class Terminated:
    """Signal sent when a watched actor is stopped.

    Delivered to actors that called ``ctx.watch()`` on the terminated
    actor's reference.
    """

    ref: ActorRef[Any]
