"""Typed actor references for fire-and-forget messaging.

ActorRef is the protocol. LocalActorRef delivers in-process via callback.
Remote refs (TcpActorRef) live in the remote layer.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

type ActorId = str


class ActorRef[M](Protocol):
    """Typed handle to an actor, used for fire-and-forget messaging."""

    @property
    def id(self) -> ActorId: ...

    def tell(self, msg: M) -> None: ...


@dataclass(frozen=True)
class LocalActorRef[M]:
    """In-process actor reference that delivers via direct callback."""

    id: ActorId
    _deliver: Callable[[Any], None]

    def tell(self, msg: M) -> None:
        self._deliver(msg)

    def __reduce__(self) -> Any:
        msg = (
            "LocalActorRef cannot be pickled — use a system with "
            "__make_ref__ that returns RemoteActorRef"
        )
        raise RuntimeError(msg)
