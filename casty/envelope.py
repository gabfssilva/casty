from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from casty.serializable import serializable

if TYPE_CHECKING:
    from .ref import ActorRef, UnresolvedActorRef


@serializable
@dataclass
class Envelope[M]:
    payload: M
    sender: "UnresolvedActorRef | ActorRef[Any] | None" = None
    target: str | None = None
    correlation_id: str | None = None
    reply_to: "asyncio.Future[Any] | None" = field(default=None, repr=False)
