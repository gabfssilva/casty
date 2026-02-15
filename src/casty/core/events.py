"""Actor system event types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.core.ref import ActorRef


@dataclass(frozen=True)
class ActorStarted:
    ref: ActorRef[Any]


@dataclass(frozen=True)
class ActorStopped:
    ref: ActorRef[Any]


@dataclass(frozen=True)
class ActorRestarted:
    ref: ActorRef[Any]
    exception: Exception


@dataclass(frozen=True)
class DeadLetter:
    message: Any
    intended_ref: ActorRef[Any]


@dataclass(frozen=True)
class UnhandledMessage:
    message: Any
    ref: ActorRef[Any]
