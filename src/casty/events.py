from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty.ref import ActorRef

if TYPE_CHECKING:
    from casty.cluster_state import Member


# --- Event types ---


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


@dataclass(frozen=True)
class MemberUp:
    member: Member


@dataclass(frozen=True)
class MemberLeft:
    member: Member


@dataclass(frozen=True)
class UnreachableMember:
    member: Member


@dataclass(frozen=True)
class ReachableMember:
    member: Member


# --- EventStream ---

type EventHandler[E] = Callable[[E], Awaitable[None] | None]


class EventStream:
    def __init__(self) -> None:
        self._subscribers: dict[type, list[EventHandler[Any]]] = defaultdict(list)

    def subscribe[E](self, event_type: type[E], handler: EventHandler[E]) -> None:
        self._subscribers[event_type].append(handler)

    def unsubscribe(self, event_type: type[object], handler: EventHandler[object]) -> None:
        handlers = self._subscribers.get(event_type)
        if handlers:
            handlers.remove(handler)

    async def publish(self, event: object) -> None:
        handlers = self._subscribers.get(type(event), [])
        for handler in handlers:
            result = handler(event)
            if asyncio.iscoroutine(result):
                await result
