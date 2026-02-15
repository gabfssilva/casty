"""Event stream actor for system-wide pub/sub."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.core.behavior import Behavior
from casty.behaviors import Behaviors

if TYPE_CHECKING:
    from casty.core.context import ActorContext
    from casty.core.ref import ActorRef


@dataclass(frozen=True)
class Subscribe[E]:
    event_type: type[E]
    handler: ActorRef[E]


@dataclass(frozen=True)
class Unsubscribe[E]:
    event_type: type[E]
    handler: ActorRef[E]


@dataclass(frozen=True)
class Publish:
    event: object


type EventStreamMsg = Subscribe[Any] | Unsubscribe[Any] | Publish


def event_stream_actor() -> Behavior[EventStreamMsg]:
    def active(
        subscribers: dict[type, tuple[ActorRef[Any], ...]] = {},
    ) -> Behavior[EventStreamMsg]:
        async def receive(
            ctx: ActorContext[EventStreamMsg], msg: EventStreamMsg,
        ) -> Behavior[EventStreamMsg]:
            match msg:
                case Subscribe(event_type=et, handler=h):
                    current = subscribers.get(et, ())
                    return active({**subscribers, et: (*current, h)})

                case Unsubscribe(event_type=et, handler=h):
                    current = subscribers.get(et, ())
                    return active({**subscribers, et: tuple(r for r in current if r != h)})

                case Publish(event=event):
                    for handler in subscribers.get(type(event), ()):
                        handler.tell(event)
                    return Behaviors.same()

            return Behaviors.unhandled()

        return Behaviors.receive(receive)

    return active()
