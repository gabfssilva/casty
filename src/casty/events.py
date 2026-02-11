"""Actor system event types and the publish-subscribe event stream.

Events are emitted by the actor system for lifecycle transitions
(start, stop, restart), dead letters, unhandled messages, and
cluster membership changes.
"""

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
    """Emitted when an actor successfully starts.

    Parameters
    ----------
    ref : ActorRef[Any]
        Reference to the started actor.

    Examples
    --------
    >>> from casty import ActorStarted
    >>> event = ActorStarted(ref=actor_ref)
    """

    ref: ActorRef[Any]


@dataclass(frozen=True)
class ActorStopped:
    """Emitted when an actor is stopped.

    Parameters
    ----------
    ref : ActorRef[Any]
        Reference to the stopped actor.

    Examples
    --------
    >>> from casty import ActorStopped
    >>> event = ActorStopped(ref=actor_ref)
    """

    ref: ActorRef[Any]


@dataclass(frozen=True)
class ActorRestarted:
    """Emitted when an actor is restarted after a failure.

    Parameters
    ----------
    ref : ActorRef[Any]
        Reference to the restarted actor.
    exception : Exception
        The exception that triggered the restart.

    Examples
    --------
    >>> from casty import ActorRestarted
    >>> event = ActorRestarted(ref=actor_ref, exception=RuntimeError("boom"))
    """

    ref: ActorRef[Any]
    exception: Exception


@dataclass(frozen=True)
class DeadLetter:
    """Emitted when a message is sent to a stopped or nonexistent actor.

    Parameters
    ----------
    message : Any
        The undeliverable message.
    intended_ref : ActorRef[Any]
        The actor reference the message was addressed to.

    Examples
    --------
    >>> from casty import DeadLetter
    >>> dl = DeadLetter(message="hello", intended_ref=dead_ref)
    """

    message: Any
    intended_ref: ActorRef[Any]


@dataclass(frozen=True)
class UnhandledMessage:
    """Emitted when an actor returns ``Behaviors.unhandled()`` for a message.

    Parameters
    ----------
    message : Any
        The message that was not handled.
    ref : ActorRef[Any]
        Reference to the actor that did not handle it.

    Examples
    --------
    >>> from casty import UnhandledMessage
    >>> evt = UnhandledMessage(message="unknown", ref=actor_ref)
    """

    message: Any
    ref: ActorRef[Any]


@dataclass(frozen=True)
class MemberUp:
    """Emitted when a cluster member transitions to the ``up`` status.

    Parameters
    ----------
    member : Member
        The cluster member that came up.

    Examples
    --------
    >>> from casty import MemberUp
    >>> event = MemberUp(member=member)
    """

    member: Member


@dataclass(frozen=True)
class MemberLeft:
    """Emitted when a cluster member leaves the cluster.

    Parameters
    ----------
    member : Member
        The cluster member that left.

    Examples
    --------
    >>> from casty import MemberLeft
    >>> event = MemberLeft(member=member)
    """

    member: Member


@dataclass(frozen=True)
class UnreachableMember:
    """Emitted when the failure detector marks a member as unreachable.

    Parameters
    ----------
    member : Member
        The cluster member that became unreachable.

    Examples
    --------
    >>> from casty import UnreachableMember
    >>> event = UnreachableMember(member=member)
    """

    member: Member


@dataclass(frozen=True)
class ReachableMember:
    """Emitted when a previously unreachable member becomes reachable again.

    Parameters
    ----------
    member : Member
        The cluster member that became reachable.

    Examples
    --------
    >>> from casty import ReachableMember
    >>> event = ReachableMember(member=member)
    """

    member: Member


# --- EventStream ---

type EventHandler[E] = Callable[[E], Awaitable[None] | None]


class EventStream:
    """Publish-subscribe bus for actor system events.

    Allows handlers to subscribe to specific event types and receive
    notifications when those events are published.

    Examples
    --------
    >>> from casty import EventStream, ActorStarted
    >>> stream = EventStream()
    >>> stream.subscribe(ActorStarted, lambda e: print(e.ref))
    """

    def __init__(self) -> None:
        self._subscribers: dict[type, list[EventHandler[Any]]] = defaultdict(list)
        self.suppress_dead_letters_on_shutdown: bool = False

    def subscribe[E](self, event_type: type[E], handler: EventHandler[E]) -> None:
        """Register a handler for a specific event type.

        Parameters
        ----------
        event_type : type[E]
            The event class to listen for.
        handler : EventHandler[E]
            Sync or async callable invoked when the event is published.

        Examples
        --------
        >>> stream = EventStream()
        >>> stream.subscribe(ActorStopped, lambda e: print("stopped"))
        """
        self._subscribers[event_type].append(handler)

    def unsubscribe(self, event_type: type[object], handler: EventHandler[object]) -> None:
        """Remove a previously registered handler.

        Parameters
        ----------
        event_type : type[object]
            The event class the handler was subscribed to.
        handler : EventHandler[object]
            The handler to remove.

        Examples
        --------
        >>> stream = EventStream()
        >>> handler = lambda e: None
        >>> stream.subscribe(ActorStopped, handler)
        >>> stream.unsubscribe(ActorStopped, handler)
        """
        handlers = self._subscribers.get(event_type)
        if handlers:
            handlers.remove(handler)

    async def publish(self, event: object) -> None:
        """Publish an event to all subscribed handlers.

        Async handlers are awaited; sync handlers are called directly.

        Parameters
        ----------
        event : object
            The event instance to publish.

        Examples
        --------
        >>> stream = EventStream()
        >>> await stream.publish(ActorStarted(ref=some_ref))
        """
        handlers = self._subscribers.get(type(event), [])
        for handler in handlers:
            result = handler(event)
            if asyncio.iscoroutine(result):
                await result
