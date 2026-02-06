from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors, ShardedBehavior
from casty.sharding import ShardEnvelope

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.journal import EventJournal
    from casty.ref import ActorRef
    from casty.system import ActorSystem


# ---------------------------------------------------------------------------
# Messages (private to module)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Enqueue:
    value: Any
    reply_to: ActorRef[None]


@dataclass(frozen=True)
class _Dequeue:
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class _Peek:
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class _QueueSize:
    reply_to: ActorRef[int]


type _QueueMsg = _Enqueue | _Dequeue | _Peek | _QueueSize


# ---------------------------------------------------------------------------
# Entity behavior
# ---------------------------------------------------------------------------


def queue_entity(entity_id: str) -> Behavior[_QueueMsg]:
    """Sharded queue entity behavior. State via mutable deque in closure."""
    items: deque[Any] = deque()

    async def receive(
        ctx: ActorContext[_QueueMsg], msg: _QueueMsg
    ) -> Behavior[_QueueMsg]:
        match msg:
            case _Enqueue(value, reply_to):
                items.append(value)
                reply_to.tell(None)
                return Behaviors.same()
            case _Dequeue(reply_to):
                value = items.popleft() if items else None
                reply_to.tell(value)
                return Behaviors.same()
            case _Peek(reply_to):
                value = items[0] if items else None
                reply_to.tell(value)
                return Behaviors.same()
            case _QueueSize(reply_to):
                reply_to.tell(len(items))
                return Behaviors.same()

    return Behaviors.receive(receive)


# ---------------------------------------------------------------------------
# Event sourcing events
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ItemEnqueued:
    value: Any


@dataclass(frozen=True)
class _ItemDequeued:
    pass


type _QueueEvent = _ItemEnqueued | _ItemDequeued


def _apply_event(state: tuple[Any, ...], event: _QueueEvent) -> tuple[Any, ...]:
    """Pure event applier for persistent queue."""
    match event:
        case _ItemEnqueued(value):
            return (*state, value)
        case _ItemDequeued():
            return state[1:] if state else state
        case _:
            msg = f"Unknown queue event: {type(event)}"
            raise TypeError(msg)


def persistent_queue_entity(
    journal: EventJournal,
) -> Callable[[str], Behavior[_QueueMsg]]:
    """Factory that returns an entity factory for event-sourced queues.

    Usage::

        entity_factory = persistent_queue_entity(journal)
        region_ref = system.spawn(
            shard_region_actor(..., entity_factory=entity_factory, ...),
            "shard-region",
        )
    """

    def factory(entity_id: str) -> Behavior[_QueueMsg]:
        async def on_command(
            ctx: ActorContext[_QueueMsg],
            state: tuple[Any, ...],
            msg: _QueueMsg,
        ) -> Behavior[_QueueMsg]:
            match msg:
                case _Enqueue(value, reply_to):
                    reply_to.tell(None)
                    return Behaviors.persisted(
                        [_ItemEnqueued(value)], then=Behaviors.same()
                    )
                case _Dequeue(reply_to):
                    if state:
                        reply_to.tell(state[0])
                        return Behaviors.persisted(
                            [_ItemDequeued()], then=Behaviors.same()
                        )
                    reply_to.tell(None)
                    return Behaviors.same()
                case _Peek(reply_to):
                    reply_to.tell(state[0] if state else None)
                    return Behaviors.same()
                case _QueueSize(reply_to):
                    reply_to.tell(len(state))
                    return Behaviors.same()

        return Behaviors.event_sourced(
            entity_id=entity_id,
            journal=journal,
            initial_state=(),
            on_event=_apply_event,
            on_command=on_command,
        )

    return factory


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def queue_behavior(*, num_shards: int = 100) -> ShardedBehavior[_QueueMsg]:
    """Returns a ShardedBehavior suitable for ClusteredActorSystem.spawn()."""
    return Behaviors.sharded(entity_factory=queue_entity, num_shards=num_shards)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class Queue[V]:
    """Client for a distributed queue backed by a sharded actor."""

    def __init__(
        self,
        *,
        system: ActorSystem,
        region_ref: ActorRef[ShardEnvelope[_QueueMsg]],
        name: str,
        timeout: float = 5.0,
    ) -> None:
        self._system = system
        self._region_ref = region_ref
        self._name = name
        self._timeout = timeout

    async def enqueue(self, value: V) -> None:
        """Append a value to the back of the queue."""
        await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _Enqueue(value=value, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def dequeue(self) -> V | None:
        """Remove and return the front value, or None if empty."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _Dequeue(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def peek(self) -> V | None:
        """Return the front value without removing, or None if empty."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _Peek(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def size(self) -> int:
        """Return the number of items in the queue."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _QueueSize(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )
