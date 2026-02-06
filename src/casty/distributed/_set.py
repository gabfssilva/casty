from __future__ import annotations

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


@dataclass(frozen=True)
class _Add:
    value: Any
    reply_to: ActorRef[bool]


@dataclass(frozen=True)
class _Remove:
    value: Any
    reply_to: ActorRef[bool]


@dataclass(frozen=True)
class _SetContains:
    value: Any
    reply_to: ActorRef[bool]


@dataclass(frozen=True)
class _SetSize:
    reply_to: ActorRef[int]


type _SetMsg = _Add | _Remove | _SetContains | _SetSize


def set_entity(entity_id: str) -> Behavior[_SetMsg]:
    """Sharded set entity behavior. State via closure, starts as empty frozenset."""

    def active(items: frozenset[Any]) -> Behavior[_SetMsg]:
        async def receive(
            ctx: ActorContext[_SetMsg], msg: _SetMsg
        ) -> Behavior[_SetMsg]:
            match msg:
                case _Add(value, reply_to):
                    was_absent = value not in items
                    reply_to.tell(was_absent)
                    if was_absent:
                        return active(items | {value})
                    return Behaviors.same()
                case _Remove(value, reply_to):
                    was_present = value in items
                    reply_to.tell(was_present)
                    if was_present:
                        return active(items - {value})
                    return Behaviors.same()
                case _SetContains(value, reply_to):
                    reply_to.tell(value in items)
                    return Behaviors.same()
                case _SetSize(reply_to):
                    reply_to.tell(len(items))
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(frozenset())


# --- Event sourcing events ---


@dataclass(frozen=True)
class _ItemAdded:
    value: Any


@dataclass(frozen=True)
class _ItemRemoved:
    value: Any


type _SetEvent = _ItemAdded | _ItemRemoved


def _apply_event(state: frozenset[Any], event: _SetEvent) -> frozenset[Any]:
    """Pure event applier for persistent set."""
    match event:
        case _ItemAdded(value):
            return state | {value}
        case _ItemRemoved(value):
            return state - {value}
        case _:
            msg = f"Unknown set event: {type(event)}"
            raise TypeError(msg)


def persistent_set_entity(
    journal: EventJournal,
) -> Callable[[str], Behavior[_SetMsg]]:
    """Factory that returns an entity factory for event-sourced sets.

    Usage::

        entity_factory = persistent_set_entity(journal)
        region_ref = system.spawn(
            shard_region_actor(..., entity_factory=entity_factory, ...),
            "shard-region",
        )
    """

    def factory(entity_id: str) -> Behavior[_SetMsg]:
        async def on_command(
            ctx: ActorContext[_SetMsg], state: frozenset[Any], msg: _SetMsg
        ) -> Behavior[_SetMsg]:
            match msg:
                case _Add(value, reply_to):
                    was_absent = value not in state
                    reply_to.tell(was_absent)
                    if was_absent:
                        return Behaviors.persisted(
                            [_ItemAdded(value)], then=Behaviors.same()
                        )
                    return Behaviors.same()
                case _Remove(value, reply_to):
                    was_present = value in state
                    reply_to.tell(was_present)
                    if was_present:
                        return Behaviors.persisted(
                            [_ItemRemoved(value)], then=Behaviors.same()
                        )
                    return Behaviors.same()
                case _SetContains(value, reply_to):
                    reply_to.tell(value in state)
                    return Behaviors.same()
                case _SetSize(reply_to):
                    reply_to.tell(len(state))
                    return Behaviors.same()

        return Behaviors.event_sourced(
            entity_id=entity_id,
            journal=journal,
            initial_state=frozenset[Any](),
            on_event=_apply_event,
            on_command=on_command,
        )

    return factory


def set_behavior(*, num_shards: int = 100) -> ShardedBehavior[_SetMsg]:
    """Returns a ShardedBehavior suitable for ClusteredActorSystem.spawn()."""
    return Behaviors.sharded(entity_factory=set_entity, num_shards=num_shards)


class Set[V]:
    """Client for a distributed set backed by a sharded actor."""

    def __init__(
        self,
        *,
        system: ActorSystem,
        region_ref: ActorRef[ShardEnvelope[_SetMsg]],
        name: str,
        timeout: float = 5.0,
    ) -> None:
        self._system = system
        self._region_ref = region_ref
        self._name = name
        self._timeout = timeout

    async def add(self, value: V) -> bool:
        """Add a value to the set. Returns True if added, False if already present."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _Add(value=value, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def remove(self, value: V) -> bool:
        """Remove a value from the set. Returns True if removed, False if not present."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _Remove(value=value, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def contains(self, value: V) -> bool:
        """Check if a value is in the set."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _SetContains(value=value, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def size(self) -> int:
        """Get the number of elements in the set."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _SetSize(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )
