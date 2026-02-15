"""Distributed set backed by a sharded actor entity.

Supports optional event sourcing via ``persistent_set_entity``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors, ShardedBehavior
from casty.cluster.envelope import ShardEnvelope

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.core.journal import EventJournal
    from casty.ref import ActorRef
    from casty.core.system import ActorSystem


@dataclass(frozen=True)
class Add:
    value: Any
    reply_to: ActorRef[bool]


@dataclass(frozen=True)
class Remove:
    value: Any
    reply_to: ActorRef[bool]


@dataclass(frozen=True)
class SetContains:
    value: Any
    reply_to: ActorRef[bool]


@dataclass(frozen=True)
class SetSize:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class DestroySet:
    reply_to: ActorRef[bool]


type SetMsg = Add | Remove | SetContains | SetSize | DestroySet


def set_entity(entity_id: str) -> Behavior[SetMsg]:
    """Sharded set entity behavior. State via closure, starts as empty frozenset."""

    def active(items: frozenset[Any]) -> Behavior[SetMsg]:
        async def receive(ctx: ActorContext[SetMsg], msg: SetMsg) -> Behavior[SetMsg]:
            match msg:
                case Add(value, reply_to):
                    was_absent = value not in items
                    reply_to.tell(was_absent)
                    if was_absent:
                        return active(items | {value})
                    return Behaviors.same()
                case Remove(value, reply_to):
                    was_present = value in items
                    reply_to.tell(was_present)
                    if was_present:
                        return active(items - {value})
                    return Behaviors.same()
                case SetContains(value, reply_to):
                    reply_to.tell(value in items)
                    return Behaviors.same()
                case SetSize(reply_to):
                    reply_to.tell(len(items))
                    return Behaviors.same()
                case DestroySet(reply_to):
                    reply_to.tell(True)
                    return Behaviors.stopped()

        return Behaviors.receive(receive)

    return active(frozenset())


# --- Event sourcing events ---


@dataclass(frozen=True)
class ItemAdded:
    value: Any


@dataclass(frozen=True)
class ItemRemoved:
    value: Any


type SetEvent = ItemAdded | ItemRemoved


def apply_event(state: frozenset[Any], event: SetEvent) -> frozenset[Any]:
    """Pure event applier for persistent set."""
    match event:
        case ItemAdded(value):
            return state | {value}
        case ItemRemoved(value):
            return state - {value}
        case _:
            msg = f"Unknown set event: {type(event)}"
            raise TypeError(msg)


def persistent_set_entity(
    journal: EventJournal,
) -> Callable[[str], Behavior[SetMsg]]:
    """Factory that returns an entity factory for event-sourced sets.

    Usage::

        entity_factory = persistent_set_entity(journal)
        region_ref = system.spawn(
            shard_region_actor(..., entity_factory=entity_factory, ...),
            "shard-region",
        )
    """

    def factory(entity_id: str) -> Behavior[SetMsg]:
        async def on_command(
            ctx: ActorContext[SetMsg], state: frozenset[Any], msg: SetMsg
        ) -> Behavior[SetMsg]:
            match msg:
                case Add(value, reply_to):
                    was_absent = value not in state
                    reply_to.tell(was_absent)
                    if was_absent:
                        return Behaviors.persisted(
                            [ItemAdded(value)], then=Behaviors.same()
                        )
                    return Behaviors.same()
                case Remove(value, reply_to):
                    was_present = value in state
                    reply_to.tell(was_present)
                    if was_present:
                        return Behaviors.persisted(
                            [ItemRemoved(value)], then=Behaviors.same()
                        )
                    return Behaviors.same()
                case SetContains(value, reply_to):
                    reply_to.tell(value in state)
                    return Behaviors.same()
                case SetSize(reply_to):
                    reply_to.tell(len(state))
                    return Behaviors.same()
                case DestroySet(reply_to):
                    reply_to.tell(True)
                    return Behaviors.stopped()

        return Behaviors.event_sourced(
            entity_id=entity_id,
            journal=journal,
            initial_state=frozenset[Any](),
            on_event=apply_event,
            on_command=on_command,
        )

    return factory


def set_behavior(*, num_shards: int = 100) -> ShardedBehavior[SetMsg]:
    """Returns a ShardedBehavior suitable for ClusteredActorSystem.spawn()."""
    return Behaviors.sharded(entity_factory=set_entity, num_shards=num_shards)


class Set[V]:
    """Client for a distributed set backed by a sharded actor.

    Parameters
    ----------
    system : ActorSystem
        The actor system for sending messages.
    region_ref : ActorRef[ShardEnvelope[SetMsg]]
        Reference to the shard proxy / region.
    name : str
        Set name (used as entity ID).
    timeout : float
        Default timeout for each operation.

    Examples
    --------
    >>> tags = d.set[str]("tags")
    >>> await tags.add("python")
    True
    >>> await tags.contains("python")
    True
    """

    def __init__(
        self,
        *,
        system: ActorSystem,
        region_ref: ActorRef[ShardEnvelope[SetMsg]],
        name: str,
        timeout: float = 5.0,
    ) -> None:
        self._system = system
        self._region_ref = region_ref
        self._name = name
        self._timeout = timeout

    async def destroy(self) -> bool:
        """Destroy this set, stopping the backing entity actor.

        Returns
        -------
        bool
            ``True`` if destroyed.
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(self._name, DestroySet(reply_to=reply_to)),
            timeout=self._timeout,
        )

    async def add(self, value: V) -> bool:
        """Add a value to the set.

        Returns
        -------
        bool
            ``True`` if added, ``False`` if already present.

        Examples
        --------
        >>> await tags.add("python")
        True
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, Add(value=value, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def remove(self, value: V) -> bool:
        """Remove a value from the set.

        Returns
        -------
        bool
            ``True`` if removed, ``False`` if not present.

        Examples
        --------
        >>> await tags.remove("python")
        True
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, Remove(value=value, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def contains(self, value: V) -> bool:
        """Check if a value is in the set.

        Returns
        -------
        bool

        Examples
        --------
        >>> await tags.contains("python")
        True
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, SetContains(value=value, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def size(self) -> int:
        """Get the number of elements in the set.

        Returns
        -------
        int

        Examples
        --------
        >>> await tags.size()
        1
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(self._name, SetSize(reply_to=reply_to)),
            timeout=self._timeout,
        )
