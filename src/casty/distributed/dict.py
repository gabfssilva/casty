"""Distributed key-value map backed by sharded actors (one entity per key).

Supports optional event sourcing via ``persistent_map_entity``.
"""
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
class Put:
    value: Any
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class Get:
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class Delete:
    reply_to: ActorRef[bool]


@dataclass(frozen=True)
class Contains:
    reply_to: ActorRef[bool]


type MapEntryMsg = Put | Get | Delete | Contains


def map_entity(entity_id: str) -> Behavior[MapEntryMsg]:
    """Sharded map entry behavior. State via closure, starts as None."""

    def active(value: Any) -> Behavior[MapEntryMsg]:
        async def receive(
            ctx: ActorContext[MapEntryMsg], msg: MapEntryMsg
        ) -> Behavior[MapEntryMsg]:
            match msg:
                case Put(new_value, reply_to):
                    reply_to.tell(value)
                    return active(new_value)
                case Get(reply_to):
                    reply_to.tell(value)
                    return Behaviors.same()
                case Delete(reply_to):
                    existed = value is not None
                    reply_to.tell(existed)
                    return active(None)
                case Contains(reply_to):
                    reply_to.tell(value is not None)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(None)


# --- Event sourcing events ---


@dataclass(frozen=True)
class ValueSet:
    value: Any


@dataclass(frozen=True)
class ValueDeleted:
    pass


type MapEntryEvent = ValueSet | ValueDeleted


def apply_event(state: Any, event: MapEntryEvent) -> Any:
    """Pure event applier for persistent map entry."""
    match event:
        case ValueSet(value):
            return value
        case ValueDeleted():
            return None
        case _:
            msg = f"Unknown map entry event: {type(event)}"
            raise TypeError(msg)


def persistent_map_entity(
    journal: EventJournal,
) -> Callable[[str], Behavior[MapEntryMsg]]:
    """Factory that returns an entity factory for event-sourced map entries.

    Usage::

        entity_factory = persistent_map_entity(journal)
        region_ref = system.spawn(
            shard_region_actor(..., entity_factory=entity_factory, ...),
            "shard-region",
        )
    """

    def factory(entity_id: str) -> Behavior[MapEntryMsg]:
        async def on_command(
            ctx: ActorContext[MapEntryMsg], state: Any, msg: MapEntryMsg
        ) -> Behavior[MapEntryMsg]:
            match msg:
                case Put(new_value, reply_to):
                    reply_to.tell(state)
                    return Behaviors.persisted(
                        [ValueSet(new_value)], then=Behaviors.same()
                    )
                case Get(reply_to):
                    reply_to.tell(state)
                    return Behaviors.same()
                case Delete(reply_to):
                    reply_to.tell(state is not None)
                    return Behaviors.persisted(
                        [ValueDeleted()], then=Behaviors.same()
                    )
                case Contains(reply_to):
                    reply_to.tell(state is not None)
                    return Behaviors.same()

        return Behaviors.event_sourced(
            entity_id=entity_id,
            journal=journal,
            initial_state=None,
            on_event=apply_event,
            on_command=on_command,
        )

    return factory


def map_behavior(*, num_shards: int = 100) -> ShardedBehavior[MapEntryMsg]:
    """Returns a ShardedBehavior suitable for ClusteredActorSystem.spawn()."""
    return Behaviors.sharded(entity_factory=map_entity, num_shards=num_shards)


class Dict[K, V]:
    """Client for a distributed key-value map backed by sharded actors.

    Each key maps to a separate entity actor (entity-per-key pattern).

    Parameters
    ----------
    system : ActorSystem
        The actor system for sending messages.
    region_ref : ActorRef[ShardEnvelope[MapEntryMsg]]
        Reference to the shard proxy / region.
    name : str
        Map name prefix (combined with key for entity ID).
    timeout : float
        Default timeout for each operation.

    Examples
    --------
    >>> users = d.map[str, dict]("users")
    >>> await users.put("alice", {"age": 30})
    >>> await users.get("alice")
    {'age': 30}
    """

    def __init__(
        self,
        *,
        system: ActorSystem,
        region_ref: ActorRef[ShardEnvelope[MapEntryMsg]],
        name: str,
        timeout: float = 5.0,
    ) -> None:
        self._system = system
        self._region_ref = region_ref
        self._name = name
        self._timeout = timeout

    def _entity_id(self, key: K) -> str:
        return f"{self._name}:{key}"

    async def put(self, key: K, value: V) -> V | None:
        """Store a value for the key.

        Returns
        -------
        V | None
            The previous value, or ``None`` if the key was new.

        Examples
        --------
        >>> await users.put("alice", {"age": 30})
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._entity_id(key), Put(value=value, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def get(self, key: K, *, local: bool = False) -> V | None:
        """Get the value for the key, or ``None`` if not set.

        Parameters
        ----------
        key : K
            The key to look up.
        local : bool
            Reserved for future local-read optimization (currently unused).

        Returns
        -------
        V | None

        Examples
        --------
        >>> await users.get("alice")
        {'age': 30}
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._entity_id(key), Get(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def delete(self, key: K) -> bool:
        """Delete the key.

        Returns
        -------
        bool
            ``True`` if the key existed, ``False`` otherwise.

        Examples
        --------
        >>> await users.delete("alice")
        True
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._entity_id(key), Delete(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def contains(self, key: K) -> bool:
        """Check whether the key exists.

        Returns
        -------
        bool

        Examples
        --------
        >>> await users.contains("alice")
        True
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._entity_id(key), Contains(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )
