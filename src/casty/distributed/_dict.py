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
class _Put:
    value: Any
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class _Get:
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class _Delete:
    reply_to: ActorRef[bool]


@dataclass(frozen=True)
class _Contains:
    reply_to: ActorRef[bool]


type _MapEntryMsg = _Put | _Get | _Delete | _Contains


def map_entity(entity_id: str) -> Behavior[_MapEntryMsg]:
    """Sharded map entry behavior. State via closure, starts as None."""

    def active(value: Any) -> Behavior[_MapEntryMsg]:
        async def receive(
            ctx: ActorContext[_MapEntryMsg], msg: _MapEntryMsg
        ) -> Behavior[_MapEntryMsg]:
            match msg:
                case _Put(new_value, reply_to):
                    reply_to.tell(value)
                    return active(new_value)
                case _Get(reply_to):
                    reply_to.tell(value)
                    return Behaviors.same()
                case _Delete(reply_to):
                    existed = value is not None
                    reply_to.tell(existed)
                    return active(None)
                case _Contains(reply_to):
                    reply_to.tell(value is not None)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(None)


# --- Event sourcing events ---


@dataclass(frozen=True)
class _ValueSet:
    value: Any


@dataclass(frozen=True)
class _ValueDeleted:
    pass


type _MapEntryEvent = _ValueSet | _ValueDeleted


def _apply_event(state: Any, event: _MapEntryEvent) -> Any:
    """Pure event applier for persistent map entry."""
    match event:
        case _ValueSet(value):
            return value
        case _ValueDeleted():
            return None
        case _:
            msg = f"Unknown map entry event: {type(event)}"
            raise TypeError(msg)


def persistent_map_entity(
    journal: EventJournal,
) -> Callable[[str], Behavior[_MapEntryMsg]]:
    """Factory that returns an entity factory for event-sourced map entries.

    Usage::

        entity_factory = persistent_map_entity(journal)
        region_ref = system.spawn(
            shard_region_actor(..., entity_factory=entity_factory, ...),
            "shard-region",
        )
    """

    def factory(entity_id: str) -> Behavior[_MapEntryMsg]:
        async def on_command(
            ctx: ActorContext[_MapEntryMsg], state: Any, msg: _MapEntryMsg
        ) -> Behavior[_MapEntryMsg]:
            match msg:
                case _Put(new_value, reply_to):
                    reply_to.tell(state)
                    return Behaviors.persisted(
                        [_ValueSet(new_value)], then=Behaviors.same()
                    )
                case _Get(reply_to):
                    reply_to.tell(state)
                    return Behaviors.same()
                case _Delete(reply_to):
                    reply_to.tell(state is not None)
                    return Behaviors.persisted(
                        [_ValueDeleted()], then=Behaviors.same()
                    )
                case _Contains(reply_to):
                    reply_to.tell(state is not None)
                    return Behaviors.same()

        return Behaviors.event_sourced(
            entity_id=entity_id,
            journal=journal,
            initial_state=None,
            on_event=_apply_event,
            on_command=on_command,
        )

    return factory


def map_behavior(*, num_shards: int = 100) -> ShardedBehavior[_MapEntryMsg]:
    """Returns a ShardedBehavior suitable for ClusteredActorSystem.spawn()."""
    return Behaviors.sharded(entity_factory=map_entity, num_shards=num_shards)


class Dict[K, V]:
    """Client for a distributed key-value map backed by sharded actors (entity-per-key)."""

    def __init__(
        self,
        *,
        system: ActorSystem,
        region_ref: ActorRef[ShardEnvelope[_MapEntryMsg]],
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
        """Store a value for the key. Returns the previous value, or None if new."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._entity_id(key), _Put(value=value, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def get(self, key: K, *, local: bool = False) -> V | None:
        """Get the value for the key, or None if not set.

        The ``local`` parameter is accepted but not yet used.
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._entity_id(key), _Get(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def delete(self, key: K) -> bool:
        """Delete the key. Returns True if it existed, False otherwise."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._entity_id(key), _Delete(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def contains(self, key: K) -> bool:
        """Check whether the key exists."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._entity_id(key), _Contains(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )
