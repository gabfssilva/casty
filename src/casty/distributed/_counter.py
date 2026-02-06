from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty.actor import Behavior, Behaviors, ShardedBehavior
from casty.sharding import ShardEnvelope

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.journal import EventJournal
    from casty.ref import ActorRef
    from casty.system import ActorSystem


@dataclass(frozen=True)
class _Increment:
    amount: int
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class _Decrement:
    amount: int
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class _GetValue:
    reply_to: ActorRef[int]


type _CounterMsg = _Increment | _Decrement | _GetValue


def counter_entity(entity_id: str) -> Behavior[_CounterMsg]:
    """Sharded counter entity behavior. State via closure, starts at 0."""

    def active(value: int) -> Behavior[_CounterMsg]:
        async def receive(
            ctx: ActorContext[_CounterMsg], msg: _CounterMsg
        ) -> Behavior[_CounterMsg]:
            match msg:
                case _Increment(amount, reply_to):
                    new_value = value + amount
                    reply_to.tell(new_value)
                    return active(new_value)
                case _Decrement(amount, reply_to):
                    new_value = value - amount
                    reply_to.tell(new_value)
                    return active(new_value)
                case _GetValue(reply_to):
                    reply_to.tell(value)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(0)


# --- Event sourcing events ---


@dataclass(frozen=True)
class _Incremented:
    amount: int


@dataclass(frozen=True)
class _Decremented:
    amount: int


type _CounterEvent = _Incremented | _Decremented


def _apply_event(state: int, event: _CounterEvent) -> int:
    """Pure event applier for persistent counter."""
    match event:
        case _Incremented(amount):
            return state + amount
        case _Decremented(amount):
            return state - amount


def persistent_counter_entity(
    journal: EventJournal,
) -> Callable[[str], Behavior[_CounterMsg]]:
    """Factory that returns an entity factory for event-sourced counters.

    Usage::

        entity_factory = persistent_counter_entity(journal)
        region_ref = system.spawn(
            shard_region_actor(..., entity_factory=entity_factory, ...),
            "shard-region",
        )
    """

    def factory(entity_id: str) -> Behavior[_CounterMsg]:
        async def on_command(
            ctx: ActorContext[_CounterMsg], state: int, msg: _CounterMsg
        ) -> Behavior[_CounterMsg]:
            match msg:
                case _Increment(amount, reply_to):
                    reply_to.tell(state + amount)
                    return Behaviors.persisted(
                        [_Incremented(amount)], then=Behaviors.same()
                    )
                case _Decrement(amount, reply_to):
                    reply_to.tell(state - amount)
                    return Behaviors.persisted(
                        [_Decremented(amount)], then=Behaviors.same()
                    )
                case _GetValue(reply_to):
                    reply_to.tell(state)
                    return Behaviors.same()

        return Behaviors.event_sourced(
            entity_id=entity_id,
            journal=journal,
            initial_state=0,
            on_event=_apply_event,
            on_command=on_command,
        )

    return factory


def counter_behavior(*, num_shards: int = 100) -> ShardedBehavior[_CounterMsg]:
    """Returns a ShardedBehavior suitable for ClusteredActorSystem.spawn()."""
    return Behaviors.sharded(entity_factory=counter_entity, num_shards=num_shards)


class Counter:
    """Client for a distributed counter backed by a sharded actor."""

    def __init__(
        self,
        *,
        system: ActorSystem,
        region_ref: ActorRef[ShardEnvelope[_CounterMsg]],
        name: str,
        timeout: float = 5.0,
    ) -> None:
        self._system = system
        self._region_ref = region_ref
        self._name = name
        self._timeout = timeout

    async def increment(self, amount: int = 1) -> int:
        """Increment the counter by amount and return the new value."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _Increment(amount=amount, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def decrement(self, amount: int = 1) -> int:
        """Decrement the counter by amount and return the new value."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, _Decrement(amount=amount, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def get(self) -> int:
        """Get the current counter value."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(self._name, _GetValue(reply_to=reply_to)),
            timeout=self._timeout,
        )
