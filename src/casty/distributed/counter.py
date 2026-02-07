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
class Increment:
    amount: int
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Decrement:
    amount: int
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


type CounterMsg = Increment | Decrement | GetValue


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    """Sharded counter entity behavior. State via closure, starts at 0."""

    def active(value: int) -> Behavior[CounterMsg]:
        async def receive(
            ctx: ActorContext[CounterMsg], msg: CounterMsg
        ) -> Behavior[CounterMsg]:
            match msg:
                case Increment(amount, reply_to):
                    new_value = value + amount
                    reply_to.tell(new_value)
                    return active(new_value)
                case Decrement(amount, reply_to):
                    new_value = value - amount
                    reply_to.tell(new_value)
                    return active(new_value)
                case GetValue(reply_to):
                    reply_to.tell(value)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(0)


# --- Event sourcing events ---


@dataclass(frozen=True)
class Incremented:
    amount: int


@dataclass(frozen=True)
class Decremented:
    amount: int


type CounterEvent = Incremented | Decremented


def apply_event(state: int, event: CounterEvent) -> int:
    """Pure event applier for persistent counter."""
    match event:
        case Incremented(amount):
            return state + amount
        case Decremented(amount):
            return state - amount
        case _:
            msg = f"Unknown counter event: {type(event)}"
            raise TypeError(msg)


def persistent_counter_entity(
    journal: EventJournal,
) -> Callable[[str], Behavior[CounterMsg]]:
    """Factory that returns an entity factory for event-sourced counters.

    Usage::

        entity_factory = persistent_counter_entity(journal)
        region_ref = system.spawn(
            shard_region_actor(..., entity_factory=entity_factory, ...),
            "shard-region",
        )
    """

    def factory(entity_id: str) -> Behavior[CounterMsg]:
        async def on_command(
            ctx: ActorContext[CounterMsg], state: int, msg: CounterMsg
        ) -> Behavior[CounterMsg]:
            match msg:
                case Increment(amount, reply_to):
                    reply_to.tell(state + amount)
                    return Behaviors.persisted(
                        [Incremented(amount)], then=Behaviors.same()
                    )
                case Decrement(amount, reply_to):
                    reply_to.tell(state - amount)
                    return Behaviors.persisted(
                        [Decremented(amount)], then=Behaviors.same()
                    )
                case GetValue(reply_to):
                    reply_to.tell(state)
                    return Behaviors.same()

        return Behaviors.event_sourced(
            entity_id=entity_id,
            journal=journal,
            initial_state=0,
            on_event=apply_event,
            on_command=on_command,
        )

    return factory


def counter_behavior(*, num_shards: int = 100) -> ShardedBehavior[CounterMsg]:
    """Returns a ShardedBehavior suitable for ClusteredActorSystem.spawn()."""
    return Behaviors.sharded(entity_factory=counter_entity, num_shards=num_shards)


class Counter:
    """Client for a distributed counter backed by a sharded actor."""

    def __init__(
        self,
        *,
        system: ActorSystem,
        region_ref: ActorRef[ShardEnvelope[CounterMsg]],
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
                self._name, Increment(amount=amount, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def decrement(self, amount: int = 1) -> int:
        """Decrement the counter by amount and return the new value."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, Decrement(amount=amount, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def get(self) -> int:
        """Get the current counter value."""
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(self._name, GetValue(reply_to=reply_to)),
            timeout=self._timeout,
        )
