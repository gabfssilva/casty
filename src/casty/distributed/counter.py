"""Distributed counter backed by a sharded actor entity.

Each counter is a single entity within a shard region.  Supports optional
event sourcing via ``persistent_counter_entity``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty.actor import Behavior, Behaviors, ShardedBehavior
from casty.cluster.envelope import ShardEnvelope

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.core.journal import EventJournal
    from casty.distributed.distributed import EntityGateway
    from casty.ref import ActorRef


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


@dataclass(frozen=True)
class DestroyCounter:
    reply_to: ActorRef[bool]


type CounterMsg = Increment | Decrement | GetValue | DestroyCounter


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
                case DestroyCounter(reply_to):
                    reply_to.tell(True)
                    return Behaviors.stopped()

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
                case DestroyCounter(reply_to):
                    reply_to.tell(True)
                    return Behaviors.stopped()

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
    """Client for a distributed counter backed by a sharded actor.

    Provides ``increment``, ``decrement``, and ``get`` operations that
    route through the shard proxy to the correct entity.

    Parameters
    ----------
    system : ActorSystem
        The actor system for sending messages.
    region_ref : ActorRef[ShardEnvelope[CounterMsg]]
        Reference to the shard proxy / region.
    name : str
        Counter name (used as entity ID).
    timeout : float
        Default timeout for each operation.

    Examples
    --------
    >>> counter = d.counter("hits")
    >>> await counter.increment(5)
    5
    >>> await counter.get()
    5
    """

    def __init__(
        self,
        *,
        gateway: EntityGateway,
        region_ref: ActorRef[ShardEnvelope[CounterMsg]],
        name: str,
        timeout: float = 5.0,
    ) -> None:
        self._gateway = gateway
        self._region_ref = region_ref
        self._name = name
        self._timeout = timeout

    async def increment(self, amount: int = 1) -> int:
        """Increment the counter and return the new value.

        Parameters
        ----------
        amount : int
            Value to add (default ``1``).

        Returns
        -------
        int
            The counter value after incrementing.

        Examples
        --------
        >>> await counter.increment()
        1
        """
        return await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, Increment(amount=amount, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def decrement(self, amount: int = 1) -> int:
        """Decrement the counter and return the new value.

        Parameters
        ----------
        amount : int
            Value to subtract (default ``1``).

        Returns
        -------
        int
            The counter value after decrementing.

        Examples
        --------
        >>> await counter.decrement()
        -1
        """
        return await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, Decrement(amount=amount, reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def destroy(self) -> bool:
        """Destroy this counter, stopping the backing entity actor.

        Returns
        -------
        bool
            ``True`` if destroyed.
        """
        return await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, DestroyCounter(reply_to=reply_to)
            ),
            timeout=self._timeout,
        )

    async def get(self) -> int:
        """Get the current counter value.

        Returns
        -------
        int

        Examples
        --------
        >>> await counter.get()
        0
        """
        return await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(self._name, GetValue(reply_to=reply_to)),
            timeout=self._timeout,
        )
