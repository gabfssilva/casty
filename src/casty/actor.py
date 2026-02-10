"""Core behavior definitions for the Casty actor library.

Behaviors are frozen dataclasses that define how an actor processes messages
and transitions between states. The ``Behaviors`` factory class provides
static methods for creating each behavior type.

State is managed through behavior recursion: a handler returns a new behavior
with updated closure-captured state, rather than mutating variables in place.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.journal import EventJournal
    from casty.messages import Terminated
    from casty.ref import ActorRef
    from casty.replication import ReplicationConfig
    from casty.supervision import SupervisionStrategy


@dataclass(frozen=True)
class ShardedBehavior[M]:
    """Behavior that distributes entity actors across cluster nodes.

    Entities are assigned to shards by hashing their entity ID.
    The shard coordinator allocates shards to nodes, and shard regions
    manage the local entity actors.

    Parameters
    ----------
    entity_factory : Callable[[str], Behavior[M]]
        Factory that creates a behavior for a given entity ID.
    num_shards : int
        Total number of shards to distribute. Default is 100.
    replication : ReplicationConfig or None
        Optional replication configuration for fault tolerance.

    Examples
    --------
    >>> def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    ...     return counter_behavior(entity_id, count=0)
    >>> sharded = ShardedBehavior(counter_entity, num_shards=50)
    """

    entity_factory: Callable[[str], Behavior[M]]
    num_shards: int = 100
    replication: ReplicationConfig | None = None


@dataclass(frozen=True)
class BroadcastedBehavior[M]:
    """Behavior wrapper that enables broadcasting messages to all instances.

    When spawned, the actor system creates a ``BroadcastRef`` alongside
    the regular ref, allowing messages to be sent to all actors of
    the same type across the cluster.

    Parameters
    ----------
    behavior : Behavior[M]
        The underlying behavior to wrap with broadcast capability.

    Examples
    --------
    >>> behavior = Behaviors.receive(my_handler)
    >>> broadcasted = BroadcastedBehavior(behavior)
    """

    behavior: Behavior[M]


@dataclass(frozen=True)
class SpyEvent[M]:
    """Event emitted by a spy wrapper for each message an actor receives.

    Contains the actor's path, the observed event (either a message or
    a ``Terminated`` signal), and a monotonic timestamp.

    Parameters
    ----------
    actor_path : str
        Path of the spied actor.
    event : M | Terminated
        The message received by the actor, or ``Terminated`` when it stops.
    timestamp : float
        Monotonic timestamp from ``time.monotonic()``.
    """

    actor_path: str
    event: M | Terminated
    timestamp: float


@dataclass(frozen=True)
class SpyBehavior[M]:
    """Behavior wrapper that observes all messages processed by the actor.

    Parameters
    ----------
    inner : Behavior[M]
        The behavior to spy on.
    observer : ActorRef[SpyEvent[M]]
        Ref that receives ``SpyEvent`` for every observed message.
    spy_children : bool
        When True, children spawned by this actor are also automatically
        spied with the same observer, recursively. Default is False.
    """

    inner: Behavior[M]
    observer: ActorRef[SpyEvent[M]]
    spy_children: bool = False


@dataclass(frozen=True)
class ReceiveBehavior[M]:
    """Behavior that processes messages with an async handler function.

    This is the most common behavior type. The handler receives the
    actor context and a message, and returns the next behavior.

    Parameters
    ----------
    handler : Callable[[ActorContext[M], M], Awaitable[Behavior[M]]]
        Async function that processes a message and returns the next behavior.

    Examples
    --------
    >>> async def greet(ctx, msg):
    ...     print(f"Hello {msg}")
    ...     return Behaviors.same()
    >>> behavior = ReceiveBehavior(greet)
    """

    handler: Callable[[ActorContext[M], M], Awaitable[Behavior[M]]]


@dataclass(frozen=True)
class SetupBehavior[M]:
    """Behavior that performs async initialization before returning the real behavior.

    Use this when an actor needs access to its ``ActorContext`` during
    initialization, for example to spawn children or schedule timers.

    Parameters
    ----------
    factory : Callable[[ActorContext[M]], Awaitable[Behavior[M]]]
        Async factory that receives the context and returns the initial behavior.

    Examples
    --------
    >>> async def init(ctx):
    ...     child = ctx.spawn(child_behavior, "worker")
    ...     return Behaviors.receive(make_handler(child))
    >>> behavior = SetupBehavior(init)
    """

    factory: Callable[[ActorContext[M]], Awaitable[Behavior[M]]]


@dataclass(frozen=True)
class SameBehavior:
    """Sentinel behavior indicating the actor keeps its current behavior.

    Return this from a message handler when no state transition is needed.

    Examples
    --------
    >>> async def handle(ctx, msg):
    ...     print(msg)
    ...     return Behaviors.same()
    """

    pass


@dataclass(frozen=True)
class StoppedBehavior:
    """Sentinel behavior indicating the actor should stop.

    Returning this from a handler triggers the actor's shutdown lifecycle,
    including ``post_stop`` hooks and ``Terminated`` notifications to watchers.

    Examples
    --------
    >>> async def handle(ctx, msg):
    ...     if msg == "shutdown":
    ...         return Behaviors.stopped()
    ...     return Behaviors.same()
    """

    pass


@dataclass(frozen=True)
class UnhandledBehavior:
    """Sentinel behavior indicating the message was not handled.

    Unhandled messages are published to the event stream as
    ``UnhandledMessage`` events and the actor keeps its current behavior.

    Examples
    --------
    >>> async def handle(ctx, msg):
    ...     match msg:
    ...         case Greet(name): ...
    ...         case _: return Behaviors.unhandled()
    """

    pass


@dataclass(frozen=True)
class RestartBehavior:
    """Sentinel behavior indicating the actor should restart.

    The actor re-initializes from its original behavior, triggering
    ``pre_restart`` and ``post_restart`` lifecycle hooks if configured.

    Examples
    --------
    >>> async def handle(ctx, msg):
    ...     if msg == "reset":
    ...         return Behaviors.restart()
    ...     return Behaviors.same()
    """

    pass


@dataclass(frozen=True)
class LifecycleBehavior[M]:
    """Behavior wrapper that adds lifecycle hooks to another behavior.

    Lifecycle hooks run at specific points in the actor's lifetime:
    start, stop, and around restarts.

    Parameters
    ----------
    behavior : Behavior[M]
        The underlying behavior to wrap.
    pre_start : Callable or None
        Async callback invoked before the actor starts processing messages.
    post_stop : Callable or None
        Async callback invoked after the actor stops.
    pre_restart : Callable or None
        Async callback invoked before a restart, receives the causing exception.
    post_restart : Callable or None
        Async callback invoked after the actor restarts.

    Examples
    --------
    >>> async def on_start(ctx):
    ...     print(f"{ctx.name} started")
    >>> behavior = LifecycleBehavior(
    ...     Behaviors.receive(my_handler), pre_start=on_start
    ... )
    """

    behavior: Behavior[M]
    pre_start: Callable[[ActorContext[M]], Awaitable[None]] | None = None
    post_stop: Callable[[ActorContext[M]], Awaitable[None]] | None = None
    pre_restart: Callable[[ActorContext[M], Exception], Awaitable[None]] | None = None
    post_restart: Callable[[ActorContext[M]], Awaitable[None]] | None = None


@dataclass(frozen=True)
class SupervisedBehavior[M]:
    """Behavior wrapper that adds a supervision strategy to another behavior.

    When a child actor fails, the supervision strategy decides whether
    to restart, stop, or escalate the failure.

    Parameters
    ----------
    behavior : Behavior[M]
        The underlying behavior to supervise.
    strategy : SupervisionStrategy
        The strategy used to handle child actor failures.

    Examples
    --------
    >>> from casty import OneForOneStrategy, Directive
    >>> strategy = OneForOneStrategy(directive=Directive.restart)
    >>> behavior = SupervisedBehavior(Behaviors.receive(handler), strategy)
    """

    behavior: Behavior[M]
    strategy: SupervisionStrategy


@dataclass(frozen=True)
class SnapshotEvery:
    """Snapshot policy that triggers a snapshot every N persisted events.

    Used with event-sourced behaviors to periodically snapshot state,
    reducing recovery time by limiting the number of events to replay.

    Parameters
    ----------
    n_events : int
        Number of events between snapshots.

    Examples
    --------
    >>> policy = SnapshotEvery(n_events=100)
    """

    n_events: int


type SnapshotPolicy = SnapshotEvery
"""Type alias for snapshot policies.

Currently only ``SnapshotEvery`` is supported. This alias exists to
allow future snapshot policy variants without breaking signatures.
"""


@dataclass(frozen=True)
class PersistedBehavior[M, E]:
    """Behavior returned from event-sourced command handlers to persist events.

    The runtime persists the events to the journal, applies them to the
    state via ``on_event``, then transitions to the ``then`` behavior.

    Parameters
    ----------
    events : tuple[E, ...]
        Events to persist to the journal.
    then : Behavior[M]
        Behavior to transition to after events are persisted.
        Defaults to ``SameBehavior`` if not specified via the factory.

    Examples
    --------
    >>> async def on_command(ctx, state, msg):
    ...     match msg:
    ...         case Deposit(amount):
    ...             return Behaviors.persisted([Deposited(amount)])
    """

    events: tuple[E, ...]
    then: Behavior[M]


@dataclass(frozen=True)
class EventSourcedBehavior[M, E, S]:
    """Behavior that persists events to a journal and rebuilds state on recovery.

    Command handlers return ``PersistedBehavior`` to persist events.
    On recovery, persisted events are replayed through ``on_event`` to
    rebuild the actor's state.

    Parameters
    ----------
    entity_id : str
        Unique identifier for the persistent entity.
    journal : EventJournal
        Storage backend for persisted events and snapshots.
    initial_state : S
        Starting state before any events are applied.
    on_event : Callable[[S, E], S]
        Pure function that applies an event to a state, returning new state.
    on_command : Callable[[ActorContext[M], S, M], Awaitable[Behavior[M]]]
        Async handler that processes commands against current state.
    snapshot_policy : SnapshotPolicy or None
        Optional policy for periodic state snapshots.
    replica_refs : tuple[ActorRef[Any], ...]
        Refs to replica actors for event replication.
    replication : ReplicationConfig or None
        Optional replication configuration.

    Examples
    --------
    >>> from casty import Behaviors
    >>> from casty.journal import InMemoryJournal
    >>> behavior = Behaviors.event_sourced(
    ...     entity_id="account-1", journal=InMemoryJournal(),
    ...     initial_state=0, on_event=apply, on_command=handle,
    ... )
    """

    entity_id: str
    journal: EventJournal
    initial_state: S
    on_event: Callable[[S, E], S]
    on_command: Callable[[ActorContext[M], S, M], Awaitable[Behavior[M]]]
    snapshot_policy: SnapshotPolicy | None = None
    replica_refs: tuple[ActorRef[Any], ...] = ()
    replication: ReplicationConfig | None = None


type Behavior[M] = (
    ReceiveBehavior[M]
    | SetupBehavior[M]
    | SameBehavior
    | StoppedBehavior
    | UnhandledBehavior
    | RestartBehavior
    | LifecycleBehavior[M]
    | SupervisedBehavior[M]
    | SpyBehavior[M]
    | EventSourcedBehavior[M, Any, Any]
    | PersistedBehavior[M, Any]
)
"""Union of all behavior types an actor can return.

This type alias represents every possible behavior that an actor's
message handler can return to define its next state.
"""


class Behaviors:
    """Factory for creating actor behaviors.

    All methods are static and return frozen behavior dataclasses.
    Behaviors define how an actor processes messages and transitions
    between states.

    Examples
    --------
    >>> from casty import Behaviors
    >>> behavior = Behaviors.receive(my_handler)
    """

    @staticmethod
    def receive[M](
        handler: Callable[[ActorContext[M], M], Awaitable[Behavior[M]]],
    ) -> ReceiveBehavior[M]:
        """Create a behavior that processes messages with a handler function.

        Parameters
        ----------
        handler : Callable[[ActorContext[M], M], Awaitable[Behavior[M]]]
            Async function receiving the actor context and a message,
            returning the next behavior.

        Returns
        -------
        ReceiveBehavior[M]
            A behavior wrapping the handler.

        Examples
        --------
        >>> async def greet(ctx, msg):
        ...     print(f"Hello {msg}")
        ...     return Behaviors.same()
        >>> behavior = Behaviors.receive(greet)
        """
        return ReceiveBehavior(handler)

    @staticmethod
    def setup[M](
        factory: Callable[[ActorContext[M]], Awaitable[Behavior[M]]],
    ) -> SetupBehavior[M]:
        """Create a behavior that initializes with the actor context.

        Use this when the actor needs its context during initialization,
        for example to spawn children or access its own ref.

        Parameters
        ----------
        factory : Callable[[ActorContext[M]], Awaitable[Behavior[M]]]
            Async factory receiving the context, returning the initial behavior.

        Returns
        -------
        SetupBehavior[M]
            A behavior that runs the factory on actor start.

        Examples
        --------
        >>> async def init(ctx):
        ...     child = ctx.spawn(worker_behavior, "worker")
        ...     return Behaviors.receive(make_handler(child))
        >>> behavior = Behaviors.setup(init)
        """
        return SetupBehavior(factory)

    @staticmethod
    def same() -> SameBehavior:
        """Keep the actor's current behavior unchanged.

        Returns
        -------
        SameBehavior
            A sentinel indicating no behavior change.

        Examples
        --------
        >>> async def handle(ctx, msg):
        ...     print(msg)
        ...     return Behaviors.same()
        """
        return SameBehavior()

    @staticmethod
    def stopped() -> StoppedBehavior:
        """Stop the actor.

        The actor will run its ``post_stop`` lifecycle hook (if any),
        stop all children, and notify watchers with ``Terminated``.

        Returns
        -------
        StoppedBehavior
            A sentinel indicating the actor should stop.

        Examples
        --------
        >>> async def handle(ctx, msg):
        ...     if msg == "quit":
        ...         return Behaviors.stopped()
        ...     return Behaviors.same()
        """
        return StoppedBehavior()

    @staticmethod
    def unhandled() -> UnhandledBehavior:
        """Signal that the message was not handled.

        The message is published to the event stream as an
        ``UnhandledMessage`` event. The actor keeps its current behavior.

        Returns
        -------
        UnhandledBehavior
            A sentinel indicating the message was not handled.

        Examples
        --------
        >>> async def handle(ctx, msg):
        ...     match msg:
        ...         case Greet(name): ...
        ...         case _: return Behaviors.unhandled()
        """
        return UnhandledBehavior()

    @staticmethod
    def restart() -> RestartBehavior:
        """Restart the actor from its initial behavior.

        Triggers ``pre_restart`` and ``post_restart`` lifecycle hooks
        if the behavior is wrapped with ``with_lifecycle``.

        Returns
        -------
        RestartBehavior
            A sentinel indicating the actor should restart.

        Examples
        --------
        >>> async def handle(ctx, msg):
        ...     if msg == "reset":
        ...         return Behaviors.restart()
        ...     return Behaviors.same()
        """
        return RestartBehavior()

    @staticmethod
    def with_lifecycle[M](
        behavior: Behavior[M],
        *,
        pre_start: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
        post_stop: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
        pre_restart: Callable[[ActorContext[M], Exception], Awaitable[None]]
        | None = None,
        post_restart: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
    ) -> LifecycleBehavior[M]:
        """Wrap a behavior with lifecycle hooks.

        Parameters
        ----------
        behavior : Behavior[M]
            The behavior to wrap.
        pre_start : Callable or None
            Called before the actor starts processing messages.
        post_stop : Callable or None
            Called after the actor stops.
        pre_restart : Callable or None
            Called before a restart, receives the causing exception.
        post_restart : Callable or None
            Called after the actor restarts.

        Returns
        -------
        LifecycleBehavior[M]
            The behavior with lifecycle hooks attached.

        Examples
        --------
        >>> async def on_stop(ctx):
        ...     print(f"{ctx.name} stopped")
        >>> behavior = Behaviors.with_lifecycle(
        ...     Behaviors.receive(my_handler), post_stop=on_stop
        ... )
        """
        return LifecycleBehavior(
            behavior, pre_start, post_stop, pre_restart, post_restart
        )

    @staticmethod
    def supervise[M](
        behavior: Behavior[M],
        strategy: SupervisionStrategy,
    ) -> SupervisedBehavior[M]:
        """Wrap a behavior with a supervision strategy.

        The strategy decides how to handle failures in child actors:
        restart, stop, or escalate.

        Parameters
        ----------
        behavior : Behavior[M]
            The behavior to supervise.
        strategy : SupervisionStrategy
            Strategy for handling child failures.

        Returns
        -------
        SupervisedBehavior[M]
            The behavior with supervision attached.

        Examples
        --------
        >>> from casty import OneForOneStrategy, Directive
        >>> strategy = OneForOneStrategy(directive=Directive.restart)
        >>> behavior = Behaviors.supervise(Behaviors.receive(handler), strategy)
        """
        return SupervisedBehavior(behavior, strategy)

    @staticmethod
    def broadcasted[M](
        behavior: Behavior[M],
    ) -> BroadcastedBehavior[M]:
        """Wrap a behavior to enable broadcast messaging.

        When spawned, the system returns a ``BroadcastRef`` that can
        send messages to all actors of this type across the cluster.

        Parameters
        ----------
        behavior : Behavior[M]
            The behavior to wrap with broadcast capability.

        Returns
        -------
        BroadcastedBehavior[M]
            The behavior with broadcasting enabled.

        Examples
        --------
        >>> behavior = Behaviors.broadcasted(Behaviors.receive(handler))
        >>> ref = system.spawn(behavior, "notifier")
        """
        return BroadcastedBehavior(behavior=behavior)

    @staticmethod
    def sharded[M](
        entity_factory: Callable[[str], Behavior[M]],
        *,
        num_shards: int = 100,
        replication: ReplicationConfig | None = None,
    ) -> ShardedBehavior[M]:
        """Create a sharded behavior that distributes entities across the cluster.

        Entities are assigned to shards by hashing their entity ID.
        Send messages via ``ShardEnvelope(entity_id, message)``.

        Parameters
        ----------
        entity_factory : Callable[[str], Behavior[M]]
            Factory that creates a behavior for a given entity ID.
        num_shards : int
            Total number of shards. Default is 100.
        replication : ReplicationConfig or None
            Optional replication configuration for fault tolerance.

        Returns
        -------
        ShardedBehavior[M]
            A behavior distributing entities across cluster shards.

        Examples
        --------
        >>> def make_counter(entity_id: str) -> Behavior[CounterMsg]:
        ...     return counter_behavior(entity_id, count=0)
        >>> behavior = Behaviors.sharded(make_counter, num_shards=50)
        """
        return ShardedBehavior(
            entity_factory=entity_factory,
            num_shards=num_shards,
            replication=replication,
        )

    @staticmethod
    def event_sourced[M, E, S](
        *,
        entity_id: str,
        journal: EventJournal,
        initial_state: S,
        on_event: Callable[[S, E], S],
        on_command: Callable[[ActorContext[M], S, M], Awaitable[Behavior[M]]],
        snapshot_policy: SnapshotPolicy | None = None,
        replica_refs: tuple[ActorRef[Any], ...] | None = None,
        replication: ReplicationConfig | None = None,
    ) -> EventSourcedBehavior[M, E, S]:
        """Create an event-sourced behavior with journal persistence.

        Commands are processed by ``on_command``, which returns
        ``Behaviors.persisted(events)`` to persist events. On recovery,
        events are replayed through ``on_event`` to rebuild state.

        Parameters
        ----------
        entity_id : str
            Unique identifier for the persistent entity.
        journal : EventJournal
            Storage backend for events and snapshots.
        initial_state : S
            Starting state before any events are applied.
        on_event : Callable[[S, E], S]
            Pure function applying an event to state, returning new state.
        on_command : Callable[[ActorContext[M], S, M], Awaitable[Behavior[M]]]
            Async handler processing commands against current state.
        snapshot_policy : SnapshotPolicy or None
            Optional policy for periodic state snapshots.
        replica_refs : tuple[ActorRef[Any], ...] or None
            Refs to replica actors for event replication.
        replication : ReplicationConfig or None
            Optional replication configuration.

        Returns
        -------
        EventSourcedBehavior[M, E, S]
            A behavior with event sourcing and journal persistence.

        Examples
        --------
        >>> from casty.journal import InMemoryJournal
        >>> behavior = Behaviors.event_sourced(
        ...     entity_id="acct-1", journal=InMemoryJournal(),
        ...     initial_state=0, on_event=apply, on_command=handle,
        ... )
        """
        return EventSourcedBehavior(
            entity_id=entity_id,
            journal=journal,
            initial_state=initial_state,
            on_event=on_event,
            on_command=on_command,
            snapshot_policy=snapshot_policy,
            replica_refs=replica_refs if replica_refs is not None else (),
            replication=replication,
        )

    @staticmethod
    def persisted[M, E](
        events: Sequence[E],
        then: Behavior[M] | None = None,
    ) -> PersistedBehavior[M, E]:
        """Persist events from an event-sourced command handler.

        Return this from ``on_command`` to persist events to the journal.
        After persistence, the actor transitions to the ``then`` behavior
        (defaults to ``same()``).

        Parameters
        ----------
        events : Sequence[E]
            Events to persist to the journal.
        then : Behavior[M] or None
            Behavior to transition to after persistence. Defaults to
            ``Behaviors.same()``.

        Returns
        -------
        PersistedBehavior[M, E]
            A behavior that persists events then transitions.

        Examples
        --------
        >>> async def on_command(ctx, state, msg):
        ...     match msg:
        ...         case Deposit(amount):
        ...             return Behaviors.persisted([Deposited(amount)])
        """
        return PersistedBehavior(
            events=tuple(events), then=then if then is not None else SameBehavior()
        )

    @staticmethod
    def spy[M](
        behavior: Behavior[M],
        observer: ActorRef[SpyEvent[M]],
        *,
        spy_children: bool = False,
    ) -> SpyBehavior[M]:
        """Wrap a behavior with a spy that reports all messages to an observer.

        The spy is a cell-level wrapper: the cell emits ``SpyEvent`` after
        processing each message, including self-tells. This captures all
        messages the actor actually processes, not just forwarded ones.

        Parameters
        ----------
        behavior : Behavior[M]
            The behavior to spy on.
        observer : ActorRef[SpyEvent[M]]
            Ref that receives ``SpyEvent`` for every observed message.
        spy_children : bool
            When True, children spawned by this actor are also automatically
            spied with the same observer, recursively. Default is False.

        Returns
        -------
        SpyBehavior[M]
            A behavior wrapper that observes all processed messages.

        Examples
        --------
        >>> spy_behavior = Behaviors.spy(my_behavior, observer_ref)
        >>> ref = system.spawn(spy_behavior, "my-actor")
        """
        return SpyBehavior(inner=behavior, observer=observer, spy_children=spy_children)
