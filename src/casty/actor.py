from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.journal import EventJournal
    from casty.ref import ActorRef
    from casty.replication import ReplicationConfig
    from casty.supervision import SupervisionStrategy


@dataclass(frozen=True)
class ShardedBehavior[M]:
    entity_factory: Callable[[str], Behavior[M]]
    num_shards: int = 100
    replication: ReplicationConfig | None = None


@dataclass(frozen=True)
class ReceiveBehavior[M]:
    handler: Callable[[ActorContext[M], M], Awaitable[Behavior[M]]]


@dataclass(frozen=True)
class SetupBehavior[M]:
    factory: Callable[[ActorContext[M]], Awaitable[Behavior[M]]]


@dataclass(frozen=True)
class SameBehavior:
    pass


@dataclass(frozen=True)
class StoppedBehavior:
    pass


@dataclass(frozen=True)
class UnhandledBehavior:
    pass


@dataclass(frozen=True)
class RestartBehavior:
    pass


@dataclass(frozen=True)
class LifecycleBehavior[M]:
    behavior: Behavior[M]
    pre_start: Callable[[ActorContext[M]], Awaitable[None]] | None = None
    post_stop: Callable[[ActorContext[M]], Awaitable[None]] | None = None
    pre_restart: Callable[[ActorContext[M], Exception], Awaitable[None]] | None = None
    post_restart: Callable[[ActorContext[M]], Awaitable[None]] | None = None


@dataclass(frozen=True)
class SupervisedBehavior[M]:
    behavior: Behavior[M]
    strategy: SupervisionStrategy


@dataclass(frozen=True)
class SnapshotEvery:
    n_events: int


type SnapshotPolicy = SnapshotEvery


@dataclass(frozen=True)
class PersistedBehavior[M, E]:
    events: list[E]
    then: Behavior[M]


@dataclass(frozen=True)
class EventSourcedBehavior[M, E, S]:
    entity_id: str
    journal: EventJournal
    initial_state: S
    on_event: Callable[[S, E], S]
    on_command: Callable[[ActorContext[M], S, M], Awaitable[Behavior[M]]]
    snapshot_policy: SnapshotPolicy | None = None
    replica_refs: list[ActorRef[Any]] = field(default_factory=lambda: list[ActorRef[Any]]())
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
    | EventSourcedBehavior[M, Any, Any]
    | PersistedBehavior[M, Any]
)


class Behaviors:
    @staticmethod
    def receive[M](
        handler: Callable[[ActorContext[M], M], Awaitable[Behavior[M]]],
    ) -> ReceiveBehavior[M]:
        return ReceiveBehavior(handler)

    @staticmethod
    def setup[M](
        factory: Callable[[ActorContext[M]], Awaitable[Behavior[M]]],
    ) -> SetupBehavior[M]:
        return SetupBehavior(factory)

    @staticmethod
    def same() -> SameBehavior:
        return SameBehavior()

    @staticmethod
    def stopped() -> StoppedBehavior:
        return StoppedBehavior()

    @staticmethod
    def unhandled() -> UnhandledBehavior:
        return UnhandledBehavior()

    @staticmethod
    def restart() -> RestartBehavior:
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
        return LifecycleBehavior(
            behavior, pre_start, post_stop, pre_restart, post_restart
        )

    @staticmethod
    def supervise[M](
        behavior: Behavior[M],
        strategy: SupervisionStrategy,
    ) -> SupervisedBehavior[M]:
        return SupervisedBehavior(behavior, strategy)

    @staticmethod
    def sharded[M](
        entity_factory: Callable[[str], Behavior[M]],
        *,
        num_shards: int = 100,
        replication: ReplicationConfig | None = None,
    ) -> ShardedBehavior[M]:
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
        replica_refs: list[ActorRef[Any]] | None = None,
        replication: ReplicationConfig | None = None,
    ) -> EventSourcedBehavior[M, E, S]:
        return EventSourcedBehavior(
            entity_id=entity_id,
            journal=journal,
            initial_state=initial_state,
            on_event=on_event,
            on_command=on_command,
            snapshot_policy=snapshot_policy,
            replica_refs=replica_refs if replica_refs is not None else [],
            replication=replication,
        )

    @staticmethod
    def persisted[M, E](
        events: list[E],
        then: Behavior[M] | None = None,
    ) -> PersistedBehavior[M, E]:
        return PersistedBehavior(
            events=events, then=then if then is not None else SameBehavior()
        )
