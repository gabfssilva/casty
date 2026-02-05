from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.supervision import SupervisionStrategy


@dataclass(frozen=True)
class ShardedBehavior[M]:
    entity_factory: Callable[[str], Behavior[M]]
    num_shards: int = 100


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


type Behavior[M] = (
    ReceiveBehavior[M]
    | SetupBehavior[M]
    | SameBehavior
    | StoppedBehavior
    | UnhandledBehavior
    | RestartBehavior
    | LifecycleBehavior[M]
    | SupervisedBehavior[M]
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
        pre_restart: Callable[[ActorContext[M], Exception], Awaitable[None]] | None = None,
        post_restart: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
    ) -> LifecycleBehavior[M]:
        return LifecycleBehavior(behavior, pre_start, post_stop, pre_restart, post_restart)

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
    ) -> ShardedBehavior[M]:
        return ShardedBehavior(entity_factory=entity_factory, num_shards=num_shards)
